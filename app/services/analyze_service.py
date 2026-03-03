"""분석 오케스트레이션 서비스.

요청 1건(jobInstanceId)에 대해 아래 순서로 처리
1) 멱등성 체크
2) `/analysis/req/{jobInstanceId}` 입력 청크 전체 조회
3) chunk별 결과 생성/저장 (이미 처리된 chunk는 스킵)
4) 상태 업데이트

"""

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Any

from app.core.config import Settings
from app.core.constants import REQUEST_STATUS_COMPLETED, REQUEST_STATUS_FAILED
from app.infra.efs.paths import (
    build_manifest_path,
    build_output_paths,
    build_ref_alias_path,
    build_req_dir,
    list_chunk_inputs,
    parse_chunk_id,
)
from app.infra.efs.reader import read_alias_records, read_counsel_records
from app.infra.efs.writer import write_result_records
from app.schemas.analyze_request import AnalyzeRequest
from app.schemas.result_record import KeywordCountRecord, ResultRecord
from app.services.idempotency_service import IdempotencyService

from app.pipeline.mapper import ExactMapper
from app.pipeline.extractor import AhoCorasickExtractor
from app.pipeline.scorer import ContextScorer

class AnalyzeService:
    def __init__(
        self,
        settings: Settings,
        idempotency: IdempotencyService,
    ) -> None:
        self.settings = settings
        self.idempotency = idempotency

        # 1, 2, 3단계 엔진 초기화
        self.mapper = ExactMapper()
        self.extractor = AhoCorasickExtractor()
        self.scorer = ContextScorer()

        # 키워드 메타데이터 (ID -> 이름) 저장을 위한 딕셔너리 (3단계 중의성 해소용)
        # 3단계 심사위원(Scorer)이 "요금"이라는 단어를 찾았을 때, 
        # 후보인 BK-012의 진짜 이름이 "요금조회"라는 것을 알아야 문맥 점수를 매길 수 있음
        # 그래서 { "BK-012": "요금조회" } 형식의 지도를 메모리에 들고 있는 것
        self.keyword_meta: Dict[str, str] = {}

    def _initialize_pipeline(self, alias_records: list):
        """
        [사전 적재] EFS에서 읽어온 사전 데이터를 각 분석 엔진들에 장전
        (job 1개당 1번만 실행됨)
        """
        dict_data_for_pipeline = []
        
        for record in alias_records:
            # 1. 공통 정보 추출
            label_id = str(record.get("label_id", ""))
            if not label_id: continue
            
            schema = record.get("schema", "")
            
            # 2. 키워드(표준어) 정보 처리
            if schema == "dict.keyword.v1":
                canon_name = record.get("business_keyword", "")
                # 심사위원용 메타데이터 등록
                self.keyword_meta[label_id] = canon_name
                
                # 1단계(Mapper)와 2단계(Extractor)가 인식할 수 있는 형태로 변환
                dict_data_for_pipeline.append({
                    "schema": "dict.keyword.v1",
                    "label_id": label_id,
                    "business_keyword": canon_name
                })

            # 3. 별칭(Alias) 정보 처리
            elif schema == "dict.alias.v1":
                alias_text = record.get("alias_text", "")
                alias_norm = record.get("alias_norm", alias_text) # norm 없으면 text 사용
                canon_name = record.get("business_keyword", "") # 표준어 명칭

                dict_data_for_pipeline.append({
                    "schema": "dict.alias.v1",
                    "label_id": label_id,
                    "business_keyword": canon_name,
                    "alias_text": alias_text,
                    "alias_norm": alias_norm
                })

        # 빌드된 통합 데이터를 엔진들에게 전송 (내부적으로 인덱스/오토마톤 생성)
        self.mapper.build_index(dict_data_for_pipeline)
        self.extractor.build_automaton(dict_data_for_pipeline)
    
    def _apply_masking(self, text: str, matches: List[Dict[str, Any]]) -> str:
        """
        [마스킹 기술] 이미 찾은 단어 위치를 '*'로 가려 다음 단계의 중복 추출을 방지
        """
        chars = list(text)
        for m in matches:
            for i in range(m["orig_start"], m["orig_end"] + 1):
                if i < len(chars):
                    chars[i] = "*"
        return "".join(chars)
    
    def analyze(self, request: AnalyzeRequest) -> tuple[bool, str]:
        """jobInstanceId 하위 모든 chunk를 처리"""
        # 같은 requestId가 다시 들어오면 중복 처리하지 않음
        accepted = self.idempotency.register_or_reject(request.request_id)
        if not accepted:
            return False, "duplicate requestId"

        try:
            req_dir = build_req_dir(self.settings.efs_base_dir, request.job_instance_id)
            if not req_dir.exists() or not req_dir.is_dir():
                raise FileNotFoundError(f"request directory not found: {req_dir}")

            # 분석 버전에 맞는 키워드/별칭 사전 로드
            alias_path = build_ref_alias_path(self.settings.efs_base_dir, request.analysis_version)
            if not alias_path.exists():
                raise FileNotFoundError(f"alias dictionary not found: {alias_path}")
            alias_records = read_alias_records(alias_path)

            # 모든 Chunk가 이 준비된 엔진을 공유해서 사용
            self._initialize_pipeline(alias_records)

            input_files = list_chunk_inputs(req_dir)
            processed_chunks = 0
            skipped_chunks = 0
            total_records = 0

            for input_file in input_files:
                chunk_id = parse_chunk_id(input_file)
                manifest_path = build_manifest_path(req_dir, chunk_id)
                mapping_path, chunk_summary_path = build_output_paths(
                    self.settings.efs_base_dir,
                    request.job_instance_id,
                    chunk_id,
                )

                if mapping_path.exists() and chunk_summary_path.exists():
                    # 결과 파일 2개가 이미 있으면 완료된 chunk로 판단하고 skip
                    skipped_chunks += 1
                    continue

                # chunk 입력(상담 JSONL gzip)을 읽어서 case 단위 결과로 변환
                counsel_records = read_counsel_records(input_file)
                results = self._build_results(request, counsel_records, alias_records, chunk_id)
                write_result_records(mapping_path, results)

                chunk_summary = self._build_chunk_summary(
                    request=request,
                    chunk_id=chunk_id,
                    input_file=input_file,
                    manifest_path=manifest_path,
                    output_file=mapping_path,
                    record_count=len(results),
                )
                self._write_json_atomic(chunk_summary_path, chunk_summary)

                processed_chunks += 1
                total_records += len(results)

            self.idempotency.registry.update_status(request.request_id, REQUEST_STATUS_COMPLETED)
            return (
                True,
                f"job={request.job_instance_id} chunks(total={len(input_files)}, processed={processed_chunks}, skipped={skipped_chunks}) records={total_records}",
            )
        except Exception:
            self.idempotency.registry.update_status(request.request_id, REQUEST_STATUS_FAILED)
            raise

    def _build_results(self, request: AnalyzeRequest, counsel_records, alias_records, chunk_id: str) -> list[ResultRecord]:
        """
        상담 1건마다 키워드별 횟수를 계산해 결과 생성.
        [핵심 파이프라인] 상담 내역을 한 줄씩 읽어 3단 콤보 분석을 수행합니다.
        """
        rows: list[ResultRecord] = []
        now = datetime.now(timezone.utc)

        for counsel in counsel_records:
            # 상담 데이터 스키마에 맞춰 'text' 필드 추출
            raw_text = counsel.get("text", "") 
            if not raw_text: continue

            # 1단계: 완전 일치 (Exact Match)
            # "요금조회" 처럼 토시 하나 안틀린 경우 바로 캐치
            step1_results = self.mapper.exact_match(raw_text)
            
            # 2단계 전 마스킹
            masked_text_v1 = self._apply_masking(raw_text, step1_results)

            # 2단계: 부분 일치 (Aho-Corasick)
            # 문장 속에 숨어있는 키워드들을 한번에 훑어서 추출
            step2_results = self.extractor.extract(masked_text_v1)
            
            # 3단계 전 마스킹
            masked_text_v2 = self._apply_masking(masked_text_v1, step2_results)

            # 3단계: 패자부활전 및 중의성 해소 (Context Scorer)
            # spaCy 문법 지도는 딱 한 번만 생성
            doc = self.scorer.parse_document(raw_text)
            
            # 오타 교정 및 중의성 판단 (요금조회 vs 요금납부)
            step3_results = self.scorer.rescue_typos(
                doc=doc,
                masked_text=masked_text_v2,
                canon_index=self.mapper.canon_norm_index,
                alias_index=self.mapper.alias_norm_index,
                keyword_meta=self.keyword_meta
            )

            # 모든 결과를 하나로 통합
            all_matches = step1_results + step2_results + step3_results

            # 결과 집계 (동일 키워드 카운팅)
            keyword_counts: Dict[str, int] = {}
            for m in all_matches:
                kid = m["keyword_id"]
                keyword_counts[kid] = keyword_counts.get(kid, 0) + 1

            # 최종 스키마 변환
            matched_records = [
                KeywordCountRecord(
                    businessKeywordId=0, # DB ID가 필요한 경우 keyword_meta 확장 필요
                    keywordCode=kid,
                    keywordName=self.keyword_meta.get(kid, "Unknown"),
                    count=count
                ) for kid, count in keyword_counts.items()
            ]

            if matched_records:
                rows.append(ResultRecord(
                    requestId=request.request_id,
                    jobInstanceId=request.job_instance_id,
                    chunkId=chunk_id,
                    caseId=counsel.get("case_id"),
                    memberId=counsel.get("memberId"),
                    matchedKeywords=matched_records,
                    analysisVersion=request.analysis_version,
                    processedAt=now
                ))

        return rows

    def _build_search_text(self, title: str, question: str) -> str:
        # None일 수 있는 답변은 빈 문자열로 처리해 안전하게 결합
        return " ".join(part for part in [title, question] if part)

    def _build_chunk_summary(
        self,
        request: AnalyzeRequest,
        chunk_id: str,
        input_file: Path,
        manifest_path: Path,
        output_file: Path,
        record_count: int,
    ) -> dict[str, object]:
        """chunk 처리 요약 payload 생성."""
        return {
            "requestId": request.request_id,
            "jobInstanceId": request.job_instance_id,
            "chunkId": chunk_id,
            "analysisVersion": request.analysis_version,
            "inputFile": str(input_file),
            "manifestFile": str(manifest_path) if manifest_path.exists() else None,
            "outputFile": str(output_file),
            "recordCount": record_count,
            "status": "processed",
            "processedAt": datetime.now(timezone.utc).isoformat(),
        }

    def _write_json_atomic(self, path: Path, payload: dict[str, object]) -> None:
        """JSON 파일 저장(원자적 교체)."""
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        with tmp.open("w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        tmp.replace(path)
