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
import gzip

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
from app.pipeline.normalizer import normalize_with_offsets

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

        # Name을 동시에 저장하도록 구조 변경
        # { "BK-100": {"id": 100, "name": "요금조회"} }
        self.keyword_meta: Dict[str, Dict[str, Any]] = {}

    def _initialize_pipeline(self, alias_records: List[Any]): # AliasRecord 객체 리스트
        """
        [사전 적재] Pydantic 객체 데이터를 엔진들이 쓸 수 있는 리스트로 가공
        """
        dict_data_for_pipeline = []
        
        for record in alias_records:
            k_code = str(record.keyword_code)
            
            # 메타데이터에 실제 DB ID와 이름을 함께 저장
            self.keyword_meta[k_code] = {
                "id": record.business_keyword_id,
                "name": record.keyword_name
            }
            
            # 1단계/2단계용 데이터 포맷팅
            dict_data_for_pipeline.append({
                "schema": "dict.keyword.v1",
                "label_id": k_code,
                "business_keyword": record.keyword_name
            })

            for alias in record.aliases:
                dict_data_for_pipeline.append({
                    "schema": "dict.alias.v1",
                    "label_id": k_code,
                    "business_keyword": record.keyword_name,
                    "alias_text": alias.alias_text,
                    "alias_norm": alias.alias_norm if alias.alias_norm else alias.alias_text
                })

        # 가공된 데이터를 엔진들에게 전송
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
                # write_result_records(mapping_path, results)
                self._write_atomic(mapping_path, results, is_jsonl=True)

                chunk_summary = self._build_chunk_summary(
                    request=request,
                    chunk_id=chunk_id,
                    input_file=input_file,
                    manifest_path=manifest_path,
                    output_file=mapping_path,
                    record_count=len(results),
                )
                # self._write_json_atomic(chunk_summary_path, chunk_summary)
                self._write_atomic(chunk_summary_path, chunk_summary, is_jsonl=False)

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

    def _run_full_pipeline(self, text: str) -> List[Dict[str, Any]]:
        """
        [3단계 연쇄 반응] 
        정규화 지도를 활용해 원문 위치를 보존하며 1->2->3단계를 실행
        """
        # 0. 기초 세팅: 분석용 정규화 텍스트와 위치 지도 생성
        # norm_text: "u+ tv 안나와요" -> "utv안나와요"
        # offset_map: [0, 3, 4, 6, 7, 8, 9] (각 글자가 원본의 몇 번째인지 기록)
        norm_text, offset_map = normalize_with_offsets(text)
        if not norm_text:
            return []

        # --- 1단계: 완전 일치 (Exact Match) ---
        # 원문(text) 전체가 사전에 있는지 확인
        step1_results = self.mapper.exact_match(text)
        
        # 1단계 결과 마스킹 (찾은 곳은 '*'로 가리기)
        masked_raw = self._apply_masking(text, step1_results)

        # --- 2단계: 부분 일치 (Aho-Corasick) ---
        # 마스킹된 원문을 다시 정규화하여 2단계용 텍스트 생성
        # (이미 1단계에서 찾은 부분은 정규화 결과에서도 '*'로 남거나 사라짐)
        norm_masked, _ = normalize_with_offsets(masked_raw)
      
        step2_results = self.extractor.extract_keywords(norm_masked, offset_map)
        
        # 2단계 결과까지 합산하여 다시 마스킹
        all_matches_so_far = step1_results + step2_results
        masked_v2 = self._apply_masking(text, all_matches_so_far)

        # --- 3단계: 오타 교정 및 중의성 해소 (Context Scorer) ---
        doc = self.scorer.parse_document(text)
        step3_results = self.scorer.rescue_typos(
            doc=doc,
            masked_text=masked_v2, # 1, 2단계가 모두 가려진 텍스트 전달
            canon_index=self.mapper.canon_norm_index,
            alias_index=self.mapper.alias_norm_index,
            keyword_meta=self.keyword_meta
        )

        return step1_results + step2_results + step3_results

    def _build_results(self, request: AnalyzeRequest, counsel_records, alias_records, chunk_id: str) -> list[ResultRecord]:
        """
        상담 1건마다 키워드별 횟수를 계산해 결과 생성.
        """
        rows: list[ResultRecord] = []
        now = datetime.now(timezone.utc)

        for counsel in counsel_records:
            # 수정: Pydantic 객체이므로 .get() 대신 속성으로 접근
            # title과 question_text를 합쳐서 풍부한 문맥으로 분석
            full_text = f"{counsel.title} {counsel.question_text}"
            
            # 3단계 파이프라인 통합 실행
            all_matches = self._run_full_pipeline(full_text)

            # 결과 집계 (동일 키워드 카운팅)
            keyword_counts: Dict[str, int] = {}
            for m in all_matches:
                kid = m["keyword_id"]
                keyword_counts[kid] = keyword_counts.get(kid, 0) + 1

            # 최종 스키마 변환
            matched_records = []
            for k_code, count in keyword_counts.items():
                # keyword_meta에서 실제 ID와 Name 추출
                meta = self.keyword_meta.get(k_code, {"id": 0, "name": "Unknown"})
                matched_records.append(
                    KeywordCountRecord(
                        businessKeywordId=meta["id"],
                        keywordCode=k_code,
                        keywordName=meta["name"],
                        count=count
                    )
                )

            if matched_records:
                rows.append(ResultRecord(
                    requestId=request.request_id,
                    jobInstanceId=request.job_instance_id,
                    chunkId=chunk_id,
                    caseId=counsel.case_id,      # .case_id 속성 접근
                    memberId=counsel.member_id,  # .member_id 속성 접근
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

    def _write_atomic(self, path: Path, data: Any, is_jsonl: bool = False) -> None:
        """
        [로컬 세이프 라이터] 
        공용 infra 코드를 수정하지 않고, 우리 서비스에서 압축과 원자적 저장을 직접 처리
        """
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        
        # 파일 확장자가 .gz로 끝나면 gzip으로 열고, 아니면 일반 open으로 연다
        is_gzip = path.name.endswith(".gz")
        opener = gzip.open(tmp, "wt", encoding="utf-8") if is_gzip else tmp.open("w", encoding="utf-8")
        
        with opener as f:
            if is_jsonl:
                # 결과 데이터(List)를 한 줄씩 저장
                for row in data:
                    # mode='json'을 추가하여 날짜를 문자열로 자동 변환
                    if hasattr(row, "model_dump"):
                        d = row.model_dump(mode='json', by_alias=True)
                    else:
                        d = row
                    
                    # 혹시 모를 상황을 대비해 default=str을 넣어주면 더 안전
                    f.write(json.dumps(d, ensure_ascii=False, default=str) + "\n")
            else:
                # 요약 데이터 저장 시에도 날짜가 있을 수 있으므로 default=str 추가
                json.dump(data, f, ensure_ascii=False, indent=2, default=str)
        
        tmp.replace(path)
