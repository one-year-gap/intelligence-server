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


class AnalyzeService:
    def __init__(
        self,
        settings: Settings,
        idempotency: IdempotencyService,
    ) -> None:
        self.settings = settings
        self.idempotency = idempotency

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
        """상담 1건마다 키워드별 횟수를 계산해 결과 생성."""
        rows: list[ResultRecord] = []
        now = datetime.now(timezone.utc)

        # alias 사전을 정규식 목록으로 미리 컴파일해, 상담 루프에서 재사용
        keyword_patterns: list[tuple[int, str, str, list[re.Pattern[str]]]] = []
        for alias_record in alias_records:
            patterns: list[re.Pattern[str]] = []
            for alias in alias_record.aliases:
                text = (alias.alias_norm or alias.alias_text).strip()
                if not text:
                    continue
                # 대소문자 무시하고 단순 부분문자열 카운트
                patterns.append(re.compile(re.escape(text), flags=re.IGNORECASE))

            keyword_patterns.append(
                (
                    alias_record.business_keyword_id,
                    alias_record.keyword_code,
                    alias_record.keyword_name,
                    patterns,
                )
            )

        for counsel in counsel_records:
            # title/question를 합쳐 한 번에 검색
            text = self._build_search_text(counsel.title, counsel.question_text)
            keyword_counts: list[KeywordCountRecord] = []

            for keyword_id, keyword_code, keyword_name, patterns in keyword_patterns:
                count = 0
                for pattern in patterns:
                    # 같은 키워드에 여러 alias가 있으면 합산 카운트
                    count += len(pattern.findall(text))

                if count > 0:
                    keyword_counts.append(
                        KeywordCountRecord(
                            businessKeywordId=keyword_id,
                            keywordCode=keyword_code,
                            keywordName=keyword_name,
                            count=count,
                        )
                    )

            rows.append(
                ResultRecord(
                    requestId=request.request_id,
                    jobInstanceId=request.job_instance_id,
                    chunkId=chunk_id,
                    caseId=counsel.case_id,
                    memberId=counsel.member_id,
                    matchedKeywords=keyword_counts,
                    analysisVersion=request.analysis_version,
                    processedAt=now,
                )
            )

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
