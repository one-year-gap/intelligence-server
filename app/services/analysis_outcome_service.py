from datetime import datetime, timezone
from typing import Any

from app.schemas.analysis_request_message import AnalysisRequestMessage


class AnalysisOutcomeService:
    def __init__(self, result_limit: int) -> None:
        self._result_limit = max(1, result_limit)

    def build_message_outcomes(
        self,
        batch: list[AnalysisRequestMessage],
        target_by_pair: dict[tuple[int, int], Any],
        outbox_metadata_by_request_id: dict[str, dict[str, Any]],
        mapping_rows: list[tuple[int, int, int]],
        completed_ids: list[int],
        failed_items: list[tuple[int, str]],
        keyword_info_by_id: dict[int, dict[str, str]],
    ) -> list[dict[str, Any]]:
        failed_by_analysis_id = {int(analysis_id): error for analysis_id, error in failed_items}
        completed_id_set = {int(analysis_id) for analysis_id in completed_ids}
        produced_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

        mapping_summary_by_analysis_id: dict[int, dict[str, int]] = {}
        mapping_detail_by_analysis_id: dict[int, dict[int, int]] = {}
        for analysis_id, _, count in mapping_rows:
            key = int(analysis_id)
            summary = mapping_summary_by_analysis_id.setdefault(
                key,
                {"keywordTypes": 0, "keywordHits": 0},
            )
            summary["keywordTypes"] += 1
            summary["keywordHits"] += int(count)

        for analysis_id, keyword_id, count in mapping_rows:
            detail = mapping_detail_by_analysis_id.setdefault(int(analysis_id), {})
            key = int(keyword_id)
            detail[key] = detail.get(key, 0) + int(count)

        outcomes: list[dict[str, Any]] = []
        for message in batch:
            pair = (message.case_id, message.analyzer_version)
            target = target_by_pair.get(pair)
            outbox_metadata = outbox_metadata_by_request_id.get(message.dispatch_request_id, {})
            chunk_id = outbox_metadata.get("chunkId")

            if target is None:
                outcomes.append(
                    {
                        "type": "RESPONSE",
                        "schema": "analysis.response.v1",
                        "dispatchRequestId": message.dispatch_request_id,
                        "chunkId": chunk_id,
                        "caseId": message.case_id,
                        "analyzerVersion": message.analyzer_version,
                        "analysisId": None,
                        "memberId": None,
                        "status": "MISSING_TARGET",
                        "keywordTypes": 0,
                        "keywordHits": 0,
                        "keywordCounts": [],
                        "error": "target not found",
                        "producedAt": produced_at,
                    }
                )
                continue

            analysis_id = int(target["analysis_id"])
            member_id = int(target["member_id"])
            summary = mapping_summary_by_analysis_id.get(analysis_id, {"keywordTypes": 0, "keywordHits": 0})
            detail = mapping_detail_by_analysis_id.get(analysis_id, {})
            error_message = failed_by_analysis_id.get(analysis_id)

            if error_message is not None:
                status = "FAILED"
            elif analysis_id in completed_id_set:
                status = "COMPLETED"
            else:
                status = "UNKNOWN"

            keyword_counts = [
                {
                    "keywordId": keyword_id,
                    "businessKeywordId": keyword_id,
                    "keywordCode": keyword_info_by_id.get(keyword_id, {}).get("keywordCode", "-"),
                    "keywordName": keyword_info_by_id.get(keyword_id, {}).get("keywordName", "-"),
                    "count": count,
                }
                for keyword_id, count in sorted(detail.items(), key=lambda item: (-item[1], item[0]))[:self._result_limit]
            ]
            outcomes.append(
                {
                    "type": "RESPONSE",
                    "schema": "analysis.response.v1",
                    "dispatchRequestId": message.dispatch_request_id,
                    "chunkId": chunk_id,
                    "caseId": message.case_id,
                    "analyzerVersion": message.analyzer_version,
                    "analysisId": analysis_id,
                    "memberId": member_id,
                    "status": status,
                    "keywordTypes": summary["keywordTypes"],
                    "keywordHits": summary["keywordHits"],
                    "keywordCounts": keyword_counts,
                    "error": error_message,
                    "producedAt": produced_at,
                }
            )

        return outcomes
