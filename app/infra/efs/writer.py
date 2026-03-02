"""EFS output file writers."""

from pathlib import Path

from app.infra.efs.jsonl import write_jsonl
from app.schemas.result_record import ResultRecord


def write_result_records(path: Path, rows: list[ResultRecord]) -> None:
    payload = [row.model_dump(by_alias=True, mode="json") for row in rows]
    write_jsonl(path, payload)
