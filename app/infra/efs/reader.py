"""EFS input file readers."""

from pathlib import Path

from app.infra.efs.jsonl import read_jsonl
from app.schemas.alias_record import AliasRecord
from app.schemas.counsel_record import CounselRecord


def read_counsel_records(path: Path) -> list[CounselRecord]:
    # 상담 JSONL(또는 JSONL gzip)을 스키마 객체 리스트로 변환
    return [CounselRecord.model_validate(row) for row in read_jsonl(path)]


def read_alias_records(path: Path) -> list[AliasRecord]:
    # 키워드/별칭 사전을 스키마 객체 리스트로 변환
    return [AliasRecord.model_validate(row) for row in read_jsonl(path)]
