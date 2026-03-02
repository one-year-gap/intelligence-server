# EFS 경로 처리 유틸
from pathlib import Path

from app.core.exceptions import InvalidPathError

INPUT_SUFFIX = ".input.jsonl.gz"
MANIFEST_SUFFIX = ".manifest.json"
MAPPING_SUFFIX = ".mapping.jsonl.gz"
CHUNK_SUMMARY_SUFFIX = ".chunk.json"
ALIAS_FILE_SUFFIX = ".alias.jsonl.gz"


def resolve_efs_path(base_dir: Path, requested_path: str) -> Path:
    """요청 경로를 절대경로로 변환하고 base_dir 내부인지 검증."""
    raw = Path(requested_path)
    full = raw if raw.is_absolute() else (base_dir / raw)
    resolved = full.resolve()
    base_resolved = base_dir.resolve()

    if not str(resolved).startswith(str(base_resolved)):
        raise InvalidPathError(f"Path is outside EFS base dir: {requested_path}")
    return resolved


def build_req_dir(base_dir: Path, job_instance_id: str) -> Path:
    # Spring이 올려둔 입력 chunk들이 있는 위치
    return (base_dir / "analysis" / "req" / job_instance_id).resolve()


def build_res_dir(base_dir: Path, job_instance_id: str) -> Path:
    # Python이 처리 결과를 저장하는 위치
    return (base_dir / "analysis" / "res" / job_instance_id).resolve()


def build_ref_alias_path(base_dir: Path, analysis_version: str) -> Path:
    # 예: /mnt/efs/analysis/ref/v1.alias.jsonl.gz
    return (base_dir / "analysis" / "ref" / f"{analysis_version}{ALIAS_FILE_SUFFIX}").resolve()


def list_chunk_inputs(req_dir: Path) -> list[Path]:
    # 처리 대상 입력 파일 규칙: {chunkId}.input.jsonl.gz
    return sorted(req_dir.glob(f"*{INPUT_SUFFIX}"))


def parse_chunk_id(input_path: Path) -> str:
    name = input_path.name
    if not name.endswith(INPUT_SUFFIX):
        raise ValueError(f"invalid input file name: {name}")
    return name[: -len(INPUT_SUFFIX)]


def build_manifest_path(req_dir: Path, chunk_id: str) -> Path:
    return req_dir / f"{chunk_id}{MANIFEST_SUFFIX}"


def build_output_paths(base_dir: Path, job_instance_id: str, chunk_id: str) -> tuple[Path, Path]:
    res_dir = build_res_dir(base_dir, job_instance_id)
    mapping_path = res_dir / f"{chunk_id}{MAPPING_SUFFIX}"
    chunk_summary_path = res_dir / f"{chunk_id}{CHUNK_SUMMARY_SUFFIX}"
    return mapping_path, chunk_summary_path
