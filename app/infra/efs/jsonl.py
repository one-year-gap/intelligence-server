"""JSONL/.jsonl.gz reader/writer utilities."""

import gzip
import json
from pathlib import Path
from typing import Any, Generator, TextIO


def _is_gzip(path: Path) -> bool:
    return path.name.endswith(".gz")


def _open_read(path: Path) -> TextIO:
    if _is_gzip(path):
        return gzip.open(path, "rt", encoding="utf-8")
    return path.open("r", encoding="utf-8")


def _open_write(path: Path) -> TextIO:
    if _is_gzip(path):
        return gzip.open(path, "wt", encoding="utf-8")
    return path.open("w", encoding="utf-8")


def read_jsonl(path: Path) -> Generator[dict[str, Any], None, None]:
    with _open_read(path) as f:
        for line_no, line in enumerate(f, start=1):
            content = line.strip()
            if not content:
                continue
            try:
                yield json.loads(content)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSONL at {path}:{line_no}") from exc


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")

    with _open_write(tmp) as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    tmp.replace(path)
