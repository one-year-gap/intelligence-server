"""Backward-compatible batch entrypoint.

Prefer `python -m app.batch.main`.
"""

from __future__ import annotations

from pathlib import Path
import sys

# Ensure `app` imports still work when this file is executed directly.
ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.batch.main import main


if __name__ == "__main__":
    main()
