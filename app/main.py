"""Backward-compatible entrypoint.

Prefer `app.realtime.main` for the realtime API server.
"""

from app.realtime.main import app, run


if __name__ == "__main__":
    run()
