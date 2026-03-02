"""프로세스 내 중복 실행 방지용 전역 락."""

from contextlib import contextmanager
from threading import Lock

_LOCK = Lock()


@contextmanager
def process_lock():
    """임계 구역 보호 컨텍스트."""
    _LOCK.acquire()
    try:
        yield
    finally:
        _LOCK.release()
