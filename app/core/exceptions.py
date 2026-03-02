"""도메인 예외 정의."""


class AppError(Exception):
    """Base application exception."""


class InvalidPathError(AppError):
    """EFS base 경로 밖 접근 시 발생."""


class DuplicateRequestError(AppError):
    """이미 처리된 requestId 사용 시 발생."""
