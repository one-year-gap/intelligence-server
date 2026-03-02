"""로깅 설정 유틸."""

import logging


def configure_logging(debug: bool = False) -> None:
    """앱 전역 로깅 포맷/레벨 설정."""
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )
