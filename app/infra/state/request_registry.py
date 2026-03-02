# 요청 처리 상태 저장소(in-memory)

class RequestRegistry:
    def __init__(self) -> None:
        self._state: dict[str, dict[str, str]] = {}

    def get(self, request_id: str) -> dict[str, str] | None:
        """요청 상태 조회"""
        return self._state.get(request_id)

    def create_if_absent(self, request_id: str, status: str) -> bool:
        """없을 때만 생성. 이미 있으면 False."""
        if request_id in self._state:
            return False

        self._state[request_id] = {"status": status}
        return True

    def update_status(self, request_id: str, status: str) -> None:
        """요청 상태 변경."""
        item = self._state.get(request_id, {})
        item["status"] = status
        self._state[request_id] = item
