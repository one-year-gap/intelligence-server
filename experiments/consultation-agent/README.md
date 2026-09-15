# 상담 분석 MCP 실험

상담에서 키워드와 감정을 추출하는 네 실행 방식을 비교한다. 모든 방식에 전체 상담과 같은 키워드 사전을 제공한다. 구조 검사 통과는 의미상 정답 판정이 아니다.

## 로컬 테스트

Python 3.12에서 확인했다. 저장소 루트에서 실행한다.

```sh
cd experiments/consultation-agent
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/python -m unittest discover -s tests -v
```

테스트는 합성 상담과 모의 모델을 사용한다. 실제 MCP stdio 서버 연결도 검사하며 유료 모델 호출은 하지 않는다. 기존 서버의 의존성과 별도의 가상환경을 사용한다.

## 구성

| 파일 | 역할 |
|---|---|
| contract.py | 공통 입력과 출력 계약, 고객 발화 근거 복원 |
| runner.py | 단일 호출, 조건부 재검토, 반복 재검토, MCP 도구 실행 |
| journal.py | SQLite 단계 기록과 완료 응답 재사용 |
| tool_backend.py | 상담 발화와 키워드 사전 조회, 구조 검사 |
| mcp_server.py / mcp_transport.py | stdio 서버와 클라이언트 연결 |
| operational_benchmark.py | 저장 시점별 장애 주입 비교 |

## 적용 범위

독립 실행하는 후속 실험 코드다. FastAPI 서비스와 연결한 운영 기능은 아니다. 응답을 받았는지 확인할 수 없는 중단은 `STOPPED_UNCERTAIN`으로 남기고 자동 재호출하지 않는다. 신고, 고객 점수 변경, 블랙리스트 처리는 구현하지 않았다.
