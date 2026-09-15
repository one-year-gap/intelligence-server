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

## 비교 실행과 채점

`evaluation.py`는 짝이 빠지거나 최초 입력·응답이 달라진 비교를 거부한다. `keyword_score.py`는 사람 정답과 키워드 집합을 대조한다. 정답 파일은 모델 입력에 포함하지 않는다.

`evaluate.py`, `replay_catalog.py`, `run_human50.py`는 기존 실험의 실행 도구다. 유료 실행에는 `OPENAI_API_KEY` 환경변수가 필요하다. 개인 PC의 키 파일을 읽지 않는다. HTTP 클라이언트는 `experiment.py`에 있다.

새 개발 실행은 `inputs/`에 원래 형식의 입력 자료를 준비한 뒤 생성한다. 상담 원문·정답·응답 기록·SQLite 상태 파일은 이 저장소에 포함하지 않는다.

- `regression`: `semantic_regressions.json`의 `id`, `text`, `required`, `forbidden`, `sentiment`와 `business_keyword_alias_map.snapshot.json`이 필요하다.
- `development`: `dataset_summary.json`의 `source_root`, `development_50.jsonl`의 원본 경로·레코드 번호·해시, 같은 사전 파일이 필요하다.

```sh
.venv/bin/python evaluate.py prepare --name development_local --dataset regression
# 입력과 protocol.json을 확인하고 API 키를 환경에 설정한 뒤 실행한다.
.venv/bin/python evaluate.py run --name development_local
```

준비 단계는 API를 호출하지 않는다. 실행은 최대 50건으로 제한한다. `run_human50.py`는 별도로 동결한 `runs/human_keywords_v1`이 있어야 하며, 원자료 없이 실행할 수 없다. `replay_catalog.py`는 완료된 `runs/development_v2`의 최초 응답 50개를 재사용한다.

원본 실험의 코드 스냅샷 해시는 원본에 대한 기록이다. 경로 정리 후 코드는 해시가 달라지므로 과거 실행의 protocol을 그대로 재사용하지 않는다. 새 실행을 준비하면 현재 소스와 입력으로 새 해시가 만들어진다.

## 결과와 검증 기록

- [비교 결과와 한계](docs/RESULTS.md)
- [테스트 성공·실패 기록](docs/TESTS.md)
