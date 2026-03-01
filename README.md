# counseling-analytics

FastAPI 개발 서버 실행 가이드입니다.

## Prerequisites

- Python 3.9+
- `pip` 사용 가능 환경

## 1) 프로젝트 루트로 이동

```bash
cd /Users/kimdoyeon/PycharmProjects/counseling-analytics
```

## 2) 가상환경 생성

### macOS / Linux

```bash
python3 -m venv .venv
```

### Windows (PowerShell)

```powershell
py -m venv .venv
```

## 3) 가상환경 활성화

### macOS / Linux

```bash
source .venv/bin/activate
```

### Windows (PowerShell)

```powershell
.venv\Scripts\Activate.ps1
```

Windows에서 실행 정책 오류가 나면(최초 1회):

```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

## 4) 의존성 설치

```bash
pip install fastapi "uvicorn[standard]"
```

## 5) FastAPI 서버 실행

```bash
python -m uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

## 6) 접속 확인

- API: http://127.0.0.1:8000
- Swagger UI: http://127.0.0.1:8000/docs
- ReDoc: http://127.0.0.1:8000/redoc

---

## Directory Structure
```
python-analysis/
├─ app/
│  ├─ main.py                      # FastAPI 앱 엔트리포인트
│  ├─ api/
│  │  ├─ deps.py                   # 공통 의존성 주입
│  │  ├─ router.py                 # APIRouter 통합
│  │  └─ v1/
│  │     ├─ analyze.py             # POST /analyze
│  │     ├─ health.py              # /health, /ready
│  │     └─ ops.py                 # 운영/상태 조회용 API
│  │
│  ├─ core/
│  │  ├─ config.py                 # 환경변수, 설정(Pydantic Settings)
│  │  ├─ logging.py                # 로깅 설정
│  │  ├─ enums.py                  # 공통 Enum
│  │  ├─ constants.py              # 경로, 파일 suffix, 재시도 상수
│  │  └─ exceptions.py             # 커스텀 예외
│  │
│  ├─ schemas/
│  │  ├─ analyze_request.py        # 요청 DTO
│  │  ├─ analyze_response.py       # 응답 DTO
│  │  ├─ callback_request.py       # Spring 콜백 DTO
│  │  ├─ counsel_record.py         # 상담 JSONL 레코드 스키마
│  │  ├─ alias_record.py           # 별칭 JSONL 레코드 스키마
│  │  ├─ result_record.py          # 결과 JSONL 레코드 스키마
│  │  └─ aggregate_record.py       # 개인별 집계 JSONL 스키마
│  │
│  ├─ services/
│  │  ├─ analyze_service.py        # 전체 분석 흐름 orchestration
│  │  ├─ alias_loader_service.py   # 별칭 파일 로딩/캐싱
│  │  ├─ result_writer_service.py  # 결과 파일 저장(tmp→rename)
│  │  ├─ callback_service.py       # Spring 결과 전송
│  │  └─ idempotency_service.py    # requestId 중복 처리 방지
│  │
│  ├─ pipeline/
│  │  ├─ normalizer.py             # 텍스트 정규화
│  │  ├─ extractor.py              # spaCy 기반 키워드 추출
│  │  ├─ mapper.py                 # Damerau-Levenshtein 매핑
│  │  ├─ scorer.py                 # match score 계산
│  │  └─ aggregator.py             # member_id 기준 키워드 집계
│  │
│  ├─ infra/
│  │  ├─ efs/
│  │  │  ├─ reader.py              # EFS 파일 읽기
│  │  │  ├─ writer.py              # EFS 파일 쓰기
│  │  │  ├─ jsonl.py               # JSONL stream read/write
│  │  │  └─ paths.py               # req/ref/result 경로 유틸
│  │  │
│  │  │
│  │  ├─ cache/
│  │  │  └─ alias_cache.py         # jobInstanceId별 alias 캐시
│  │  │
│  │  └─ state/
│  │     ├─ request_registry.py    # 처리 상태 기록(파일/redis/sqlite 등)
│  │     └─ lock.py                # 중복 실행 방지 락
│  │
│  │
│  └─ utils/
│     ├─ time.py
│     ├─ hashing.py
│     └─ retry.py
│
├─ tests/
│  ├─ api/
│  ├─ services/
│  ├─ pipeline/
│  └─ infra/
│
├─ requirements.txt
├─ Dockerfile
└─ .env**b**

```