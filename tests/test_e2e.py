import json
import gzip
import shutil
from pathlib import Path
from app.core.config import Settings
from app.services.analyze_service import AnalyzeService
from app.services.idempotency_service import IdempotencyService
from app.infra.state.request_registry import RequestRegistry
from app.schemas.analyze_request import AnalyzeRequest

def test_analyze_e2e():
    # 1. 안전한 격리 폴더 설정 (경로 오버라이딩)
    test_efs_base = Path("./e2e_test_efs")
    if test_efs_base.exists():
        shutil.rmtree(test_efs_base)

    # 2. 테스트용 가짜 데이터 준비 (paths.py 규칙 준수)
    job_id = "job-2026"
    version = "v1-aho"
    
    # 경로 생성
    req_dir = test_efs_base / "analysis" / "req" / job_id
    ref_dir = test_efs_base / "analysis" / "ref"
    req_dir.mkdir(parents=True)
    ref_dir.mkdir(parents=True)

    # 가짜 사전 데이터 생성 (.alias.jsonl.gz)
    # AliasRecord 데이터
    alias_path = ref_dir / f"{version}.alias.jsonl.gz"
    with gzip.open(alias_path, "wt", encoding="utf-8") as f:
        ref_data = [
            {
                "businessKeywordId": 100,
                "keywordCode": "BK-100",
                "keywordName": "요금조회",
                "aliases": [
                    {
                        "aliasId": 1,        # 필수 추가
                        "aliasText": "요금조회",
                        "aliasNorm": "요금조회"
                        # match_mode는 제거 (모델에 없음)
                    }
                ]
            },
            {
                "businessKeywordId": 200,
                "keywordCode": "BK-200",
                "keywordName": "선택약정",
                "aliases": [
                    {
                        "aliasId": 2,        # 필수 추가
                        "aliasText": "선텍약정",
                        "aliasNorm": "선텍약정"
                    }
                ]
            }
        ]
        for d in ref_data:
            f.write(json.dumps(d, ensure_ascii=False) + "\n")

    # 가짜 상담 입력 데이터 생성 (.input.jsonl.gz)
    # CounselRecord 데이터
    input_path = req_dir / "chunk-01.input.jsonl.gz"
    with gzip.open(input_path, "wt", encoding="utf-8") as f:
        counsel_data = [
            {
                "caseId": 1,
                "memberId": 10,
                "categoryCode": "CAT-01",
                "title": "요금 문의",
                "questionText": "요금조회 해주세요", # 분석 대상
                "status": "OPEN"
            },
            {
                "caseId": 2,
                "memberId": 20,
                "categoryCode": "CAT-02",
                "title": "약정 문의",
                "questionText": "선텍약정 얼마에요", # 분석 대상
                "status": "OPEN"
            }
        ]
        for d in counsel_data:
            f.write(json.dumps(d, ensure_ascii=False) + "\n")

    # 3. 서비스 조립 (진짜 설정 대신 가짜 설정을 주입)
    test_settings = Settings(efs_base_dir=test_efs_base)
    registry = RequestRegistry()
    idempotency = IdempotencyService(registry)
    service = AnalyzeService(settings=test_settings, idempotency=idempotency)

    # 4. 실행 (지휘자에게 명령 내리기)
    request = AnalyzeRequest(
        request_id="unique-req-id",
        job_instance_id=job_id,
        analysis_version=version
    )
    
    print("--- 분석 시작 ---")
    success, message = service.analyze(request)
    print(f"결과: {success}, 메시지: {message}")

    # 5. 검증 (결과 파일이 진짜 생겼고, 압축되어 있는가?)
    result_file = test_efs_base / "analysis" / "res" / job_id / "chunk-01.mapping.jsonl.gz"
    assert result_file.exists(), "결과 파일이 생성되지 않았습니다!"

    # 압축된 결과 파일 열어서 실제 분석 내용 검증 
    with gzip.open(result_file, "rt", encoding="utf-8") as f:
        print("\n--- 최종 분석 결과 파일 내용 및 검증 ---")
        
        # 파일 내용을 한 줄씩 읽어서 파이썬 딕셔너리로 변환
        results = [json.loads(line) for line in f]

        # 터미널에서 파일 내용 확인
        print("[생성된 파일 내용]")
        for i, res_dict in enumerate(results):
            print(f"[{i+1}번째 기록]")
            print(json.dumps(res_dict, ensure_ascii=False, indent=2))
        print("-" * 40)
        
        # 1. 2개의 상담 데이터를 넣었으니, 결과도 2개가 나와야 함
        assert len(results) == 2, f"예상한 결과 갯수(2)와 다름: {len(results)}"
        
        # 2. 첫 번째 상담("요금조회 해주세요") 검증
        # -> BK-100 키워드가 매칭되어야 함
        assert results[0]["matchedKeywords"][0]["keywordCode"] == "BK-100", "첫 번째 데이터 분석 실패"
        print(f"검증 통과: Case 1 매핑 결과 - {results[0]['matchedKeywords'][0]['keywordCode']}")
        
        # 3. 두 번째 상담("선텍약정 얼마에요" - 오타) 검증
        # -> 패자부활전(Fallback)을 통해 BK-200 키워드가 매칭되어야 함
        assert results[1]["matchedKeywords"][0]["keywordCode"] == "BK-200", "두 번째 데이터 분석 실패 (오타 교정 실패)"
        print(f"검증 통과: Case 2 매핑 결과 - {results[1]['matchedKeywords'][0]['keywordCode']}")

if __name__ == "__main__":
    test_analyze_e2e()