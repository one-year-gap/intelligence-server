import json
import gzip
import shutil
from pathlib import Path

from app.core.config import Settings
from app.services.analyze_service import AnalyzeService
from app.services.idempotency_service import IdempotencyService
from app.infra.state.request_registry import RequestRegistry
from app.schemas.analyze_request import AnalyzeRequest
from app.pipeline.aggregator import ResultAggregator 

def test_analyze_e2e():
    # 1. 안전한 격리 폴더 설정
    test_efs_base = Path("./e2e_test_efs")
    if test_efs_base.exists():
        shutil.rmtree(test_efs_base)

    # 2. 테스트용 가짜 데이터 준비
    job_id = "job-2026"
    version = "v1-aho"
    
    req_dir = test_efs_base / "analysis" / "req" / job_id
    ref_dir = test_efs_base / "analysis" / "ref"
    req_dir.mkdir(parents=True)
    ref_dir.mkdir(parents=True)

    # [사전 데이터 세팅] 기본 키워드와 별칭이 똑같은 '요금조회' (중복 카운트 버그 재현용)
    alias_path = ref_dir / f"{version}.alias.jsonl.gz"
    with gzip.open(alias_path, "wt", encoding="utf-8") as f:
        ref_data = [
            {
                "businessKeywordId": 100,
                "keywordCode": "BK-100",
                "keywordName": "요금조회",
                "aliases": [
                    {"aliasId": 1, "aliasText": "요금조회", "aliasNorm": "요금조회"}
                ]
            },
            {
                "businessKeywordId": 200,
                "keywordCode": "BK-200",
                "keywordName": "선택약정",
                "aliases": [
                    {"aliasId": 2, "aliasText": "아무별칭", "aliasNorm": "아무별칭"}
                ]
            }
        ]
        for d in ref_data:
            f.write(json.dumps(d, ensure_ascii=False) + "\n")

    # [상담 입력 데이터 세팅] 10번 회원이 요금조회를 2번 물어보도록 Case 3 추가
    input_path = req_dir / "chunk-01.input.jsonl.gz"
    with gzip.open(input_path, "wt", encoding="utf-8") as f:
        counsel_data = [
            {"caseId": 1, "memberId": 10, "categoryCode": "CAT-01", "title": "요금 문의", "questionText": "요금조회 해주세요", "status": "OPEN"},
            {"caseId": 2, "memberId": 20, "categoryCode": "CAT-02", "title": "약정 문의", "questionText": "그거 선텍약정 얼마에요", "status": "OPEN"},
            {"caseId": 3, "memberId": 10, "categoryCode": "CAT-01", "title": "추가 문의", "questionText": "아까 그 요금조회 다시 확인요", "status": "OPEN"}
        ]
        for d in counsel_data:
            f.write(json.dumps(d, ensure_ascii=False) + "\n")

    # 3. 분석기(AnalyzeService) 조립 및 실행
    test_settings = Settings(efs_base_dir=test_efs_base)
    registry = RequestRegistry()
    idempotency = IdempotencyService(registry)
    service = AnalyzeService(settings=test_settings, idempotency=idempotency)

    request = AnalyzeRequest(request_id="unique-req-id", job_instance_id=job_id, analysis_version=version)
    
    print("--- 1. 분석(AnalyzeService) 파이프라인 시작 ---")
    success, message = service.analyze(request)
    print(f"결과: {success}, 메시지: {message}")

    # 4. 분석 결과(mapping) 검증
    result_file = test_efs_base / "analysis" / "res" / job_id / "chunk-01.mapping.jsonl.gz"
    assert result_file.exists(), "결과 파일이 생성되지 않았습니다!"

    with gzip.open(result_file, "rt", encoding="utf-8") as f:
        results = [json.loads(line) for line in f]
        
        # [핵심 검증 1] Aho-Corasick 중복 카운트 버그가 고쳐졌는가? (count가 2가 아니라 1이어야 함)
        case1_keyword = results[0]["matchedKeywords"][0]
        assert case1_keyword["keywordCode"] == "BK-100", "Case 1 매핑 실패"
        assert case1_keyword["count"] == 1, f"중복 카운트 버그 발생! count가 1이어야 하는데 {case1_keyword['count']} 입니다."
        print(f"검증 통과: Aho-Corasick 중복 카운트 버그 수정됨 (count: {case1_keyword['count']})")
        
        # [핵심 검증 2] 오타 교정(Fallback)이 잘 되었는가?
        assert results[1]["matchedKeywords"][0]["keywordCode"] == "BK-200", "Case 2 오타 교정 매핑 실패"

    # 5. 집계기(Aggregator) 실행 및 검증
    print("\n--- 2. 집계(Aggregator) 파이프라인 시작 ---")
    aggregator = ResultAggregator(test_settings)
    aggregated_results = aggregator.aggregate_job(job_id)

    # 회원별로 그룹핑이 잘 되었는지 확인
    member_10_data = next(r for r in aggregated_results if r["memberId"] == 10)
    member_20_data = next(r for r in aggregated_results if r["memberId"] == 20)
    
    # [핵심 검증 3] 10번 회원이 '요금조회'를 총 2번(Case 1, Case 3) 물어봤으므로 totalCount는 2여야 함
    member_10_bk_100 = member_10_data["topKeywords"][0]
    assert member_10_bk_100["totalCount"] == 2, f"회원 10의 누적합 오류! 2여야 하는데 {member_10_bk_100['totalCount']} 입니다."
    
    # 파일 생성 확인
    summary_file = test_efs_base / "analysis" / "res" / job_id / "aggregated_summary.json"
    assert summary_file.exists(), "aggregated_summary.json 최종 요약 파일이 생성되지 않았습니다!"

    print("검증 통과: 회원별(memberId) 키워드 카운트 누적합(+)이 정상적으로 계산 및 저장되었습니다.")
    print("\n모든 E2E 테스트가 성공적으로 통과되었습니다!")

if __name__ == "__main__":
    test_analyze_e2e()