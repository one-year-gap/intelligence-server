import json
import gzip
import shutil
from pathlib import Path
from app.core.config import Settings
from app.pipeline.aggregator import ResultAggregator

def test_member_aggregation():
    # 1. 격리된 테스트 폴더 준비
    test_efs_base = Path("./e2e_test_efs_agg")
    job_id = "job-9999"
    res_dir = test_efs_base / "analysis" / "res" / job_id
    
    if test_efs_base.exists():
        shutil.rmtree(test_efs_base)
    res_dir.mkdir(parents=True)

    # 2. 가짜 Chunk 1 결과 생성 (회원 10번이 요금조회 1번, 회원 20번이 요금조회 1번)
    chunk1_path = res_dir / "chunk-01.mapping.jsonl.gz"
    with gzip.open(chunk1_path, "wt", encoding="utf-8") as f:
        f.write(json.dumps({"memberId": 10, "matchedKeywords": [{"businessKeywordId": 100, "keywordCode": "BK-100", "keywordName": "요금조회", "count": 1}]}) + "\n")
        f.write(json.dumps({"memberId": 20, "matchedKeywords": [{"businessKeywordId": 100, "keywordCode": "BK-100", "keywordName": "요금조회", "count": 1}]}) + "\n")

    # 3. 가짜 Chunk 2 결과 생성 (회원 10번이 요금조회 2번 또 함, 그리고 결합할인 1번)
    chunk2_path = res_dir / "chunk-02.mapping.jsonl.gz"
    with gzip.open(chunk2_path, "wt", encoding="utf-8") as f:
        f.write(json.dumps({"memberId": 10, "matchedKeywords": [{"businessKeywordId": 100, "keywordCode": "BK-100", "keywordName": "요금조회", "count": 2}]}) + "\n")
        f.write(json.dumps({"memberId": 10, "matchedKeywords": [{"businessKeywordId": 300, "keywordCode": "BK-300", "keywordName": "결합할인", "count": 1}]}) + "\n")

    # 4. 집계기(Aggregator) 실행
    settings = Settings(efs_base_dir=test_efs_base)
    aggregator = ResultAggregator(settings)
    
    print("--- Aggregator 집계 시작 ---")
    results = aggregator.aggregate_job(job_id)

    # 5. 결과 검증
    print("\n[집계 결과 확인]")
    print(json.dumps(results, ensure_ascii=False, indent=2))

    # 회원 10번의 데이터 검증 (Chunk1에서 1번 + Chunk2에서 2번 = 총 3번이어야 함)
    member_10_data = next(r for r in results if r["memberId"] == 10)
    bk_100_data = next(k for k in member_10_data["topKeywords"] if k["keywordCode"] == "BK-100")
    
    assert bk_100_data["totalCount"] == 3, f"회원 10의 요금조회 카운트 오류: {bk_100_data['totalCount']}"
    assert member_10_data["topKeywords"][0]["keywordCode"] == "BK-100", "가장 많이 나온 키워드가 1등으로 정렬되지 않았음"
    
    print("\n검증 통과: 누적합(+) 및 정렬 로직이 정상적으로 작동합니다")

    # 파일이 실제로 잘 저장되었는지 확인
    summary_file = res_dir / "aggregated_summary.json"
    assert summary_file.exists(), "aggregated_summary.json 파일이 생성되지 않았습니다!"
    print(f"검증 통과: 최종 요약 파일이 정상적으로 저장되었습니다 ({summary_file})")

if __name__ == "__main__":
    test_member_aggregation()