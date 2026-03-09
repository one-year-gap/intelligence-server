import json
import gzip
from pathlib import Path
from collections import defaultdict
from typing import Dict, Any, List

from app.core.config import Settings
from app.infra.efs.paths import build_res_dir

class ResultAggregator:
    """
    최종 결과 집계기
    AnalyzeService가 처리한 여러 chunk 결과를 모아, 
    '회원(member_id)'을 기준으로 어떤 키워드를 몇 번 문의했는지 총합을 구함
    """
    def __init__(self, settings: Settings):
        self.settings = settings

    def aggregate_job(self, job_instance_id: str) -> List[Dict[str, Any]]:
        """
        특정 Job의 모든 청크 결과를 읽어 member_id 기준 키워드 누적합 결과를 반환 및 저장
        """
        # 1. 결과 폴더 경로 탐색
        res_dir = build_res_dir(Path(self.settings.efs_base_dir), job_instance_id)
        if not res_dir.exists():
            raise FileNotFoundError(f"결과 폴더를 찾을 수 없습니다: {res_dir}")

        # 2. 집계를 위한 자료구조 세팅
        # 구조: member_counts[member_id][keyword_code] = count 누적
        member_counts = defaultdict(lambda: defaultdict(int))
        
        # 키워드 메타데이터(이름, ID)를 기억해두기 위한 딕셔너리
        keyword_meta_map = {}

        # 3. 폴더 내의 모든 mapping.jsonl.gz 파일 순회
        mapping_files = list(res_dir.glob("*.mapping.jsonl.gz"))
        if not mapping_files:
            print(f"[Aggregator] 집계할 파일이 없습니다. (job: {job_instance_id})")
            return []

        for file_path in mapping_files:
            with gzip.open(file_path, "rt", encoding="utf-8") as f:
                for line in f:
                    if not line.strip():
                        continue
                        
                    record = json.loads(line)
                    member_id = record.get("memberId")
                    matched_keywords = record.get("matchedKeywords", [])
                    
                    if not member_id or not matched_keywords:
                        continue
                        
                    # 4. 키워드별 카운트 누적 (+)
                    for mk in matched_keywords:
                        k_code = mk["keywordCode"]
                        count = mk.get("count", 1)
                        
                        # 카운트 더하기
                        member_counts[member_id][k_code] += count
                        
                        # 나중에 출력하기 위해 메타데이터 기억해두기
                        if k_code not in keyword_meta_map:
                            keyword_meta_map[k_code] = {
                                "businessKeywordId": mk["businessKeywordId"],
                                "keywordName": mk["keywordName"]
                            }

        # 5. 백엔드(Spring)가 먹기 좋게 JSON 리스트 포맷으로 변환
        final_results = []
        for member_id, counts_by_code in member_counts.items():
            keywords_list = []
            
            for k_code, total_count in counts_by_code.items():
                meta = keyword_meta_map[k_code]
                keywords_list.append({
                    "businessKeywordId": meta["businessKeywordId"],
                    "keywordCode": k_code,
                    "keywordName": meta["keywordName"],
                    "totalCount": total_count
                })
                
            # 많이 문의한 키워드가 위로 오도록 내림차순 정렬
            keywords_list.sort(key=lambda x: x["totalCount"], reverse=True)
            
            final_results.append({
                "memberId": member_id,
                "topKeywords": keywords_list
            })

        # 6. 최종 결과를 하나의 파일로 저장
        output_path = res_dir / "aggregated_summary.json"
        tmp_path = output_path.with_suffix(".tmp")
        
        with tmp_path.open("w", encoding="utf-8") as f:
            json.dump(final_results, f, ensure_ascii=False, indent=2)
        tmp_path.replace(output_path)
        
        print(f"[Aggregator] 집계 완료! 총 {len(final_results)}명의 회원 통계가 저장되었습니다: {output_path}")
        return final_results