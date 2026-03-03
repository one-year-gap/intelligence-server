from collections import defaultdict
from typing import List, Dict

# 텍스트 정규화 모듈
from app.pipeline.normalizer import normalize

class ExactMapper:
    # 스키마 상수를 클래스 변수로 정의 (오타 방지 및 유지보수성 향상)
    _KEYWORD_SCHEMA = "dict.keyword.v1"
    _ALIAS_SCHEMA = "dict.alias.v1"

    def __init__(self):
        # 두 개의 O(1) 검색용 메모리 인덱스(Hash Map) 초기화
        # 구조: { "정규화된_텍스트": ["label_id_1", "label_id_2"] }
        # List를 사용하는 이유: 동일한 별칭이 여러 표준 키워드에 매핑되는 '다중 충돌(Collision)' 상황을 허용하기 위함
        
        # 1. 표준 키워드(Canonical) 전용 인덱스
        self.canon_norm_index: Dict[str, List[str]] = defaultdict(list)
        # 2. 별칭(Alias) 전용 인덱스
        self.alias_norm_index: Dict[str, List[str]] = defaultdict(list)

    def build_index(self, keyword_data_list: List[dict]):
        """
        [초기화 단계] 서버 시작 시 1회 실행
        EFS에서 로드한 사전 데이터(JSONL)를 파싱하여 인덱스를 메모리에 적재
        """
        for item in keyword_data_list:
            # 공통 식별자인 정답 라벨 ID (예: 'BK-020') 추출
            label_id = item.get("label_id")
            if not label_id:
                continue

            # 데이터 유형(표준 키워드 vs 별칭)을 구분하는 schema 필드 확인
            schema = item.get("schema", "")

            # 1. 표준 키워드(Canonical) 데이터 적재
            if schema == self._KEYWORD_SCHEMA:
                business_keyword = item.get("business_keyword")
                if business_keyword:
                    # 텍스트 정규화 수행 후 인덱스에 매핑
                    norm_canon = normalize(business_keyword)
                    if norm_canon:
                        self.canon_norm_index[norm_canon].append(label_id)

            # 2. 별칭(Alias) 데이터 적재
            elif schema == self._ALIAS_SCHEMA:
                norm_alias = item.get("alias_norm")
                if norm_alias:
                    self.alias_norm_index[norm_alias].append(label_id)

    def exact_match(self, raw_text: str) -> List[str]:
        """
        [분석 단계] 단건 상담 텍스트 분석 요청 시 실행
        고객의 입력 텍스트가 구축된 인덱스의 키워드와 "완전 일치(Exact Match)" 하는지 검사
        """
        # 입력된 원문 텍스트에 동일한 정규화 파이프라인 적용
        norm_text = normalize(raw_text)
        
        # 정규화 결과가 빈 문자열인 경우 (특수문자/공백만 존재 시) 매칭을 중단하고 빈 리스트 반환
        if not norm_text:
            return []

        # 1순위: 표준 키워드(Canonical) 인덱스 매칭 검사 (가장 높은 우선순위)
        match = self.canon_norm_index.get(norm_text)
        if match:
            return match

        # 2순위: 별칭(Alias) 인덱스 매칭 검사
        match = self.alias_norm_index.get(norm_text)
        if match:
            return match

        # 완전 일치 항목이 없는 경우 빈 리스트 반환 (이후 파이프라인인 Stage 2 Aho-Corasick 단계로 이관)
        return []


# 테스트 공간
if __name__ == "__main__":
    # 1. 참조 데이터(ref) Mock 업
    mock_dict_data = [
        # Keyword
        {"schema":"dict.keyword.v1","analysis_version":"v1-norm-aho-dl","label_id":"BK-012","business_keyword":"요금조회"},
        {"schema":"dict.keyword.v1","analysis_version":"v1-norm-aho-dl","label_id":"BK-042","business_keyword":"자동이체 신청"},
        # Alias
        {"schema":"dict.alias.v1","label_id":"BK-020","business_keyword":"통화품질","alias_text":"통화 끊김","alias_norm":"통화끊김","match_mode":"EXACT"},
        {"schema":"dict.alias.v1","label_id":"BK-043","business_keyword":"단말기분실 신고","alias_text":"휴대폰분실","alias_norm":"휴대폰분실","match_mode":"EXACT"},
        {"schema":"dict.alias.v1","label_id":"BK-041","business_keyword":"번호이동 문의","alias_text":"MNP","alias_norm":"mnp","match_mode":"EXACT"},
        {"schema":"dict.alias.v1","label_id":"BK-047","business_keyword":"와이파이 설정","alias_text":"공유기 설정","alias_norm":"공유기설정","match_mode":"CONTAINS"},
        # [다중 충돌 테스트용 추가] 똑같은 '요금' 별칭이 서로 다른 정답을 가리킴
        {"schema":"dict.alias.v1","label_id":"BK-012","business_keyword":"요금조회","alias_text":"요금","alias_norm":"요금","match_mode":"EXACT"},
        {"schema":"dict.alias.v1","label_id":"BK-013","business_keyword":"요금납부","alias_text":"요금","alias_norm":"요금","match_mode":"EXACT"}
    ]

    # 2. 매퍼 인스턴스화 및 인덱스 빌드
    mapper = ExactMapper()
    mapper.build_index(mock_dict_data)

    print("--- 메모리에 적재된 인덱스 확인 ---")
    print("표준 키워드(Canon):", dict(mapper.canon_norm_index))
    print("별칭(Alias):", dict(mapper.alias_norm_index))
    print("-" * 50)

    # 3. 입력 데이터(req.v1) Mock 업
    mock_customer_requests = [
        {"schema":"req.v1","request_id":"job-20260301-0001-chunk-0001","analysis_id":123,"case_id":999,"channel":"chat","text":"요금조회가 안되고 자동이체 신청도 하고 싶은데 앱오류가 나요","memberId":2},
        {"schema":"req.v1","request_id":"job-20260301-0001-chunk-0001","analysis_id":124,"case_id":1000,"channel":"call","text":"단말기분실 신고하려고 하는데 발신제한 설정도 같이 가능한가요","memberId":4},
        {"schema":"req.v1","request_id":"job-20260301-0001-chunk-0001","analysis_id":125,"case_id":1001,"channel":"chat","text":"선텍약정 위약금조회 부탁드립니다","memberId":1},
        {"schema":"req.v1","request_id":"test-exact","analysis_id":999,"case_id":9999,"channel":"chat","text":"통화 끊김","memberId":9}, # 성공 케이스 
        # [다중 충돌 테스트용 추가] 중의적인 단어 입력
        {"schema":"req.v1","case_id":8888,"text":"요금"}
    ]

    print("\n--- Stage 1 (Exact Match) 분석 시작 ---\n")
    for req in mock_customer_requests:
        customer_text = req.get("text", "")
        
        # O(1) 매칭 검사
        matched_ids = mapper.exact_match(customer_text)
        
        if matched_ids:
            print(f"[매칭 성공] 원문: '{customer_text}'\n   ➔ 매핑된 Label ID: {matched_ids}\n")
        else:
            print(f"[매칭 실패] 원문: '{customer_text}'\n   ➔ (완전 일치 실패. Stage 2 Aho-Corasick 파이프라인으로 이관 필요)\n")