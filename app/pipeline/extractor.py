"""
Aho-Corasick 기반 멀티패턴 추출기 (Stage 2)
- 긴 문장 속에서 여러 개의 키워드를 동시에, 부분 일치(Substring Match)로 매우 빠르게 찾아냄
- 단어끼리 겹치는(Overlap) 경우, 기획서의 룰(길이 우선 -> 우선순위 우선)에 따라 1등만 필터링
- 띄어쓰기 증발로 인한 위치 틀어짐을 방지하기 위해 Offset Mapping 기능을 지원
"""

import ahocorasick
import re # CANON 마스터 키워드 정규화를 위해 임시 임포트
from typing import List, Dict, Any

class AhoCorasickExtractor:
    def __init__(self):
        # pyahocorasick 라이브러리의 핵심인 Automaton(거미줄) 객체 생성
        self.automaton = ahocorasick.Automaton()
        # 키워드별 우선순위(priority)를 저장할 딕셔너리
        self.keyword_meta: Dict[str, Dict[str, Any]] = {}
        # 거미줄이 다 짜였는지 확인하는 상태 플래그
        self.is_built = False

    def build_automaton(self, dict_data: List[Dict[str, Any]]):
        """
        [시작 시 1회 실행] DB에서 가져온 사전 데이터로 오토마톤(거미줄)을 엮는다
        """
        # 1. 메타 데이터 세팅 및 마스터 키워드(CANON) 등록
        for item in dict_data:
            if item["schema"] == "dict.keyword.v1":
                label_id = item["label_id"]
                keyword_name = item["business_keyword"]
                
                # 메타 데이터 저장
                self.keyword_meta[label_id] = {
                    "keyword_name": keyword_name
                }
                
                # 마스터 키워드 자체도 정규화해서 오토마톤에 등록
                norm_canon = re.sub(r'[^a-z0-9가-힣]', '', keyword_name.lower())
                
                if norm_canon:
                    payload = {
                        "keyword_id": label_id,
                        "pattern_length": len(norm_canon),
                        "source": "CANON" # 출처가 마스터 키워드임을 명시
                    }
                    self._add_to_automaton(norm_canon, payload)

        # 2. 오토마톤에 별칭(ALIAS) 패턴 등록
        for item in dict_data:
            if item["schema"] == "dict.alias.v1":
                label_id = item["label_id"]
                norm_text = item["alias_norm"] # 이미 DB에 정규화되어 들어있음
                
                if not norm_text:
                    continue 
                
                # 별칭용 페이로드(Payload) 구성
                payload = {
                    "keyword_id": label_id,
                    "pattern_length": len(norm_text),
                    "source": "ALIAS" # 출처가 별칭임을 명시
                }
                self._add_to_automaton(norm_text, payload)

        # 3. 거미줄 완성 (내부적으로 검색 트리를 최적화하여 엮어줌)
        self.automaton.make_automaton()
        self.is_built = True

    def _add_to_automaton(self, text: str, payload: Dict[str, Any]):
        """내부 헬퍼 함수: 다중 충돌(하나의 단어가 여러 ID를 가짐)을 안전하게 처리하며 오토마톤에 추가"""
        if self.automaton.exists(text):
            existing_payloads = self.automaton.get(text)
            existing_payloads.append(payload)
            self.automaton.add_word(text, existing_payloads) # 덮어쓰기
        else:
            self.automaton.add_word(text, [payload]) # 리스트 형태로 첫 등록

    def extract_keywords(self, text: str, offset_map: List[int] = None) -> List[Dict[str, Any]]:
        """
        [고객 요청 시 매번 실행] 문장 속에서 키워드를 찾고 겹침을 제거
        Args:
            text (str): 정규화가 완료된 고객 입력 텍스트 (예: '요금조회가안되고...')
            offset_map (List[int], optional): normalizer가 만든 원본 위치 지도
        """
        if not self.is_built:
            return []

        raw_matches = []
        
        # 1. 1차 스캔: iter()가 텍스트를 훑으며 끝 인덱스와 보따리들을 반환
        for norm_end_index, payloads in self.automaton.iter(text):
            for p in payloads:
                # 정규화 텍스트 기준의 시작 인덱스 = 끝 인덱스 - 글자 길이 + 1
                norm_start_index = norm_end_index - p["pattern_length"] + 1
                
                # [오프셋 매핑] 지도가 있다면, 정규화 위치를 '진짜 원본 위치'로 번역
                if offset_map:
                    orig_start = offset_map[norm_start_index]
                    orig_end = offset_map[norm_end_index]
                else:
                    orig_start = norm_start_index
                    orig_end = norm_end_index
                    
                # 후보 리스트에 추가
                raw_matches.append({
                    "keyword_id": p["keyword_id"],
                    "source": p["source"],
                    "orig_start": orig_start,     # spaCy가 원본에서 텍스트 도려낼 때 쓸 위치
                    "orig_end": orig_end,         # spaCy가 원본에서 텍스트 도려낼 때 쓸 위치
                    "norm_start": norm_start_index, # 정렬 및 겹침 계산용
                    "norm_end": norm_end_index,     # 정렬 및 겹침 계산용
                    "length": p["pattern_length"]
                })

        if not raw_matches:
            return []

        # 2. Tie-break 정렬
        # 1순위: 빨리 나타난 놈 (norm_start 오름차순)
        # 2순위: 길이가 긴 놈 (-length 내림차순)
        raw_matches.sort(key=lambda x: (x["norm_start"], -x["length"]))

        # 3. 겹침(Overlap) 제거 로직
        final_matches = []
        last_norm_end = -1 # 가장 최근에 확정된 단어의 정규화 끝 위치 기록
        last_norm_start = -1 # 공동 우승자 판별을 위해 시작 위치도 같이 기록

        for match in raw_matches:
            # 케이스 A: 현재 후보의 시작 위치가, 이전 확정 단어의 끝 위치보다 뒤에 있으면 -> 겹치지 않는 새 단어
            if match["norm_start"] > last_norm_end:
                final_matches.append(match)
                last_norm_start = match["norm_start"] # 내 시작 위치 갱신
                last_norm_end = match["norm_end"]     # 내 끝 위치 갱신
                
            # 케이스 B: 시작 위치와 끝 위치(길이)가 완전히 똑같은 다중 매핑
            elif match["norm_start"] == last_norm_start and match["norm_end"] == last_norm_end:
                final_matches.append(match) # 스캐너 선에서 탈락시키지 않고 지배인에게 같이 넘김
                
            # 케이스 C: 부분만 겹치거나, 큰 단어 안에 포함된 짧은 단어
            else:
                pass # 이미 1등(가장 긴 단어)이 들어간 상태이므로 무시

        return final_matches


# 테스트 공간
if __name__ == "__main__":
    # 임시 오프셋 매핑 함수 (테스트용)
    def temp_normalize_with_offsets(raw_text: str):
        norm_chars = []
        offsets = []
        for i, char in enumerate(raw_text.lower()):
            if re.match(r'[a-z0-9가-힣]', char):
                norm_chars.append(char)
                offsets.append(i)
        return "".join(norm_chars), offsets

    # 1. 사전 데이터 셋업 (CANON과 ALIAS)
    mock_dict_data = [
        {"schema":"dict.keyword.v1", "label_id":"BK-012", "business_keyword":"요금조회", "priority": 100},
        {"schema":"dict.keyword.v1", "label_id":"BK-099", "business_keyword":"자동이체", "priority": 80},
        
        # 별칭 등록 (요금조회의 별칭으로 '요금' 등록)
        {"schema":"dict.alias.v1", "label_id":"BK-012", "alias_norm":"요금"},
    ]

    extractor = AhoCorasickExtractor()
    extractor.build_automaton(mock_dict_data)

    # 2. JSONL 원문 텍스트
    raw_text = "요금조회가 안되고 자동이체 신청도 하고 싶은데 앱오류가 나요"
    
    # 3. 텍스트 정규화 및 오프셋 지도 생성
    norm_text, offset_map = temp_normalize_with_offsets(raw_text)
    print(f"--- 원문: '{raw_text}' ---")
    print(f"--- 정규화: '{norm_text}' ---")

    # 4. 스캐너 실행
    results = extractor.extract_keywords(norm_text, offset_map)
    
    print("\n최종 추출 결과:")
    for res in results:
        # 원문에서 도려낸 진짜 텍스트 확인 (orig_start 부터 orig_end까지)
        extracted_raw_word = raw_text[res['orig_start'] : res['orig_end'] + 1]
        
        print(f" - ID: {res['keyword_id']} | Source: {res['source']} | "
              f"도려낸 원문: '{extracted_raw_word}' (인덱스: {res['orig_start']}~{res['orig_end']})")

    # 예상 출력 결과:
    # '요금조회' (ID: BK-012, Source: CANON, 인덱스 0~3)
    # '자동이체' (ID: BK-099, Source: CANON, 인덱스 10~13)
    # ('요금' 별칭은 '요금조회' 마스터 키워드와 겹치지만, 길이가 짧아서 Tie-break에서 탈락되어 출력되지 않음)