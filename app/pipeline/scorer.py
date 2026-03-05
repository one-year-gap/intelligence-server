import spacy
import itertools
from typing import List, Dict, Any
# rapidfuzz: C++ 기반의 초고속 문자열 비교 라이브러리
from rapidfuzz.distance import DamerauLevenshtein

class ContextScorer:
    """
    [Stage 3] 문맥 분석 및 오타 교정 심판관 (spaCy 기반)
    1, 2단계에서 넘어온 결과를 바탕으로 중의성을 해결하고, 
    놓친 오타를 구출(Fallback)하는 최종 분석기
    """

    def __init__(self, model_name: str = "ko_core_news_sm"):
        """
        [서버 시작 시 1회 실행] 
        무거운 AI 모델(spaCy 한국어 모델)을 메모리에 사전 로딩
        """
        try:
            # spacy.load()는 딥러닝 모델을 불러오는 작업이라 몇 초 정도 걸림
            # 그래서 고객이 채팅을 칠 때마다 부르지 않고, 서버가 켜질 때 딱 한 번만 불러서 self.nlp에 저장해 둔다
            self.nlp = spacy.load(model_name)
            print(f"[System] spaCy '{model_name}' 모델 로딩 완료!")
        except OSError:
            # 만약 개발 환경에 한국어 모델이 안 깔려있으면 에러 
            raise OSError(
                f"{model_name} 모델이 설치되지 않았습니다.\n"
                f"터미널에서 아래 명령어를 실행해주세요:\n"
                f"python -m spacy download {model_name}"
            )

    def parse_document(self, raw_text: str) -> spacy.tokens.Doc:
        """
        [단일 doc 객체 생성 로직]
        고객의 문장을 spaCy AI에게 읽혀서 '문법 지도(Doc 객체)'를 만든다 
        이 작업은 CPU를 많이 쓰기 때문에, 문장당 무조건 딱 1번만 실행
        """
        # self.nlp(텍스트)를 실행하면, 문장의 형태소(명사, 동사 등), 띄어쓰기, 문장 구조가 
        # 싹 다 분석된 거대한 '문법 지도(Doc)'가 만들어짐
        doc = self.nlp(raw_text)
        
        # 이제 이 doc 객체 하나만 있으면, 뒤에서 쌍둥이 심사(문맥 파악)를 하든
        # 오타 패자부활전(명사 추출)을 하든 AI를 다시 돌릴 필요 없이 이 지도를 돌려쓰면 됨
        return doc
    

    def resolve_ambiguity(self, doc: spacy.tokens.Doc, target_start: int, target_end: int, candidates: List[str], keyword_meta: Dict[str, str]) -> str:
        """
        [동적 심사위원 로직] 
        다중 매핑(쌍둥이) 발생 시, 후보들의 '마스터 키워드' 글자와 주변 문맥이 얼마나 겹치는지 채점
        (keyword_meta에는 {"BK-012": "요금조회", "BK-013": "요금납부"} 같은 사전 정보가 들어온다)
        """
        # 1. 문제의 단어("요금")를 제외한 주변 문맥 텍스트를 하나로 뭉침
        context_words = [token.text for token in doc if token.idx < target_start or token.idx > target_end]
        context_text = "".join(context_words)

        best_candidate = candidates[0]
        max_score = -1

        # 2. 각 후보를 돌면서 점수(Score)를 매김
        for cand_id in candidates:
            canon_name = keyword_meta.get(cand_id, "") # 예: "요금조회"
            
            # 채점 방식: 마스터 키워드의 글자가 문맥에 얼마나 들어있는지 1글자당 1점
            # (예: "조회"라는 글자가 문맥에 있으면 점수 올라감)
            score = sum(1 for char in canon_name if char in context_text)
            
            if score > max_score:
                max_score = score
                best_candidate = cand_id
                
        return best_candidate

    def rescue_typos(self, doc: spacy.tokens.Doc, masked_text: str, canon_index: Dict[str, List[str]], alias_index: Dict[str, List[str]]) -> List[Dict[str, Any]]:
        """
        [패자부활전 로직] 
        지배인이 1,2단계에서 찾은 단어를 '*'로 마스킹(Masking)한 문장(masked_text)을 받는다.
        남은 찌꺼기 텍스트에서 명사를 뭉쳐서 O(1) 검사 후, 실패 시 다메라우 오타 검사를 진행.
        """
        rescued_results = []
        nouns_to_check = [] # 검사할 명사 덩어리 보관함
        current_noun = ""
        start_idx = -1

        # --- STEP 1: 미탐 영역 형태소 분석 (명사 찰흙놀이) ---
        for token in doc:
            # token.idx 위치가 마스킹('*') 되어있다면, 이미 1,2단계에서 찾은 단어이므로 무시
            if masked_text[token.idx] == "*":
                if current_noun:
                    nouns_to_check.append((current_noun, start_idx, token.idx - 1))
                    current_noun = ""
                continue

            # 명사(NOUN)이면 계속 이어 붙입니다. (예: '선텍' + '약정')
            if token.pos_ in ["NOUN", "PROPN"]:
                if not current_noun:
                    start_idx = token.idx
                current_noun += token.text
            else:
                # 명사가 끝났으면 (예: 조사가 나오면) 지금까지 뭉친 명사를 검사 후보 리스트에 넣음
                if current_noun:
                    # 끝 인덱스 = 시작 인덱스 + 뭉친 단어 길이 - 1
                    nouns_to_check.append((current_noun, start_idx, start_idx + len(current_noun) - 1))
                    current_noun = ""
                    
        # 문장 끝까지 명사로 끝났을 경우를 대비해 마지막 털기
        if current_noun:
            nouns_to_check.append((current_noun, start_idx, start_idx + len(current_noun) - 1))

        # --- STEP 2: O(1) 매칭 및 다메라우 연산 ---
        for noun_text, s, e in nouns_to_check:
            # 글자가 2글자 미만(예: '나', '저')이면 오타 검사에서 제외 (무의미한 연산 방지)
            if len(noun_text) < 2:
                continue

            # [핵심 로직 1] O(1) 완전 매칭 검사
            # Mapper가 만든 사전을 보고, "이 덩어리가 혹시 오타가 아니라 진짜 단어 아니야?" 확인
            matched_ids = canon_index.get(noun_text) or alias_index.get(noun_text)
                
            if matched_ids:
                # 사전에 완벽하게 있으면 오타 검사 패스
                rescued_results.append({
                    "keyword_id": matched_ids[0],
                    "source": "FALLBACK_EXACT", # 정확히 일치해서 구출됨
                    "orig_start": s,
                    "orig_end": e,
                    "rescued_word": noun_text
                })
                continue # 다음 명사 덩어리로 넘어감

            # [핵심 로직 2] 다메라우 연산 (rapidfuzz 활용 오타 검사)
            # O(1)에서 실패했으니, 이제 진짜 오타인지 단어장을 돌면서 검사
            # itertools.chain을 사용해 표준 키워드(canon)와 별칭(alias) 단어장을 이어서 순회
            for dict_word, label_ids in itertools.chain(canon_index.items(), alias_index.items()):
                # DamerauLevenshtein: 글자 바뀜, 순서 바뀜 등을 C++ 엔진으로 초고속 계산
                # 거리 1 = 1글자만 틀린 오타
                if DamerauLevenshtein.distance(noun_text, dict_word) == 1:
                    rescued_results.append({
                        "keyword_id": label_ids[0],
                        "source": "FALLBACK_TYPO", # 오타를 교정해서 구출됨
                        "orig_start": s,
                        "orig_end": e,
                        "rescued_word": noun_text
                    })
                    break # 오타 찾았으면 더 안 찾아도 됨

        return rescued_results


# 테스트 공간
if __name__ == "__main__":
    # 1. 심판관 인스턴스 생성 (이때 모델 로딩 시간이 약간 걸림)
    scorer = ContextScorer()

    # 2. 고객의 입력 문장
    customer_text = "이번 달 요금 얼마인지 조회 좀 해주세요"
    print(f"\n--- 텍스트 분석 시작: '{customer_text}' ---")

    # 3. 단일 doc 객체(문법 지도) 생성 (딱 1번만 실행)
    analyzed_doc = scorer.parse_document(customer_text)

    # 4. 만들어진 지도(doc) 안에 뭐가 들었는지 구경하기
    print("\n[문법 지도(Doc)에 기록된 형태소 분석 결과]")
    for token in analyzed_doc:
        # token.text는 단어, token.pos_는 품사(명사, 동사 등)를 의미
        print(f" - 단어: {token.text: <6} | 시작위치(idx): {token.idx: <2} | 품사: {token.pos_}")

    # 5. 동적 심사위원 테스트용 가짜 데이터(Mock) 셋업
    # 문장 속 '요금'(인덱스 5~6)이 쌍둥이 후보(조회, 납부)를 가졌다고 가정
    target_start = 5  
    target_end = 6    
    candidates = ["BK-012", "BK-013"]
    keyword_meta = {
        "BK-012": "요금조회",
        "BK-013": "요금납부"
    }

    # 6. 심사위원 호출
    winner_id = scorer.resolve_ambiguity(analyzed_doc, target_start, target_end, candidates, keyword_meta)
    
    print(f"\n[다중 매핑 심사 결과]")
    print(f" - 문제 단어: '요금'")
    print(f" - 심사 후보: {candidates}")
    print(f" - 제공된 마스터 키워드 정보: {keyword_meta}")
    print(f" ➔ 문맥 채점 기반 최종 1등: {winner_id} ({keyword_meta[winner_id]})")

    # ---------------------------------------------------------
    # 패자부활전(Fallback) 로직 검증
    # O(1) 완전 매칭과 rapidfuzz 오타 교정이 잘 작동하는지 확인
    # ---------------------------------------------------------
    print("\n" + "="*50)
    print("--- 패자부활전(Fallback) 테스트 시작 ---")
    
    # 7. 패자부활전용 가상의 고객 입력
    # 상황: 1, 2단계가 오류로 인해 단어를 하나도 못 찾았다고 가정
    fallback_text = "선텍약정 언제 끝나고 스마트폰 어떻게 해요?"
    fallback_masked = "선텍약정 언제 끝나고 스마트폰 어떻게 해요?"
    
    print(f" - 원본 문장: '{fallback_text}'")
    
    # 8. 패자부활전용 문법 지도 생성
    fallback_doc = scorer.parse_document(fallback_text)

    # 9. Mapper가 서버 시작 시 만들어둔 단어장(Hash Map) 흉내내기
    # - 선택약정: 오타 테스트용 (선텍약정 -> 1글자 오타)
    # - 스마트폰: O(1) 매칭 테스트용 (토시 하나 안 틀리고 똑같음)
    mock_canon_index = {
        "선택약정": ["BK-025"], 
        "스마트폰": ["BK-047"]
    }
    mock_alias_index = {} # 별칭 단어장은 비어있다고 가정

    # 10. 패자부활전(rescue_typos) 실행
    rescued_results = scorer.rescue_typos(fallback_doc, fallback_masked, mock_canon_index, mock_alias_index)

    print("\n[패자부활전 결과]")
    if not rescued_results:
        print(" ➔ 구출된 단어가 없습니다.")
    else:
        for res in rescued_results:
            print(f"구출 성공! 추출된 단어: '{res['rescued_word']}'")
            print(f"   ➔ 매핑된 라벨 ID: {res['keyword_id']}")
            print(f"   ➔ 구출 사유(Source): {res['source']}")
            
            # 구출 사유에 대한 설명 출력
            if res['source'] == "FALLBACK_EXACT":
                print("   ➔ (설명: 단어장에 완벽하게 똑같은 단어가 있어서 O(1) 매칭으로 구출됨)")
            elif res['source'] == "FALLBACK_TYPO":
                print("   ➔ (설명: rapidfuzz 검사 결과 1글자 오타로 판명되어 교정 후 구출됨)")
            print("-" * 30)