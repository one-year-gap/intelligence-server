import spacy
from typing import List, Dict, Any

class ContextScorer:
    """
    [Stage 3] 문맥 분석 및 오타 교정 심판관 (spaCy 기반)
    1, 2단계에서 넘어온 결과를 바탕으로 중의성을 해결(Disambiguation)하고, 
    놓친 오타를 구출(Fallback)하는 최종 분석기입니다.
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
                f"🚨 {model_name} 모델이 설치되지 않았습니다.\n"
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
    

    def resolve_ambiguity(self, doc: spacy.tokens.Doc, target_start: int, target_end: int, candidates: List[str]) -> str:
        """
        [심사위원 로직] 
        다중 매핑(쌍둥이) 발생 시, doc 지도를 보고 주변 문맥을 파악해 진짜 1등을 고른다
        """
        pass

    def rescue_typos(self, doc: spacy.tokens.Doc, masked_text: str) -> List[Dict[str, Any]]:
        """
        [패자부활전 로직] 
        1, 2단계가 찾은 걸 다 지워버린(Masking) 문장에서, 남은 명사들만 뽑아 기본 매핑, 오타를 검사
        """
        pass


# 테스트 공간
if __name__ == "__main__":
    # 1. 심판관 인스턴스 생성 (이때 모델 로딩 시간이 약간 걸림)
    scorer = ContextScorer()

    # 2. 고객의 입력 문장
    customer_text = "인터넷 요금조회 해주세요"
    print(f"\n--- 텍스트 분석 시작: '{customer_text}' ---")

    # 3. 단일 doc 객체(문법 지도) 생성 (딱 1번만 실행)
    analyzed_doc = scorer.parse_document(customer_text)

    # 4. 만들어진 지도(doc) 안에 뭐가 들었는지 구경하기
    print("\n[문법 지도(Doc)에 기록된 형태소 분석 결과]")
    for token in analyzed_doc:
        # token.text는 단어, token.pos_는 품사(명사, 동사 등)를 의미
        print(f" - 단어: {token.text: <6} | 품사: {token.pos_}")