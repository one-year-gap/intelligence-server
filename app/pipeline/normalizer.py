import re

def normalize(raw_text: str) -> str:
    """
    원문 텍스트를 분석용(alias_norm) 정규화 텍스트로 변환
    (Spring Boot의 KeywordNormalizer.java 와 100% 동일한 규칙 적용)
    
    Args:
        raw_text (str): 고객이 입력한 원문 상담 텍스트 또는 사전의 원문 별칭
        
    Returns:
        str: 특수문자와 공백이 제거되고 소문자로 통일된 문자열
    """
    
    # 1. Null 체크 및 빈 문자열(공백만 있는 경우 포함) 체크
    # 파이썬에서는 not raw_text 로 None이나 "" 처리가 가능하고, 
    # strip()을 써서 공백만 있는 경우(Java의 isBlank)를 걸러냄
    if not raw_text or not raw_text.strip():
        return ""

    # 2. 소문자 통일 (예: U+tv -> u+tv, VIP -> vip)
    # 파이썬의 lower() 함수는 자바의 toLowerCase()와 완벽히 똑같이 동작
    normalized = raw_text.lower()

    # 3. 특수문자 및 공백 제거 (한글, 영문 소문자, 숫자만 남김)
    # 정규식 패턴 [^a-z0-9가-힣] 의 의미:
    #   ^ : '아닌 것' (Not)
    #   a-z : 영문 소문자
    #   0-9 : 숫자
    #   가-힣 : 완성형 한글
    # 즉, 위 조건에 해당하지 않는 모든 기호(!, ?, 공백 등)를 찾아 ''(빈 문자열)로 교체
    normalized = re.sub(r'[^a-z0-9가-힣]', '', normalized)

    return normalized


# 간단 테스트 공간
if __name__ == "__main__":
    
    test_cases = [
        "U+tv 스마트 홈",      # 대소문자 섞임 + 특수기호 + 공백
        "  VIP 콕 혜택!!!  ",   # 양옆 공백 + 느낌표
        "요금조회 안됨ㅠㅠ",    # 한글 자음/모음(ㅠㅠ)은 정규식에 없으므로 날아감
        "",                    # 빈 문자열
        None                   # Null 값
    ]
    
    for text in test_cases:
        result = normalize(text)
        print(f"원문: '{text}'  ➔  정규화: '{result}'")