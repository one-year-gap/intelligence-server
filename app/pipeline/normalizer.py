import re
from typing import Tuple, List

def normalize(raw_text: str) -> str:
    # 인덱스 구축용 정규화
    if not raw_text or not raw_text.strip():
        return ""
    normalized = raw_text.lower()
    return re.sub(r'[^a-z0-9가-힣]', '', normalized)

def normalize_with_offsets(raw_text: str) -> Tuple[str, List[int]]:
    """
    고객 텍스트 분석용 정규화 + 원본 위치 추적 지도(Offset Map) 생성
    
    Returns:
        Tuple[정규화된 문자열, 원본 인덱스 리스트]
    """
    if not raw_text or not raw_text.strip():
        return "", []

    normalized_chars = []
    offset_map = []  # 정규화된 글자가 원본의 몇 번째 인덱스인지 기록하는 지도
    
    lower_text = raw_text.lower()
    
    # 원문 글자를 하나씩 돌면서 검사
    for i, char in enumerate(lower_text):
        # 정규식 패턴에 맞는(허용된) 글자일 때만
        if re.match(r'[a-z0-9가-힣]', char):
            normalized_chars.append(char) # 정규화 결과에 추가
            offset_map.append(i)          # 이 글자의 원본 인덱스를 지도에 기록
            
    normalized_text = "".join(normalized_chars)
    return normalized_text, offset_map

# 테스트
if __name__ == "__main__":
    raw = "u+ TV 안나와요"
    norm, offsets = normalize_with_offsets(raw)
    print(f"원문: '{raw}'")
    print(f"정규화: '{norm}'")
    print(f"위치 지도: {offsets}")
    # 출력 예측: [0, 3, 4, 6, 7, 8, 9] 
    # (u=0, t=3, v=4, 안=6 ... -> 중간의 '+'와 '공백' 인덱스가 건너뛰어짐)