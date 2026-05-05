import re

GENERIC_BLOCK_EXACT = {
    "프라모델",
    "건프라",
    "건담 프라모델",
    "건담프라모델",
    "모형",
    "프라모델 키트",
    "건담 키트",
}

GENERIC_BLOCK_CONTAINS = [
    "프라모델 입고",
    "프라모델 예약",
    "프라모델 판매",
    "건프라 입고",
    "건프라 예약",
    "건프라 판매",
]

STOPWORDS = [
    "1", "144", "100", "60",
    "scale", "model", "kit",
    "건담베이스", "건담샵", "루리웹",
]


def normalize_name(name: str) -> str:
    text = (name or "").lower().strip()

    text = text.replace("ver.", "ver")
    text = text.replace("version", "ver")

    text = re.sub(r"\b1/\d+\b", "", text)
    text = re.sub(r"\b\d+/\d+\b", "", text)

    text = re.sub(r"[\[\]\(\)\-_/.,:+]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()

    return text


def tokenize_name(name: str) -> list:
    text = normalize_name(name)
    tokens = text.split()

    result = []
    for token in tokens:
        if token in STOPWORDS:
            continue
        if len(token) <= 1:
            continue
        result.append(token)

    return result


def is_too_generic_product_name(name: str) -> bool:
    text = normalize_name(name)

    if not text:
        return True

    if text in GENERIC_BLOCK_EXACT:
        return True

    if any(keyword in text for keyword in GENERIC_BLOCK_CONTAINS):
        return True

    tokens = tokenize_name(name)
    if len(tokens) <= 1:
        return True

    return False

def jaccard_similarity(a: str, b: str) -> float:
    a_tokens = set(tokenize_name(a))
    b_tokens = set(tokenize_name(b))

    if not a_tokens or not b_tokens:
        return 0.0

    inter = len(a_tokens & b_tokens)
    union = len(a_tokens | b_tokens)
    return inter / union if union else 0.0


def is_probably_same_product(a: str, b: str) -> bool:
    na = normalize_name(a)
    nb = normalize_name(b)

    if not na or not nb:
        return False

    # 완전 동일
    if na == nb:
        return True

    score = jaccard_similarity(a, b)

    # 유사도 기준
    if score >= 0.6:
        return True

    # 핵심 토큰 2개 이상 겹치면 같은 상품으로 판단
    a_tokens = set(tokenize_name(a))
    b_tokens = set(tokenize_name(b))

    common = a_tokens & b_tokens
    if len(common) >= 2:
        return True

    return False