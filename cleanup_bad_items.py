import re

import firebase_admin
from firebase_admin import credentials, firestore

SERVICE_ACCOUNT_PATH = "serviceAccountKey.json"

SOURCE_COLLECTIONS = [
    "aggregated_items",
    "aggregated_items_merged",
]

BAD_EXACT_NAMES = {
    "26.95 regular",
    "16.95 sale price now",
    "sale price now",
    "sale price",
    "regular",
    "건담샵에서 예약중인 예약상품 - 건담샵",
    "건담샵에서 예약중인 예약상품",
    "[hg]1/144 ¼¼¹óºñ gnhw/b",
}

BAD_SUBSTRINGS = [
    "sale price",
    "price now",
    "regular",
    "zagtoon",
    "method",
    "samg",
    "toei animation",
    "level-5",
    "hisago amazake-no",
    "예약중인",
    "예약상품",
    "건담샵에서 예약중인",
    "ⓒ",
    "©",
    "¼",
    "¹",
    "º",
    "³",
    "²",
    "¾",
]

ALLOWED_HINTS = [
    "건담", "자쿠", "유니콘", "프리덤", "스트라이크", "에어리얼",
    "즈고크", "사자비", "뉴건담", "시난주", "엑시아",
    "바르바토스", "캘리번", "루브리스", "데스티니",
    "저스티스", "아스트레이", "더블오", "톨기스",
    "윙건담", "짐", "건캐논", "건탱크",
    "mgsd", "mgex", "pg", "mg", "rg", "hg", "sd", "bb", "eg",
    "bb전사", "삼국창걸전", "30ms", "30mm",
]


def init_firestore():
    if not firebase_admin._apps:
        cred = credentials.Certificate(SERVICE_ACCOUNT_PATH)
        firebase_admin.initialize_app(cred)
    return firestore.client()


def normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()


def should_delete(data: dict) -> bool:
    name = normalize_space(data.get("name") or data.get("title")).lower()
    source = normalize_space(data.get("source")).lower()
    site = normalize_space(data.get("site")).lower()
    mall = normalize_space(data.get("mallName")).lower()
    url = normalize_space(data.get("url") or data.get("productUrl") or data.get("link")).lower()

    if "gundamshop.co.kr/theme/reserve.html" in url:
        return True

    if not name:
        return True

    if name in BAD_EXACT_NAMES:
        return True

    if "ⓒ" in name or "©" in name:
        return True

    if any(bad in name for bad in BAD_SUBSTRINGS):
        return True
    
    if re.search(r"[¼¹º³²¾ÐÑÕÖ]", name):
        return True

    if re.fullmatch(r"[\d.,]+\s*(regular|sale price now|sale price|price now)", name):
        return True

    if re.search(r"[\d.,]+\s*(regular|sale price|price now)", name):
        return True

    if re.fullmatch(r"[a-z0-9\s.,/\-]+", name):
        if not any(ok in name for ok in ["gundam", "hg", "mg", "rg", "pg", "sd", "mgsd", "mgex"]):
            return True

    if len(name) < 4:
        return True

    # 건담 관련 키워드가 전혀 없는 짧은 이상 텍스트 제거
    if not any(hint.lower() in name for hint in ALLOWED_HINTS):
        if source in {"gundamshop", "bnkrmall"} or "건담샵" in site or "반다이" in site or "건담샵" in mall or "반다이" in mall:
            return True

    return False


def main():
    db = init_firestore()

    for collection_name in SOURCE_COLLECTIONS:
        docs = list(db.collection(collection_name).stream())
        print(f"[{collection_name}] 전체 문서 수: {len(docs)}")

        batch = db.batch()
        count = 0
        deleted = 0

        for doc in docs:
            data = doc.to_dict() or {}

            if should_delete(data):
                print(f"삭제: {collection_name} / {data.get('name', '')}")
                batch.delete(doc.reference)
                count += 1
                deleted += 1

                if count >= 400:
                    batch.commit()
                    batch = db.batch()
                    count = 0

        if count > 0:
            batch.commit()

        print(f"[{collection_name}] 삭제 완료: {deleted}개")


if __name__ == "__main__":
    main()