import re
import firebase_admin
from firebase_admin import credentials, firestore

SERVICE_ACCOUNT_PATH = "serviceAccountKey.json"

BAD_EXACT = {
    "프라모델",
    "건프라",
    "모형",
    "건담 프라모델",
    "건담프라모델",
    "프라모델 키트",
    "건담 키트",
    "mgsd",
    "hg",
    "mg",
    "rg",
    "pg",
    "sd",
    "eg",
    "bb",
    "re/100",
    "30ms",
    "30mm",
}

BAD_CONTAINS = [
    "www", "http", "https", ".com", ".kr",".jpg"
    "유의사항", "공지 확인", "판매 방식", "출처", "댓글", "링크", "안내", "이벤트",
    "확인 부탁", "확인 바랍니다", "건담베이스는", "올라오고 있습니다",
    "프라모델",
    "clamp", "sega", "tsuburaya", "trigger,akira", "copyright",
    "�", "Ã", "Â", "°ç", "´ã", "¿", "½", "À", "Ã¬", "Ã¥","©"
]

SENTENCE_ENDINGS = [
    "합니다", "합니다.",
    "있습니다", "있습니다.",
    "드립니다", "드립니다.",
    "바랍니다", "바랍니다.",
    "입니다", "입니다.",
    "됩니다", "됩니다.",
]


def init_firestore():
    if not firebase_admin._apps:
        cred = credentials.Certificate(SERVICE_ACCOUNT_PATH)
        firebase_admin.initialize_app(cred)
    return firestore.client()


def safe_str(value, fallback=""):
    if value is None:
        return fallback
    text = str(value).strip()
    return text if text else fallback


def should_delete_name(name: str) -> bool:
    if not name:
        return True

    lower = name.lower().strip()

    if lower in BAD_EXACT:
        return True

    if any(bad.lower() in lower for bad in BAD_CONTAINS):
        return True

    if "©" in name:
        return True

    if re.search(r"\b20\d{2}\s*-\s*20\d{2}\b", name):
        return True

    if any(lower.endswith(end) for end in SENTENCE_ENDINGS):
        return True

    if name.count(" ") >= 8:
        return True

    return False


def clean_collection(db, collection_name: str):
    deleted = 0
    docs = list(db.collection(collection_name).stream())

    for doc in docs:
        data = doc.to_dict() or {}
        name = safe_str(data.get("name") or data.get("title"))

        if should_delete_name(name):
            print(f"[삭제 {collection_name}] {name}")
            doc.reference.delete()
            deleted += 1

    return deleted


def main():
    db = init_firestore()

    deleted_items = clean_collection(db, "aggregated_items")
    deleted_merged = clean_collection(db, "aggregated_items_merged")
    deleted_watchlist = clean_collection(db, "watchlist")

    print("\n✅ 패턴 기반 정리 완료")
    print(f"aggregated_items 삭제 수: {deleted_items}")
    print(f"aggregated_items_merged 삭제 수: {deleted_merged}")
    print(f"watchlist 삭제 수: {deleted_watchlist}")


if __name__ == "__main__":
    main()