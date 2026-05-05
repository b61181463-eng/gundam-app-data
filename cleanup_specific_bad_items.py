import firebase_admin
from firebase_admin import credentials, firestore

SERVICE_ACCOUNT_PATH = "serviceAccountKey.json"

TARGETS = [
    "2006-2018 CLAMP",
    "TSUBURAYA",
    "TRIGGER,AKIRA",
    "역시 pg뉴건담을",
    "10년안에 못 구할거란",
    "예상을 점점 더 확신이",
    "나의 히어로 아카데미아",
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


def should_delete(name: str) -> bool:
    if not name:
        return False

    lower = name.lower()
    for target in TARGETS:
        if target.lower() in lower:
            return True

    if "©" in name:
        return True

    return False


def clean_collection(db, collection_name: str):
    deleted = 0
    docs = list(db.collection(collection_name).stream())

    for doc in docs:
        data = doc.to_dict() or {}
        name = safe_str(data.get("name") or data.get("title"))

        if should_delete(name):
            print(f"[삭제 {collection_name}] {name}")
            doc.reference.delete()
            deleted += 1

    return deleted


def main():
    db = init_firestore()

    deleted_items = clean_collection(db, "aggregated_items")
    deleted_merged = clean_collection(db, "aggregated_items_merged")
    deleted_watchlist = clean_collection(db, "watchlist")

    print("\n✅ 특정 문제 문서 정리 완료")
    print(f"aggregated_items 삭제 수: {deleted_items}")
    print(f"aggregated_items_merged 삭제 수: {deleted_merged}")
    print(f"watchlist 삭제 수: {deleted_watchlist}")


if __name__ == "__main__":
    main()