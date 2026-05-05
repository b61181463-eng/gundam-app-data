import firebase_admin
from firebase_admin import credentials, firestore

SERVICE_ACCOUNT_PATH = "serviceAccountKey.json"

BAD_CONTAINS = [
    "�", "Ã", "Â", "°ç", "´ã", "¿", "½", "À", "Ã¬", "Ã¥",
    "www", "http", "https", ".com", ".kr",".jpg"
    "건담베이스는", "올라오고 있습니다",
    "확인 부탁", "확인 바랍니다",
    "매장별로 상이", "점포별로 상이",
    "유의사항", "공지 확인", "판매 방식", "출처", "댓글", "링크", "안내", "이벤트",
    "프라모델","CLAMP",
    "SEGA",
    "TSUBURAYA",
    "TRIGGER,AKIRA",
    "역시 pg뉴건담을",
    "10년안에 못 구할거란",
    "예상을 점점 더 확신이",
    "©",
]

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

    stripped = name.strip()
    lower = stripped.lower()

    if lower in BAD_EXACT:
        return True

    if any(bad.lower() in lower for bad in BAD_CONTAINS):
        return True

    return False


def main():
    db = init_firestore()

    deleted_items = 0
    deleted_merged = 0

    docs = list(db.collection("aggregated_items").stream())
    for doc in docs:
        data = doc.to_dict() or {}
        name = safe_str(data.get("name") or data.get("title"))

        if should_delete_name(name):
            doc.reference.delete()
            deleted_items += 1
            print(f"[삭제 aggregated_items] {name}")

    merged_docs = list(db.collection("aggregated_items_merged").stream())
    for doc in merged_docs:
        data = doc.to_dict() or {}
        name = safe_str(data.get("name") or data.get("title"))

        if should_delete_name(name):
            doc.reference.delete()
            deleted_merged += 1
            print(f"[삭제 aggregated_items_merged] {name}")

    print("\n✅ 정리 완료")
    print(f"aggregated_items 삭제 수: {deleted_items}")
    print(f"aggregated_items_merged 삭제 수: {deleted_merged}")


if __name__ == "__main__":
    main()