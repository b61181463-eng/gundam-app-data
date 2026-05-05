import json
import re
from datetime import datetime
from pathlib import Path

import firebase_admin
from firebase_admin import credentials, firestore


COLLECTION_NAME = "aggregated_items"
BASE_DIR = Path(__file__).resolve().parent.parent
SERVICE_ACCOUNT_PATH = BASE_DIR / "serviceAccountKey.json"
BACKUP_DIR = BASE_DIR / "backups"


def normalize_name(raw_name: str) -> str:
    name = (raw_name or "").lower()

    name = re.sub(r"\([^)]*\)", " ", name)
    name = re.sub(r"\[[^\]]*\]", " ", name)

    remove_words = [
        "bandai", "반다이", "반다이스피리츠", "banpresto",
        "건담프라모델", "프라모델", "재입고", "예약", "입고",
        "특가", "세일", "신상품", "당일발송", "국내배송",
        "정품", "한정판", "일본내수",
    ]

    for word in remove_words:
        name = name.replace(word, " ")

    replacements = {
        "master grade": "mg",
        "real grade": "rg",
        "high grade": "hg",
        "perfect grade": "pg",
        "entry grade": "eg",
        "super deformed": "sd",
    }

    for old, new in replacements.items():
        name = name.replace(old, new)

    name = re.sub(r"1\s*/\s*144", " ", name)
    name = re.sub(r"1\s*/\s*100", " ", name)
    name = re.sub(r"1\s*/\s*60", " ", name)

    name = re.sub(r"[^a-z0-9가-힣\s]", " ", name)
    name = re.sub(r"\s+", " ", name).strip()

    return name


def detect_grade(raw_name: str) -> str:
    name = (raw_name or "").lower()

    if "mgex" in name:
        return "MGEX"
    if "mgsd" in name:
        return "MGSD"
    if re.search(r"\bpg\b|perfect grade|퍼펙트", name):
        return "PG"
    if re.search(r"\bmg\b|master grade|마스터", name):
        return "MG"
    if re.search(r"\brg\b|real grade|리얼", name):
        return "RG"
    if re.search(r"\bhg\b|hgce|high grade|하이", name):
        return "HG"
    if re.search(r"\beg\b|entry grade|엔트리", name):
        return "EG"
    if re.search(r"\bsd\b|sdcs|sdw|super deformed", name):
        return "SD"

    return "기타"


def normalize_status(raw_status: str, raw_name: str = "") -> str:
    text = f"{raw_status or ''} {raw_name or ''}".lower()

    if any(k in text for k in [
        "품절", "sold out", "soldout", "일시품절",
        "재고없음", "out of stock",
    ]):
        return "품절"

    if any(k in text for k in [
        "예약", "pre-order", "preorder", "pre order",
        "예약판매", "예약중",
    ]):
        return "예약중"

    if any(k in text for k in [
        "입고예정", "출시예정", "coming soon", "발매예정",
    ]):
        return "입고예정"

    if any(k in text for k in [
        "판매중", "구매가능", "재고있음", "in stock",
        "available", "장바구니", "바로구매",
    ]):
        return "판매중"

    return "상태 확인중"


def normalize_price(raw_price):
    if raw_price is None:
        return None

    text = str(raw_price)
    only_number = re.sub(r"[^0-9]", "", text)

    if not only_number:
        return None

    price = int(only_number)

    if price < 1000:
        return None
    if price > 3_000_000:
        return None

    return price


def make_group_key(raw_name: str) -> str:
    grade = detect_grade(raw_name)
    normalized = normalize_name(raw_name)
    return f"{grade}::{normalized}"


def init_firestore():
    if not SERVICE_ACCOUNT_PATH.exists():
        raise FileNotFoundError(
            f"serviceAccountKey.json 없음: {SERVICE_ACCOUNT_PATH}"
        )

    if not firebase_admin._apps:
        cred = credentials.Certificate(str(SERVICE_ACCOUNT_PATH))
        firebase_admin.initialize_app(cred)

    return firestore.client()


def backup_documents(docs):
    BACKUP_DIR.mkdir(exist_ok=True)

    backup_path = BACKUP_DIR / f"{COLLECTION_NAME}_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

    backup_data = []
    for doc in docs:
        data = doc.to_dict()
        data["_doc_id"] = doc.id
        backup_data.append(data)

    with backup_path.open("w", encoding="utf-8") as f:
        json.dump(backup_data, f, ensure_ascii=False, indent=2, default=str)

    print(f"백업 완료: {backup_path}")


def main():
    db = init_firestore()

    print(f"Firestore 컬렉션 읽는 중: {COLLECTION_NAME}")
    docs = list(db.collection(COLLECTION_NAME).stream())

    print(f"총 문서 수: {len(docs)}")

    if not docs:
        print("문서가 없습니다. 컬렉션 이름을 확인하세요.")
        return

    backup_documents(docs)

    batch = db.batch()
    count = 0

    for doc in docs:
        data = doc.to_dict()

        name = str(data.get("name") or data.get("title") or "")
        raw_price = data.get("price")
        raw_status = str(data.get("status") or "")

        normalized_price = normalize_price(raw_price)
        normalized_status = normalize_status(raw_status, name)
        grade = detect_grade(name)
        group_key = make_group_key(name)

        update_data = {
            "normalizedName": normalize_name(name),
            "normalizedPrice": normalized_price,
            "normalizedStatus": normalized_status,
            "grade": grade,
            "groupKey": group_key,
            "updatedNormalizedAt": firestore.SERVER_TIMESTAMP,
        }

        # 기존 price/status도 정리된 값으로 맞추고 싶으면 유지
        update_data["price"] = normalized_price
        update_data["status"] = normalized_status

        batch.update(doc.reference, update_data)
        count += 1

        if count % 450 == 0:
            batch.commit()
            print(f"{count}개 업데이트 완료")
            batch = db.batch()

    batch.commit()
    print(f"전체 정규화 완료: {count}개")


if __name__ == "__main__":
    main()