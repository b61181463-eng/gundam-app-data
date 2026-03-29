import json
import re
from pathlib import Path
from collections import defaultdict

import firebase_admin
from firebase_admin import credentials, firestore

# =====================
# 설정
# =====================

INPUT_FILES = [
    "data/gundamshop_items.json",
    "data/gundambase_items.json",
    "data/bnkrmall_items.json",
    "data/hobbyfactory_items.json",
    "data/gundamcity_items.json",
    "data/modelsale_items.json",
]

SERVICE_ACCOUNT_PATH = "serviceAccountKey.json"
COLLECTION_NAME = "aggregated_items"

# =====================
# Firebase 초기화
# =====================

def init_firestore():
    if not firebase_admin._apps:
        cred = credentials.Certificate(SERVICE_ACCOUNT_PATH)
        firebase_admin.initialize_app(cred)
    return firestore.client()

# =====================
# 유틸
# =====================

def clean_text(text):
    return re.sub(r"\s+", " ", (text or "")).strip()

def clean_name(name):
    name = clean_text(name)

    # 앞 괄호 제거
    name = re.sub(r'^\[[^\]]+\]\s*', '', name)
    name = re.sub(r'^\([^)]+\)\s*', '', name)

    return name

def is_valid_gundam_item(item):
    text = (
        f"{item.get('name','')} "
        f"{item.get('title','')} "
        f"{item.get('stockText','')}"
    ).upper()

    # ❌ 제거 대상
    exclude = [
        "공지", "NOTICE", "EVENT", "이벤트",
        "쿠폰", "사은품", "증정",
        "배송", "안내", "문의", "교환",
        "반품", "결제", "공지사항",
    ]

    for e in exclude:
        if e in text:
            return False

    # ✅ 건담 등급 필수
    include = ["PG", "MG", "RG", "HG", "SD", "MGEX", "MGSD"]

    return any(i in text for i in include)

def extract_price(text):
    text = text or ""
    m = re.search(r"[\d,]+원", text)
    return m.group(0) if m else ""

def normalize_status(text):
    t = (text or "").lower()

    if any(x in t for x in ["품절", "sold out", "out of stock"]):
        return "품절"
    if any(x in t for x in ["예약", "preorder"]):
        return "예약중"
    if any(x in t for x in ["판매", "구매", "장바구니", "in stock"]):
        return "판매중"

    return "상태 확인중"

def normalize_key(name):
    name = name.upper()
    name = re.sub(r"[^A-Z0-9가-힣]", "", name)
    return name

# =====================
# 데이터 로드
# =====================

def load_all_items():
    items = []

    for file in INPUT_FILES:
        path = Path(file)
        if not path.exists():
            continue

        data = json.loads(path.read_text(encoding="utf-8"))
        print(f"{file}: {len(data)}개")

        items.extend(data)

    return items

# =====================
# 메인 로직
# =====================

def process_items(items):
    print(f"총 수집: {len(items)}개")

    # 1. 필터링
    items = [item for item in items if is_valid_gundam_item(item)]
    print(f"건담 필터 후: {len(items)}개")

    # 2. 정리
    for item in items:
        item["name"] = clean_name(item.get("name", ""))
        item["price"] = item.get("price") or extract_price(item.get("stockText", ""))
        item["status"] = normalize_status(item.get("stockText", ""))

    # 3. 중복 그룹화
    grouped = defaultdict(list)

    for item in items:
        key = normalize_key(item["name"])
        grouped[key].append(item)

    # 4. 병합
    merged_items = []

    for key, group in grouped.items():
        # 가격 정렬
        def price_val(x):
            p = x.get("price", "")
            p = re.sub(r"[^\d]", "", p)
            return int(p) if p else 99999999

        group.sort(key=price_val)

        best = group[0]

        merged_items.append({
            "itemId": key,
            "name": best["name"],
            "title": best["name"],
            "price": best["price"],
            "minPrice": best["price"],
            "sellerCount": len(group),
            "site": best.get("site", ""),
            "mallName": best.get("mallName", ""),
            "status": best.get("status", ""),
            "stockText": best.get("stockText", ""),
            "url": best.get("url", ""),
            "productUrl": best.get("productUrl", ""),
            "resolvedUrl": best.get("productUrl", ""),
            "offers": group,
        })

    print(f"최종 병합: {len(merged_items)}개")

    return merged_items

# =====================
# Firestore 업로드
# =====================

def upload(db, items):
    batch = db.batch()

    for item in items:
        doc_ref = db.collection(COLLECTION_NAME).document(item["itemId"])
        batch.set(doc_ref, item)

    batch.commit()
    print("Firestore 업로드 완료")

# =====================
# 실행
# =====================

def main():
    db = init_firestore()

    items = load_all_items()
    processed = process_items(items)

    upload(db, processed)

if __name__ == "__main__":
    main()
