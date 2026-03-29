# -*- coding: utf-8 -*-

import os
import re
import json
import hashlib
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse, urlunparse

import firebase_admin
from firebase_admin import credentials, firestore

SERVICE_ACCOUNT_PATH = os.getenv("SERVICE_ACCOUNT_PATH", "serviceAccountKey.json")

def init_firestore():
    if not firebase_admin._apps:
        cred = credentials.Certificate(SERVICE_ACCOUNT_PATH)
        firebase_admin.initialize_app(cred)
    return firestore.client()

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def normalize_whitespace(text):
    if text is None:
        return ""
    text = str(text)
    text = text.replace("\u200b", " ").replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text

def strip_html_entities(text):
    text = normalize_whitespace(text)
    text = text.replace("&amp;", "&")
    text = text.replace("&quot;", '"')
    text = text.replace("&#39;", "'")
    return text

def normalize_title(title):
    title = strip_html_entities(title)
    title = re.sub(r"\[[^\]]+\]", " ", title)
    title = re.sub(r"\s+", " ", title).strip()
    return title

def parse_price(price_text):
    if price_text is None:
        return None
    s = normalize_whitespace(str(price_text)).lower()
    nums = re.findall(r"\d[\d,]*", s)
    if not nums:
        return None
    try:
        return int(nums[0].replace(",", ""))
    except:
        return None

def canonicalize_url(url):
    url = normalize_whitespace(url)
    if not url:
        return ""
    try:
        parsed = urlparse(url)
        scheme = parsed.scheme or "https"
        netloc = parsed.netloc
        path = parsed.path or "/"
        query = parsed.query
        clean = urlunparse((scheme, netloc, path, "", query, ""))
        return clean
    except:
        return url

def make_absolute_url(base_url, maybe_relative):
    if not maybe_relative:
        return ""
    return canonicalize_url(urljoin(base_url, maybe_relative))

def has_any_pattern(text, patterns):
    t = normalize_whitespace(text).lower()
    return any(re.search(p, t, re.I) for p in patterns)

BAD_TITLE_PATTERNS = [
    r"공지",
    r"이벤트",
    r"사은품",
    r"쿠폰",
    r"적립금",
    r"배송",
    r"교환",
    r"환불",
    r"안내",
    r"필독",
    r"당첨",
    r"발표",
    r"예약\s*안내",
    r"결제\s*안내",
    r"입고\s*안내",
    r"재입고\s*안내",
    r"배송비",
    r"운영시간",
    r"휴무",
    r"점검",
    r"문의",
]

GOOD_PRODUCT_PATTERNS = [
    r"\bHG\b",
    r"\bRG\b",
    r"\bMG\b",
    r"\bPG\b",
    r"\bSD\b",
    r"\bMGSD\b",
    r"\bRE/100\b",
    r"\bFM\b",
    r"\bENTRY GRADE\b",
    r"건담",
    r"건프라",
    r"자쿠",
    r"스트라이크",
    r"프리덤",
    r"유니콘",
    r"사자비",
    r"뉴건담",
    r"에어리얼",
    r"발바토스",
    r"윙건담",
    r"헤비암즈",
    r"데스사이즈",
    r"샌드록",
    r"엑시아",
    r"듀나메스",
    r"큐리오스",
    r"버체",
    r"짐",
    r"제타",
    r"더블오",
    r"프라모델",
    r"gunpla",
    r"gundam",
]

IN_STOCK_PATTERNS = [
    r"판매중",
    r"구매가능",
    r"재고있음",
    r"재고 있음",
    r"in stock",
    r"available",
    r"즉시출고",
    r"당일출고",
]

OUT_OF_STOCK_PATTERNS = [
    r"품절",
    r"일시품절",
    r"sold out",
    r"out of stock",
    r"재고없음",
    r"재고 없음",
    r"판매종료",
]

PREORDER_PATTERNS = [
    r"예약",
    r"예약중",
    r"pre[\s\-]?order",
    r"입고예정",
    r"coming soon",
]

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "assets", "data")

# 네 파일명에 맞게 바꿔도 됨
INPUT_FILE = os.path.join(DATA_DIR, "stores_kr_all_resolved.json")

def load_json(path):
    if not os.path.exists(path):
        print(f"[ERROR] 파일 없음: {path}")
        return []
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, list):
        return data
    if isinstance(data, dict) and "items" in data:
        return data["items"]
    return []

def is_bad_title(title):
    return has_any_pattern(title, BAD_TITLE_PATTERNS)

def is_good_product_title(title):
    return has_any_pattern(title, GOOD_PRODUCT_PATTERNS)

def looks_like_product_url(url):
    if not url:
        return False
    low = url.lower()
    bad_parts = [
        "notice",
        "event",
        "board",
        "bbs",
        "javascript:",
        "#",
    ]
    return not any(part in low for part in bad_parts)

def infer_stock_status(title, status_text="", extra_text="", price=None):
    combined = " ".join([
        normalize_whitespace(title),
        normalize_whitespace(status_text),
        normalize_whitespace(extra_text),
    ]).lower()

    if has_any_pattern(combined, OUT_OF_STOCK_PATTERNS):
        return "품절"
    if has_any_pattern(combined, PREORDER_PATTERNS):
        return "예약중"
    if has_any_pattern(combined, IN_STOCK_PATTERNS):
        return "판매중"

    if price is not None:
        return "판매중"

    return "상태 확인중"

def build_item_id(site, title, url):
    raw = f"{normalize_whitespace(site).lower()}|{normalize_title(title)}|{canonicalize_url(url)}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()

def normalize_compare_title(title):
    t = normalize_title(title).lower()
    t = re.sub(r"[^a-z0-9가-힣]+", "", t)
    return t

def normalize_item(raw):
    site = normalize_whitespace(raw.get("site", "unknown"))
    title = normalize_title(raw.get("title") or raw.get("name") or raw.get("product_name") or "")
    price_text = raw.get("price_text") or raw.get("price") or raw.get("display_price") or ""
    price = parse_price(price_text) if not isinstance(raw.get("price"), int) else raw.get("price")

    base_url = raw.get("base_url", "") or raw.get("source_url", "")
    product_url = raw.get("product_url") or raw.get("url") or raw.get("link") or ""
    if base_url:
        product_url = make_absolute_url(base_url, product_url)
    else:
        product_url = canonicalize_url(product_url)

    status = infer_stock_status(
        title=title,
        status_text=raw.get("status", ""),
        extra_text=raw.get("extra_text", ""),
        price=price,
    )

    item = {
        "id": build_item_id(site, title, product_url),
        "site": site,
        "title": title,
        "price": price,
        "price_text": normalize_whitespace(price_text),
        "status": status,
        "extra_text": normalize_whitespace(raw.get("extra_text", "")),
        "image_url": canonicalize_url(raw.get("image_url", "")),
        "product_url": product_url,
        "base_url": base_url,
        "source_url": raw.get("source_url", ""),
        "currency": "KRW",
        "updated_at": now_iso(),
    }
    return item

def should_keep_item(item):
    title = item["title"]

    if not title:
        return False, "빈 제목"

    if is_bad_title(title):
        return False, "공지/이벤트류"

    if not is_good_product_title(title):
        return False, "건담 관련 키워드 부족"

    if not looks_like_product_url(item.get("product_url", "")):
        return False, "비정상 링크"

    return True, ""

def save_to_firestore(db, collection_name, docs, id_field=None):
    batch = db.batch()
    count = 0

    for doc in docs:
        if id_field and id_field in doc:
            ref = db.collection(collection_name).document(str(doc[id_field]))
        else:
            ref = db.collection(collection_name).document()
        batch.set(ref, doc)
        count += 1

        if count % 300 == 0:
            batch.commit()
            batch = db.batch()

    batch.commit()

def merge_items(all_items):
    grouped = {}

    for item in all_items:
        key = normalize_compare_title(item["title"])

        if key not in grouped:
            grouped[key] = {
                "title": item["title"],
                "normalized_title": key,
                "offers": [],
                "best_price": None,
                "lowest_price": None,
                "lowest_price_site": "",
                "lowest_price_url": "",
                "grouped_url": "",
                "status_summary": item["status"],
                "updated_at": now_iso(),
            }

        grouped[key]["offers"].append({
            "site": item["site"],
            "price": item["price"],
            "price_text": item["price_text"],
            "status": item["status"],
            "product_url": item["product_url"],
            "image_url": item["image_url"],
            "updated_at": item["updated_at"],
        })

        offers_with_price = [
            o for o in grouped[key]["offers"]
            if o.get("price") is not None and o.get("product_url")
        ]

        if offers_with_price:
            cheapest = min(offers_with_price, key=lambda x: x["price"])
            grouped[key]["best_price"] = cheapest["price"]
            grouped[key]["lowest_price"] = cheapest["price"]
            grouped[key]["lowest_price_site"] = cheapest.get("site", "")
            grouped[key]["lowest_price_url"] = cheapest.get("product_url", "")

        valid_urls = [
            o["product_url"] for o in grouped[key]["offers"]
            if o.get("product_url")
        ]
        grouped[key]["grouped_url"] = valid_urls[0] if valid_urls else ""

        statuses = [o["status"] for o in grouped[key]["offers"]]
        if "판매중" in statuses:
            grouped[key]["status_summary"] = "판매중"
        elif "예약중" in statuses:
            grouped[key]["status_summary"] = "예약중"
        elif "품절" in statuses and len(set(statuses)) == 1:
            grouped[key]["status_summary"] = "품절"
        else:
            grouped[key]["status_summary"] = "상태 확인중"

    return list(grouped.values())
    
def main():
    db = init_firestore()

    raw_items = load_json(INPUT_FILE)
    print(f"입력 상품 수: {len(raw_items)}")

    cleaned_items = []
    filtered_count = 0

    for raw in raw_items:
        item = normalize_item(raw)
        keep, reason = should_keep_item(item)
        if not keep:
            filtered_count += 1
            continue
        cleaned_items.append(item)

    print(f"정제 후 상품 수: {len(cleaned_items)}")
    print(f"제거된 상품 수: {filtered_count}")

    merged_items = merge_items(cleaned_items)

    print(f"aggregated_items 저장 개수: {len(merged_items)}")
    print(f"site_items_clean 저장 개수: {len(cleaned_items)}")

    save_to_firestore(db, "site_items_clean", cleaned_items, id_field="id")
    save_to_firestore(db, "aggregated_items", merged_items)

    print("Firestore 저장 완료")

if __name__ == "__main__":
    main()
