# -*- coding: utf-8 -*-

import os
import re
import json
import time
import requests
from urllib.parse import urlparse

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/146.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "assets", "data")

# 네 파일명에 맞게 필요하면 여기만 바꿔
INPUT_FILE = os.path.join(DATA_DIR, "stores_kr_all.json")
OUTPUT_FILE = os.path.join(DATA_DIR, "stores_kr_all_resolved.json")
REPORT_FILE = os.path.join(DATA_DIR, "unknown_status_report.json")

COMMON_IN_STOCK = [
    r"판매중",
    r"구매가능",
    r"재고있음",
    r"재고 있음",
    r"in stock",
    r"available",
    r"즉시출고",
    r"당일출고",
    r"바로구매",
    r"장바구니",
    r"구매하기",
    r"add to cart",
    r"buy now",
]

COMMON_OUT_OF_STOCK = [
    r"품절",
    r"일시품절",
    r"sold out",
    r"out of stock",
    r"재고없음",
    r"재고 없음",
    r"판매종료",
    r"soldout",
]

COMMON_PREORDER = [
    r"예약",
    r"예약중",
    r"pre[\s\-]?order",
    r"입고예정",
    r"coming soon",
    r"예약상품",
]

COMMON_COMING_SOON = [
    r"입고예정",
    r"coming soon",
    r"발매예정",
    r"출시예정",
]

# 사이트별로 필요한 예외 패턴은 여기에 계속 추가하면 됨
SITE_PATTERNS = {
    "gundamshop": {
        "in_stock": [
            r"판매중",
            r"장바구니",
            r"구매하기",
        ],
        "out_of_stock": [
            r"품절",
            r"일시품절",
        ],
        "preorder": [
            r"예약",
            r"예약중",
        ],
        "coming_soon": [
            r"입고예정",
        ],
    },
    "thegundambase": {
        "in_stock": [],
        "out_of_stock": [],
        "preorder": [],
        "coming_soon": [],
    },
    "hobbyfactory": {
        "in_stock": [],
        "out_of_stock": [],
        "preorder": [],
        "coming_soon": [],
    },
}

def normalize_text(text):
    if text is None:
        return ""
    text = str(text)
    text = text.replace("\u200b", " ").replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text

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

def save_json(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def has_any_pattern(text, patterns):
    t = normalize_text(text).lower()
    return any(re.search(p, t, re.I) for p in patterns)

def extract_page_text(html):
    text = re.sub(r"<script.*?>.*?</script>", " ", html, flags=re.S | re.I)
    text = re.sub(r"<style.*?>.*?</style>", " ", text, flags=re.S | re.I)
    text = re.sub(r"<noscript.*?>.*?</noscript>", " ", text, flags=re.S | re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    return normalize_text(text)

def detect_site_key(item):
    candidates = [
        normalize_text(item.get("site", "")).lower(),
        normalize_text(item.get("mall_name", "")).lower(),
        normalize_text(item.get("mallName", "")).lower(),
    ]

    url = normalize_text(item.get("product_url") or item.get("url") or item.get("resolved_url") or "")
    if url:
        host = urlparse(url).netloc.lower()
        candidates.append(host)

    joined = " ".join(candidates)

    if "gundamshop" in joined:
        return "gundamshop"
    if "thegundambase" in joined:
        return "thegundambase"
    if "hobbyfactory" in joined:
        return "hobbyfactory"
    return ""

def merged_patterns(site_key, kind):
    base = {
        "in_stock": COMMON_IN_STOCK,
        "out_of_stock": COMMON_OUT_OF_STOCK,
        "preorder": COMMON_PREORDER,
        "coming_soon": COMMON_COMING_SOON,
    }[kind]

    site_extra = SITE_PATTERNS.get(site_key, {}).get(kind, [])
    return base + site_extra

def fetch_page(url):
    try:
        res = requests.get(url, headers=HEADERS, timeout=25)
        res.raise_for_status()
        return res.text
    except Exception as e:
        print(f"[WARN] 상세페이지 요청 실패: {url} / {e}")
        return ""

def detect_status_from_text(text, site_key=""):
    text = normalize_text(text)

    out_patterns = merged_patterns(site_key, "out_of_stock")
    preorder_patterns = merged_patterns(site_key, "preorder")
    coming_patterns = merged_patterns(site_key, "coming_soon")
    in_patterns = merged_patterns(site_key, "in_stock")

    if has_any_pattern(text, out_patterns):
        return "품절"
    if has_any_pattern(text, preorder_patterns):
        return "예약중"
    if has_any_pattern(text, coming_patterns):
        return "입고예정"
    if has_any_pattern(text, in_patterns):
        return "판매중"
    return "상태 확인중"

def resolve_status(item):
    old_status = normalize_text(item.get("status", ""))
    if old_status != "상태 확인중":
        return item, False, "이미 판별됨"

    title = normalize_text(item.get("title") or item.get("name") or "")
    price = item.get("price")
    product_url = normalize_text(
        item.get("product_url") or item.get("url") or item.get("resolved_url") or ""
    )
    site_key = detect_site_key(item)

    seed_text = " ".join([
        title,
        normalize_text(item.get("stock_text", "")),
        normalize_text(item.get("stockText", "")),
        normalize_text(item.get("extra_text", "")),
        normalize_text(item.get("status", "")),
    ])

    # 1차: 기존 필드로 판별
    detected = detect_status_from_text(seed_text, site_key)
    if detected != "상태 확인중":
        item["status"] = detected
        return item, True, "기존 필드"

    # 2차: 상세페이지 판별
    if product_url:
        html = fetch_page(product_url)
        if html:
            page_text = extract_page_text(html)
            detected = detect_status_from_text(page_text, site_key)
            if detected != "상태 확인중":
                item["status"] = detected
                return item, True, "상세페이지"

            # 구매 버튼류만 남아 있으면 판매중으로 보정
            if ("장바구니" in page_text) or ("구매하기" in page_text) or ("BUY NOW" in page_text.upper()):
                item["status"] = "판매중"
                return item, True, "상세페이지 버튼 보정"

    # 3차: 가격 있으면 판매중으로 보정
    if price is not None:
        item["status"] = "판매중"
        return item, True, "가격 보정"

    return item, False, "미분류"

def main():
    items = load_json(INPUT_FILE)
    print(f"전체 상품 수: {len(items)}")

    before_unknown = [x for x in items if normalize_text(x.get("status")) == "상태 확인중"]
    print(f"상태 확인중(수정 전): {len(before_unknown)}")

    changed = 0
    unresolved = []
    reason_count = {}
    status_count = {
        "판매중": 0,
        "품절": 0,
        "예약중": 0,
        "입고예정": 0,
        "상태 확인중": 0,
    }

    resolved_items = []

    for idx, item in enumerate(items, 1):
        old_status = normalize_text(item.get("status", ""))
        item, updated, reason = resolve_status(item)
        new_status = normalize_text(item.get("status", ""))

        reason_count[reason] = reason_count.get(reason, 0) + 1

        if new_status in status_count:
            status_count[new_status] += 1
        else:
            status_count[new_status] = status_count.get(new_status, 0) + 1

        if old_status != new_status:
            changed += 1
            print(f"[{idx}] {normalize_text(item.get('title', ''))[:80]} :: {old_status} -> {new_status} ({reason})")

        if new_status == "상태 확인중":
            unresolved.append({
                "title": normalize_text(item.get("title") or item.get("name") or ""),
                "site": normalize_text(item.get("site", "")),
                "url": normalize_text(item.get("product_url") or item.get("url") or item.get("resolved_url") or ""),
                "reason": reason,
            })

        resolved_items.append(item)
        time.sleep(0.25)

    after_unknown = [x for x in resolved_items if normalize_text(x.get("status")) == "상태 확인중"]

    save_json(OUTPUT_FILE, resolved_items)
    save_json(REPORT_FILE, {
        "before_unknown_count": len(before_unknown),
        "after_unknown_count": len(after_unknown),
        "changed_count": changed,
        "status_count": status_count,
        "reason_count": reason_count,
        "unresolved_examples": unresolved[:100],
    })

    print("")
    print("===== 결과 =====")
    print(f"상태 변경된 상품 수: {changed}")
    print(f"판매중: {status_count.get('판매중', 0)}")
    print(f"품절: {status_count.get('품절', 0)}")
    print(f"예약중: {status_count.get('예약중', 0)}")
    print(f"입고예정: {status_count.get('입고예정', 0)}")
    print(f"상태 확인중(수정 후): {len(after_unknown)}")
    print(f"저장 완료: {OUTPUT_FILE}")
    print(f"리포트 저장 완료: {REPORT_FILE}")

if __name__ == "__main__":
    main()
