# resolve_unknown_status.py
# -*- coding: utf-8 -*-

import os
import re
import json
import time
import requests
from urllib.parse import urljoin

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

DATA_DIR = os.path.join("assets", "data")
INPUT_FILE = os.path.join(DATA_DIR, "stores_kr_all.json")   # 네 통합 raw 파일명에 맞게 바꿔도 됨
OUTPUT_FILE = os.path.join(DATA_DIR, "stores_kr_all_resolved.json")

IN_STOCK_PATTERNS = [
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
    r"예약상품",
]

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

def fetch_page_text(url):
    try:
        res = requests.get(url, headers=HEADERS, timeout=25)
        res.raise_for_status()
        html = res.text
        # HTML 전체에서 텍스트만 거칠게 추출
        text = re.sub(r"<script.*?>.*?</script>", " ", html, flags=re.S | re.I)
        text = re.sub(r"<style.*?>.*?</style>", " ", text, flags=re.S | re.I)
        text = re.sub(r"<[^>]+>", " ", text)
        text = normalize_text(text)
        return text
    except Exception as e:
        print(f"[WARN] 상세페이지 요청 실패: {url} / {e}")
        return ""

def resolve_status(item):
    current_status = normalize_text(item.get("status", ""))
    if current_status != "상태 확인중":
        return item

    title = normalize_text(item.get("title", ""))
    price = item.get("price")
    product_url = normalize_text(item.get("product_url") or item.get("url") or "")
    combined_seed = f"{title} {current_status}"

    # 1차: 기존 텍스트 안에서 다시 한번 판별
    if has_any_pattern(combined_seed, OUT_OF_STOCK_PATTERNS):
        item["status"] = "품절"
        return item
    if has_any_pattern(combined_seed, PREORDER_PATTERNS):
        item["status"] = "예약중"
        return item
    if has_any_pattern(combined_seed, IN_STOCK_PATTERNS):
        item["status"] = "판매중"
        return item

    # 2차: 상세페이지 판별
    if product_url:
        page_text = fetch_page_text(product_url)

        if has_any_pattern(page_text, OUT_OF_STOCK_PATTERNS):
            item["status"] = "품절"
            return item

        if has_any_pattern(page_text, PREORDER_PATTERNS):
            item["status"] = "예약중"
            return item

        if has_any_pattern(page_text, IN_STOCK_PATTERNS):
            item["status"] = "판매중"
            return item

        # 상세페이지에 장바구니/구매하기가 있으면 판매중으로 보는 보정
        if ("장바구니" in page_text) or ("구매하기" in page_text):
            item["status"] = "판매중"
            return item

    # 3차: 가격 기반 최종 보정
    if price is not None:
        item["status"] = "판매중"
    else:
        item["status"] = "상태 확인중"

    return item

def main():
    items = load_json(INPUT_FILE)
    print(f"전체 상품 수: {len(items)}")

    unknown_before = [x for x in items if normalize_text(x.get("status")) == "상태 확인중"]
    print(f"상태 확인중(수정 전): {len(unknown_before)}")

    resolved = []
    changed = 0

    for idx, item in enumerate(items, 1):
        old_status = normalize_text(item.get("status"))
        item = resolve_status(item)
        new_status = normalize_text(item.get("status"))

        if old_status != new_status:
            changed += 1
            print(f"[{idx}] {item.get('title','')[:80]} :: {old_status} -> {new_status}")

        resolved.append(item)
        time.sleep(0.3)  # 사이트 부담 줄이기

    unknown_after = [x for x in resolved if normalize_text(x.get("status")) == "상태 확인중"]

    save_json(OUTPUT_FILE, resolved)

    print("")
    print(f"상태 변경된 상품 수: {changed}")
    print(f"상태 확인중(수정 후): {len(unknown_after)}")
    print(f"저장 완료: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()