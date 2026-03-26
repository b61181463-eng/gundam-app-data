import json
import re
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://www.thegundambase.com"
LIST_URL = "https://www.thegundambase.com"
OUTPUT_JSON_PATH = Path("data/gundambase_items.json")

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8",
}


# -------------------------
# 기본 유틸
# -------------------------
def normalize_space(text):
    return re.sub(r"\s+", " ", str(text or "")).strip()


def normalize_name(name):
    text = normalize_space(name).lower()
    text = re.sub(r"[\[\]\(\)\-_/.,:+]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


# -------------------------
# 외부 URL 차단
# -------------------------
def is_allowed_domain(url):
    try:
        host = (urlparse(url).netloc or "").lower()
    except Exception:
        return False

    return host.endswith("thegundambase.com")


# -------------------------
# 상품명 필터 (핵심)
# -------------------------
def is_valid_product_name(name):
    text = normalize_name(name)

    if len(text) < 5:
        return False

    # ❌ 메뉴 / 카테고리 제거
    bad_patterns = [
        "go to",
        "scale model",
        "reborn one hundred",
        "hi resolution",
        "full mechanics",
        "category",
        "list",
    ]

    if any(p in text for p in bad_patterns):
        return False

    # 영어 메뉴형 텍스트 제거
    if re.fullmatch(r"[a-z0-9\s\-/]+", text) and "gundam" not in text:
        return False

    # 건담 키워드 포함
    good_keywords = [
        "gundam", "건담", "자쿠", "프리덤", "유니콘",
        "mg", "rg", "hg", "pg", "sd"
    ]

    return any(k in text for k in good_keywords)


# -------------------------
# 상태 판별
# -------------------------
def detect_stock(html_text):
    t = html_text.lower()

    if "품절" in t or "sold out" in t:
        return "품절"

    if "예약" in t or "입고예정" in t:
        return "예약/입고예정"

    return "판매중"


def verify_detail_stock(url):
    try:
        res = requests.get(url, headers=HEADERS, timeout=10)
        html = res.text
        return detect_stock(html)
    except Exception:
        return "판매중"


# -------------------------
# 크롤링
# -------------------------
def fetch_html(url):
    res = requests.get(url, headers=HEADERS, timeout=20)
    res.raise_for_status()
    return res.text


def parse_items(html):
    soup = BeautifulSoup(html, "html.parser")

    items = []
    seen = set()

    for a in soup.select("a[href]"):
        href = a.get("href", "").strip()
        if not href:
            continue

        full_url = urljoin(BASE_URL, href)

        # 외부 URL 제거
        if not is_allowed_domain(full_url):
            continue

        name = normalize_space(a.get_text(" ", strip=True))

        if not is_valid_product_name(name):
            continue

        key = full_url + "|" + normalize_name(name)
        if key in seen:
            continue
        seen.add(key)

        # 상태 강제 판별
        stock = verify_detail_stock(full_url)

        items.append({
            "itemId": key,
            "name": name,
            "title": name,
            "canonicalName": normalize_name(name),
            "price": "",
            "stockText": stock,
            "status": stock,
            "source": "gundambase",
            "site": "건담베이스",
            "mallName": "건담베이스",
            "url": full_url,
            "productUrl": full_url,
        })

    return items


# -------------------------
# 저장
# -------------------------
def save_items_json(items):
    OUTPUT_JSON_PATH.parent.mkdir(parents=True, exist_ok=True)

    OUTPUT_JSON_PATH.write_text(
        json.dumps(items, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print(f"[저장] GundamBase JSON / {len(items)}개")


# -------------------------
# 실행
# -------------------------
def main():
    items = []

    try:
        print("[시작] GundamBase 크롤링")
        html = fetch_html(LIST_URL)
        items = parse_items(html)
        print(f"[결과] 추출 개수: {len(items)}")
    except Exception as e:
        print(f"[오류] {e}")

    save_items_json(items)
    print("[완료] GundamBase 종료")


if __name__ == "__main__":
    main()
