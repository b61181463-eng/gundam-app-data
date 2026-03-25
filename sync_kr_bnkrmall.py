import json
import re
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://www.bnkrmall.co.kr"
LIST_URL = "https://www.bnkrmall.co.kr/main/index.do"
OUTPUT_JSON_PATH = Path("data/bnkrmall_items.json")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/134.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8",
}


# ---------------------------
# 기본 유틸
# ---------------------------
def normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()


def normalize_name(name: str) -> str:
    text = normalize_space(name).lower()
    text = re.sub(r"[\[\]\(\)\-_/.,:+]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def is_allowed_domain(url: str) -> bool:
    try:
        host = (urlparse(url).netloc or "").lower()
    except Exception:
        return False

    return host.endswith("bnkrmall.co.kr")


# ---------------------------
# 상태 판별
# ---------------------------
def detect_stock_text(text: str) -> str:
    t = normalize_space(text).lower()

    if "품절" in t or "sold out" in t:
        return "품절"

    if "예약" in t or "입고예정" in t:
        return "예약/입고예정"

    if "판매중" in t or "구매" in t or "장바구니" in t:
        return "판매중"

    return ""


def verify_detail_stock(url: str) -> str:
    try:
        html = requests.get(url, headers=HEADERS, timeout=10).text
        text = html.lower()

        if "품절" in text or "sold out" in text:
            return "품절"

        return "판매중"
    except Exception:
        return "판매중"


# ---------------------------
# 크롤링
# ---------------------------
def fetch_html(url: str) -> str:
    res = requests.get(url, headers=HEADERS, timeout=20)
    res.raise_for_status()
    return res.text


def parse_items(html: str):
    soup = BeautifulSoup(html, "html.parser")

    items = []
    seen = set()

    for a in soup.select("a[href]"):
        href = a.get("href", "").strip()
        if not href:
            continue

        full_url = urljoin(BASE_URL, href)

        if not is_allowed_domain(full_url):
            continue

        name = normalize_space(a.get_text(" ", strip=True))
        if len(name) < 3:
            continue

        key = full_url + "|" + normalize_name(name)
        if key in seen:
            continue
        seen.add(key)

        stock = detect_stock_text(name)

        if not stock:
            stock = verify_detail_stock(full_url)

        items.append({
            "itemId": key,
            "name": name,
            "title": name,
            "canonicalName": normalize_name(name),
            "price": "",
            "stockText": stock,
            "status": stock,
            "source": "bnkrmall",
            "site": "비엔케이알몰",
            "mallName": "비엔케이알몰",
            "url": full_url,
            "productUrl": full_url,
        })

    return items


# ---------------------------
# 저장
# ---------------------------
def save_items_json(items):
    OUTPUT_JSON_PATH.parent.mkdir(parents=True, exist_ok=True)

    OUTPUT_JSON_PATH.write_text(
        json.dumps(items, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print(f"[저장] BNKRMALL JSON 저장 완료 / {len(items)}개")


# ---------------------------
# 실행
# ---------------------------
def main():
    items = []

    try:
        print(f"[시작] BNKRMALL 크롤링")
        html = fetch_html(LIST_URL)
        items = parse_items(html)
        print(f"[결과] 추출 개수: {len(items)}")
    except Exception as e:
        print(f"[오류] {e}")

    # 무조건 저장
    save_items_json(items)
    print("[완료] BNKRMALL 종료")


if __name__ == "__main__":
    main()
