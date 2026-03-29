import json
import re
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://www.modelsale.co.kr"
CATEGORY_URL = "https://www.modelsale.co.kr"
OUTPUT_PATH = Path("data/modelsale_items.json")

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "ko,en;q=0.9",
}


def clean_text(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()


def parse_price(text: str) -> str:
    m = re.search(r"[\d,]+원", text or "")
    return m.group(0) if m else ""


def infer_status(text: str) -> str:
    t = (text or "").lower()
    if "품절" in t or "sold out" in t:
        return "품절"
    if "예약" in t or "preorder" in t or "pre-order" in t:
        return "예약중"
    if "판매" in t or "구매" in t or "장바구니" in t or "available" in t:
        return "판매중"
    return "상태 확인중"


def parse_item(card):
    link_tag = (
        card.select_one("a[href*='shopdetail']")
        or card.select_one("a[href*='product']")
        or card.select_one("a[href]")
    )
    if not link_tag:
        return None

    href = link_tag.get("href", "").strip()
    product_url = urljoin(BASE_URL, href)

    name_tag = (
        card.select_one(".item_name")
        or card.select_one(".goods_name")
        or card.select_one(".name")
        or card.select_one("strong")
        or card.select_one("p")
        or link_tag
    )
    name = clean_text(name_tag.get_text(" ", strip=True))
    if not name:
        return None

    text = clean_text(card.get_text(" ", strip=True))
    price = parse_price(text)
    status = infer_status(text)

    return {
        "sourcePage": "modelsale",
        "site": "네이버 하비 코리아",
        "mallName": "네이버 하비 코리아",
        "name": name,
        "title": name,
        "price": price,
        "status": status,
        "stockText": text,
        "url": product_url,
        "productUrl": product_url,
    }


def crawl():
    resp = requests.get(CATEGORY_URL, headers=HEADERS, timeout=20)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    cards = (
        soup.select(".goods_list_item")
        or soup.select(".item_cont")
        or soup.select("li[class*=item]")
        or soup.select("div[class*=goods]")
    )

    items = []
    seen = set()

    for card in cards:
        item = parse_item(card)
        if not item:
            continue
        key = (item["name"], item["productUrl"])
        if key in seen:
            continue
        seen.add(key)
        items.append(item)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(
        json.dumps(items, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"[네이버 하비 코리아] {len(items)}개 저장 완료")


if __name__ == "__main__":
    crawl()