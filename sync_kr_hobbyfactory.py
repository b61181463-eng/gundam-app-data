import json
import re
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://www.hobbyfactory.kr"
CATEGORY_URL = "https://www.hobbyfactory.kr/shop/shopbrand.html?type=Y&xcode=042"
OUTPUT_PATH = Path("data/hobbyfactory_items.json")

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "ko,en;q=0.9",
}


def clean_text(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()


def infer_status(text: str) -> str:
    t = (text or "").lower()
    if any(x in t for x in ["품절", "sold out", "out of stock"]):
        return "품절"
    if any(x in t for x in ["예약", "preorder", "pre-order"]):
        return "예약중"
    if any(x in t for x in ["판매", "구매", "장바구니", "available", "in stock"]):
        return "판매중"
    return "상태 확인중"


def parse_price(text: str) -> str:
    text = clean_text(text)
    m = re.search(r"[\d,]+원", text)
    return m.group(0) if m else text


def extract_cards(soup: BeautifulSoup):
    selectors = [
        ".item-wrap",
        ".item_box",
        ".goods_list_item",
        ".item_cont",
        "li[class*=item]",
        "div[class*=item]",
        "div[class*=goods]",
    ]
    found = []
    for sel in selectors:
        found = soup.select(sel)
        if len(found) >= 8:
            return found
    return found


def parse_item(card):
    link_tag = (
        card.select_one("a[href*='shopdetail']")
        or card.select_one("a[href*='goods']")
        or card.select_one("a[href]")
    )
    if not link_tag:
        return None

    href = link_tag.get("href", "").strip()
    product_url = urljoin(BASE_URL, href)

    name_tag = (
        card.select_one(".item_name")
        or card.select_one(".goods_name")
        or card.select_one(".prd_name")
        or card.select_one("strong")
        or card.select_one("p")
        or link_tag
    )
    name = clean_text(name_tag.get_text(" ", strip=True))

    price_text = clean_text(card.get_text(" ", strip=True))
    price = parse_price(price_text)
    status = infer_status(price_text)

    if not name:
        return None

    return {
        "sourcePage": "hobbyfactory",
        "site": "하비팩토리",
        "mallName": "하비팩토리",
        "name": name,
        "title": name,
        "price": price,
        "status": status,
        "stockText": price_text,
        "url": product_url,
        "productUrl": product_url,
    }


def crawl():
    resp = requests.get(CATEGORY_URL, headers=HEADERS, timeout=20)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    cards = extract_cards(soup)

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
    print(f"[하비팩토리] {len(items)}개 저장 완료")


if __name__ == "__main__":
    crawl()