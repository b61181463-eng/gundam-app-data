import json
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://www.gundamcity.co.kr"
OUTPUT_PATH = Path("data/gundamcity_items.json")

HEADERS = {
    "User-Agent": "Mozilla/5.0",
}


def extract_items(soup):
    items = []
    seen = set()

    for a in soup.select("a[href]"):
        href = a.get("href", "").strip()
        text = a.get_text(" ", strip=True)

        if not href or not text:
            continue

        if not any(g in text.upper() for g in ["PG", "MG", "RG", "HG", "SD"]):
            continue

        url = urljoin(BASE_URL, href)

        key = (text, url)
        if key in seen:
            continue
        seen.add(key)

        items.append({
            "sourcePage": "gundamcity",
            "site": "건담시티",
            "mallName": "건담시티",
            "name": text,
            "title": text,
            "price": "",
            "status": "상태 확인중",
            "stockText": text,
            "url": url,
            "productUrl": url,
        })

    return items


def crawl():
    res = requests.get(BASE_URL, headers=HEADERS)
    soup = BeautifulSoup(res.text, "html.parser")

    items = extract_items(soup)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(items, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[건담시티] {len(items)}개")


if __name__ == "__main__":
    crawl()
