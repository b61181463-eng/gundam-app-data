import json
import re
from pathlib import Path
from urllib.parse import urljoin

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


def normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()


def normalize_name(name: str) -> str:
    text = normalize_space(name).lower()
    text = text.replace("ver.", "ver").replace("version", "ver")
    text = re.sub(r"\b1/\d+\b", "", text)
    text = re.sub(r"\b\d+/\d+\b", "", text)
    text = re.sub(r"[\[\]\(\)\-_/.,:+]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def looks_like_gundam_name(name: str) -> bool:
    text = normalize_space(name).lower()
    if not text:
        return False

    blocked = [
        "공지", "안내", "이벤트", "문의", "게시판", "로그인",
        "티셔츠", "머그컵", "포스터", "아크릴", "키링", "스티커",
        "가방", "쿠션", "노트", "인형", "문구",
    ]
    if any(b in text for b in blocked):
        return False

    allowed = [
        "건담", "자쿠", "유니콘", "프리덤", "스트라이크", "에어리얼",
        "즈고크", "사자비", "뉴건담", "시난주", "엑시아", "바르바토스",
        "캘리번", "루브리스", "데스티니", "저스티스", "아스트레이",
        "더블오", "톨기스", "윙건담", "짐", "건캐논", "건탱크",
        "mgsd", "mgex", "pg", "mg", "rg", "hg", "sd", "bb", "eg",
        "re/100", "full mechanics", "figure-rise", "30ms", "30mm",
    ]
    return any(a in text for a in allowed)


def detect_stock_text(text: str) -> str:
    t = normalize_space(text).lower()
    if "품절" in t or "sold out" in t or "out of stock" in t:
        return "품절"
    if "예약" in t or "입고예정" in t or "입고 예정" in t:
        return "예약/입고예정"
    if "판매중" in t or "구매" in t or "장바구니" in t:
        return "판매중"
    return "판매중"


def extract_price(text: str) -> str:
    m = re.search(r"([\d,]+)\s*원", text)
    if m:
        return f"{m.group(1)}원"
    return ""


def fetch_html(url: str) -> str:
    res = requests.get(url, headers=HEADERS, timeout=20)
    res.raise_for_status()
    if not res.encoding or res.encoding.lower() == "iso-8859-1":
        res.encoding = res.apparent_encoding or "utf-8"
    return res.text


def parse_items(html: str):
    soup = BeautifulSoup(html, "html.parser")
    items = []
    seen = set()

    # 최대한 넓게 잡는 보수적 방식
    for a in soup.select("a[href]"):
        href = a.get("href", "").strip()
        if not href:
            continue

        full_url = urljoin(BASE_URL, href)
        text = normalize_space(a.get_text(" ", strip=True))
        if not text:
            continue

        if not looks_like_gundam_name(text):
            continue

        key = f"{full_url}|{normalize_name(text)}"
        if key in seen:
            continue
        seen.add(key)

        items.append({
            "itemId": key,
            "name": text,
            "title": text,
            "canonicalName": normalize_name(text),
            "price": extract_price(text),
            "stockText": detect_stock_text(text),
            "status": detect_stock_text(text),
            "source": "bnkrmall",
            "sourceType": "product",
            "site": "비엔케이알몰",
            "mallName": "비엔케이알몰",
            "country": "KR",
            "region": "KR",
            "productUrl": full_url,
            "url": full_url,
            "sourcePage": LIST_URL,
            "verificationSources": ["bnkrmall"],
        })

    return items


def save_items_json(items):
    OUTPUT_JSON_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_JSON_PATH.write_text(
        json.dumps(items, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"[저장] JSON 저장 완료: {OUTPUT_JSON_PATH} / {len(items)}개")


def main():
    items = []

    try:
        print(f"[시작] BNKRMALL 요청: {LIST_URL}")
        html = fetch_html(LIST_URL)
        items = parse_items(html)
        print(f"[정보] 추출 개수: {len(items)}")
    except Exception as e:
        print(f"[실패] BNKRMALL 크롤링 실패 -> {e}")
        items = []

    # 핵심: 실패해도 무조건 저장
    save_items_json(items)
    print("[완료] BNKRMALL 종료")


if __name__ == "__main__":
    main()
