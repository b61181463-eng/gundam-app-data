import json
import os
import re
import time
import hashlib
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Set
from urllib.parse import urljoin, urlparse, parse_qs

import firebase_admin
import requests
from bs4 import BeautifulSoup
from firebase_admin import credentials, firestore
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
import time

@dataclass
class ItemRecord:
    item_id: str
    name: str
    title: str
    price: str
    status: str
    stock_text: str
    mall_name: str
    site: str
    source_page: str
    url: str
    product_url: str
    detail_url: str

def crawl_bnkrmall_selenium() -> List[ItemRecord]:
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.common.by import By
    from webdriver_manager.chrome import ChromeDriverManager

    results: List[ItemRecord] = []
    seen_ids: Set[str] = set()

    print("[BNKR Selenium 시작]")

    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1400,2200")

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options,
    )

    try:
        url = "https://m.bnkrmall.co.kr/mw/goods/category.do?cate=1576&cateName=%EA%B1%B4%ED%94%84%EB%9D%BC&endGoods=Y"
        driver.get(url)
        time.sleep(4)

        anchors = driver.find_elements(By.CSS_SELECTOR, "a[href*='detail.do']")
        print(f"[BNKR 링크 개수] {len(anchors)}")

        links: Set[str] = set()
        for el in anchors:
            href = (el.get_attribute("href") or "").strip()
            if href and "detail.do" in href and "gno=" in href:
                links.add(href)

        print(f"[BNKR 상세 링크] {len(links)}")

        for idx, link in enumerate(sorted(links), start=1):
            try:
                driver.get(link)
                time.sleep(2)

                html = driver.page_source
                soup = BeautifulSoup(html, "html.parser")
                page_text = soup.get_text(" ", strip=True)

                raw_title = parse_title_from_soup(soup) or ""
                title_only = clean_product_name(raw_title)
                joined = f"{title_only} {page_text}".strip()

                if not title_only:
                    if DEBUG:
                        print(f"[BNKR 탈락:title없음] {link}")
                    continue

                invalid_titles = {
                    "반다이남코코리아몰",
                    "bnkrmall",
                    "상품명",
                }
                if title_only.strip().lower() in invalid_titles:
                    if DEBUG:
                        print(f"[BNKR 탈락:잘못된제목] {title_only} / {link}")
                    continue

                if is_excluded(title_only):
                    if DEBUG:
                        print(f"[BNKR 탈락:제외키워드] {title_only} / {link}")
                    continue

                if is_non_gundam_figure_like(title_only):
                    if DEBUG:
                        print(f"[BNKR 탈락:피규어류] {title_only} / {link}")
                    continue

                if not is_valid_gundam_plamodel(title_only, joined):
                    if DEBUG:
                        print(f"[BNKR 탈락:건담판별실패] {title_only} / {link}")
                    continue

                price = parse_price_from_soup(soup) or ""
                status = parse_status_from_soup(soup) or "상태 확인중"
                stock_text = status

                parsed_price = price_to_int(price)
                if parsed_price is not None and parsed_price < 3000:
                    if DEBUG:
                        print(f"[BNKR 탈락:비정상가격] {title_only} / {price} / {link}")
                    continue

                parsed = urlparse(link)
                qs = parse_qs(parsed.query)
                gno = qs.get("gno", [""])[0]
                goodsno = qs.get("goodsno", [""])[0]
                stable_key = gno or goodsno or link
                item_id = f"bnkr_{sha_id(stable_key)}"

                if item_id in seen_ids:
                    continue
                seen_ids.add(item_id)

                results.append(
                    ItemRecord(
                        item_id=item_id,
                        name=title_only,
                        title=title_only,
                        price=price,
                        status=status,
                        stock_text=stock_text,
                        mall_name="반다이남코코리아몰",
                        site="bnkrmall",
                        source_page="kr_bnkrmall",
                        url=link,
                        product_url=link,
                        detail_url=link,
                    )
                )

                if DEBUG:
                    print(f"[BNKR 통과] {title_only} / {price} / {status}")

            except Exception as e:
                print(f"[BNKR 상세 루프 실패] {link} / {type(e).__name__}: {e}")

            if idx % 10 == 0:
                print(f"[BNKR 진행] {idx}/{len(links)}")

    finally:
        driver.quit()

    print(f"[BNKR 최종] {len(results)}")
    return results

DEBUG = False
SERVICE_ACCOUNT_PATH = "serviceAccountKey.json"
COLLECTION_NAME = "aggregated_items"

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

TIMEOUT = 20
REQUEST_SLEEP = 0.3


def is_notice_like(title: str, text: str) -> bool:
    t = f"{title} {text}".lower()

    notice_keywords = [
        "공지",
        "공지사항",
        "입고 안내",
        "입고공지",
        "판매 일정",
        "예약 안내",
        "발매 일정",
        "notice",
        "announcement",
    ]

    return any(k in t for k in notice_keywords)

def normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()


def sha_id(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()[:24]


def extract_price(text: str) -> str:
    if not text:
        return ""
    cleaned = text.replace(",", "")
    match = re.search(r"(\d{2,9})\s*원", cleaned)
    if match:
        n = int(match.group(1))
        return f"{n:,}원"

    match = re.search(r"(\d{4,9})", cleaned)
    if match:
        n = int(match.group(1))
        return f"{n:,}원"

    return ""


def looks_like_gundam(text: str) -> bool:
    t = (text or "").upper()

    include_keywords = [
        "건담",
        "GUNDAM",
        "건프라",
        "GUNPLA",
        "MG",
        "RG",
        "HG",
        "PG",
        "SD",
        "MGEX",
        "MGSD",
        "RE/100",
        "FULL MECHANICS",
        "30MS",
        "30MM",
        "에어리얼",
        "유니콘",
        "사자비",
        "자쿠",
        "뉴건담",
        "스트라이크",
        "프리덤",
        "저스티스",
        "데스티니",
        "바르바토스",
        "헤비암즈",
        "윙건담",
        "즈고크",
        "건캐논",
        "건탱크",
        "시난주",
        "제타",
        "더블오",
        "엑시아",
        "릭돔",
        "돔",
    ]
    return any(k in t for k in include_keywords)

def clean_product_name(title: str) -> str:
    text = normalize_space(title)

    # 건담샵 쪽에서 뒤에 붙는 긴 숫자 제거
    # 예: "MG 건담 발바토스 4573102661234"
    text = re.sub(r"\s+\d{6,}$", "", text)

    # 앞뒤 불필요 공백 정리
    text = normalize_space(text)
    return text


def is_non_gundam_figure_like(text: str) -> bool:
    t = (text or "").upper()

    exclude_keywords = [
        "피규어",
        "FIGURE",
        "완성품",
        "액션피규어",
        "ACTION FIGURE",
        "넨도로이드",
        "NENDOROID",
        "프라イズ",
        "경품",
        "봉제",
        "인형",
        "아크릴",
        "스탠드",
        "포스터",
        "캔뱃지",
        "키링",
        "머그컵",
        "의류",
        "티셔츠",
        "쿠션",
        "가방",
        "메탈빌드",
        "METAL BUILD",
        "로봇혼",
        "ROBOT魂",
        "초합금",
        "S.H.FIGUARTS",
        "SHFIGUARTS",
        "피규아츠",
    ]

    return any(k in t for k in exclude_keywords)


def is_valid_gundam_plamodel(title: str, joined_text: str) -> bool:
    cleaned_title = clean_product_name(title)
    merged = f"{cleaned_title} {joined_text}"

    if is_non_gundam_figure_like(cleaned_title):
        return False

    if not looks_like_gundam(merged):
        return False

    return True

def is_excluded(text: str) -> bool:
    t = normalize_space(text).upper()

    exclude_keywords = [
        "쿠폰",
        "사은품",
        "증정",
        "이벤트",
        "공지사항",
        "NOTICE",
    ]

    return any(k in t for k in exclude_keywords)


def normalize_status(text: str) -> str:
    t = (text or "").lower()

    if any(k in t for k in ["판매중", "구매가능", "장바구니", "바로구매", "in stock", "available"]):
        return "판매중"
    if any(k in t for k in ["예약", "pre-order", "preorder", "예약구매", "예약중"]):
        return "예약중"
    if any(k in t for k in ["품절", "sold out", "out of stock", "일시품절"]):
        return "품절"
    if any(k in t for k in ["입고예정", "coming soon"]):
        return "입고예정"
    return "상태 확인중"


def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    return s


def fetch_html(session: requests.Session, url: str, encoding: Optional[str] = None) -> str:
    resp = session.get(url, timeout=TIMEOUT)
    resp.raise_for_status()

    if encoding:
        resp.encoding = encoding
    elif not resp.encoding or resp.encoding.lower() == "iso-8859-1":
        resp.encoding = resp.apparent_encoding or "utf-8"

    return resp.text


def soup_from_url(session: requests.Session, url: str, encoding: Optional[str] = None) -> BeautifulSoup:
    html = fetch_html(session, url, encoding=encoding)
    return BeautifulSoup(html, "html.parser")


def init_firestore():
    if not os.path.exists(SERVICE_ACCOUNT_PATH):
        raise FileNotFoundError(
            f"{SERVICE_ACCOUNT_PATH} 파일이 없습니다. "
            f"merge_kr_crosscheck.py 와 같은 폴더에 넣어줘."
        )

    if not firebase_admin._apps:
        cred = credentials.Certificate(SERVICE_ACCOUNT_PATH)
        firebase_admin.initialize_app(cred)

    return firestore.client()


def parse_title_from_soup(soup: BeautifulSoup) -> str:
    meta_candidates = [
        ('meta[property="og:title"]', "content"),
        ('meta[name="title"]', "content"),
        ('meta[name="twitter:title"]', "content"),
    ]
    for selector, attr in meta_candidates:
        el = soup.select_one(selector)
        if el and el.get(attr):
            title = normalize_space(el.get(attr))
            if title:
                return title

    selectors = [
        "h1",
        "h2",
        "h3",
        ".goods_name",
        ".item_name",
        ".prd_name",
        ".product_name",
        ".tit",
        ".title",
        ".name",
        ".subject",
        ".infoArea h3",
        ".headingArea h2",
    ]

    bad_titles = {
        "반다이남코코리아몰",
        "bnkrmall",
        "상품명",
    }

    # 1순위: 화면 본문 selector
    for selector in selectors:
        for el in soup.select(selector):
            text = normalize_space(el.get_text(" ", strip=True))
            if not text:
                continue
            if text.strip().lower() in bad_titles:
                continue
            if len(text) < 2:
                continue
            return text

    # 2순위: 메타 태그
    meta_candidates = [
        ('meta[property="og:title"]', "content"),
        ('meta[name="title"]', "content"),
        ('meta[name="twitter:title"]', "content"),
    ]
    for selector in selectors:
        for el in soup.select(selector):
            title = normalize_space(el.get_text(" ", strip=True))
            if 2 <= len(title) <= 300:
                return title
    
    body_text = normalize_space(soup.get_text(" ", strip=True))
    match = re.search(
        r"((?:MGEX|MGSD|PG|MG|RG|HG|SD|RE/100|FULL MECHANICS)[^\n]{0,80})",
        body_text,
        re.IGNORECASE,
    )
    if match:
        return normalize_space(match.group(1))

    return ""


def parse_price_from_soup(soup: BeautifulSoup) -> str:
    selectors = [
        ".price",
        ".sell_price",
        ".goods_price",
        ".product_price",
        ".price_area",
        ".price_wrap",
        ".txt_price",
        ".pricebox",
        ".priceBox",
        ".infoArea",
        ".item_detail_tit",
    ]
    for selector in selectors:
        for el in soup.select(selector):
            price = extract_price(el.get_text(" ", strip=True))
            if price:
                return price

    text = soup.get_text(" ", strip=True)
    return extract_price(text)


def parse_status_from_soup(soup: BeautifulSoup) -> str:
    selectors = [
        ".soldout",
        ".state",
        ".status",
        ".goods_state",
        ".prd_state",
        ".icon_area",
        ".label_area",
        ".badge",
    ]
    for selector in selectors:
        texts = [normalize_space(el.get_text(" ", strip=True)) for el in soup.select(selector)]
        joined = " ".join(t for t in texts if t)
        status = normalize_status(joined)
        if status != "상태 확인중":
            return status

    page_text = soup.get_text(" ", strip=True)
    return normalize_status(page_text)

MODELSALE_SEEDS = [
    "https://www.modelsale.co.kr/modelsale/poprec/cat_detail.php?idx1=3&idx2=1&idx3=2&idx4=1",   # Master Grade
    "https://www.modelsale.co.kr/modelsale/poprec/cat_detail.php?idx1=3&idx2=1&idx3=4&idx4=1",   # Real Grade
    "https://www.modelsale.co.kr/modelsale/poprec/cat_detail.php?idx1=3&idx2=1&idx3=5&idx4=1",   # HGUC
    "https://www.modelsale.co.kr/modelsale/poprec/cat_detail.php?idx1=3&idx2=1&idx3=19&idx4=1",  # Seed
    "https://www.modelsale.co.kr/modelsale/poprec/cat_detail.php?idx1=3&idx2=1&idx3=34",         # Full Mechanics
]

GUNDAMBASE_SEEDS = [
    "https://www.thegundambase.co.kr/product/list.html?cate_no=42",
    "https://www.thegundambase.co.kr/product/list.html?cate_no=43",
    "https://www.thegundambase.co.kr/product/list.html?cate_no=44",
]

GUNDAMSHOP_SEEDS = [
    "https://www.gundamshop.co.kr/theme/Reserve.html?cate=0001&ordr=wol_panme&sort=DESC",
    "https://www.gundamshop.co.kr/theme/Reserve.html?cate=0001&ordr=reg_time&sort=DESC",
    "https://www.gundamshop.co.kr/theme/Reserve.html?cate=&ordr=reg_time&sort=DESC",
    "https://www.gundamshop.co.kr/Search/Search.html?key=건담&mode=reserve",
    "https://www.gundamshop.co.kr/Search/Search.html?key=MG&mode=reserve",
    "https://www.gundamshop.co.kr/Search/Search.html?key=RG&mode=reserve",
    "https://www.gundamshop.co.kr/Search/Search.html?key=HG&mode=reserve",
]

HOBBYFACTORY_SEEDS = [
    "https://www.hobbyfactory.kr/shop/shopbrand.html?type=Y&xcode=042",
    "https://www.hobbyfactory.kr/shop/shopbrand.html?mcode=006&type=N&xcode=042",
    "https://www.hobbyfactory.kr/m/product_list.html?mcode=001&type=N&xcode=042",
    "https://www.hobbyfactory.kr/shop/shopbrand.html?mcode=015&xcode=042",
    "https://www.hobbyfactory.kr/shop/shopbrand.html?mcode=020&type=N&xcode=042",
    "https://www.hobbyfactory.kr/shop/shopbrand.html?mcode=022&type=N&xcode=042",
]

GUNDAMCITY_SEEDS = [
    "https://www.gundamcity.co.kr/shop/shopbrand.html?type=X&xcode=019",
    "https://www.gundamcity.co.kr/m/product_list.html?mcode=003&scode=008&type=M&xcode=019",
    "https://www.gundamcity.co.kr/m/product_list.html?mcode=015&type=M&xcode=019",
    "https://www.gundamcity.co.kr/m/product_list.html?mcode=005&scode=002&sort=price&type=X&xcode=019",
]

BNKR_SEEDS = [
    "https://m.bnkrmall.co.kr/mw/goods/new.do?endGoods=Y",
    "https://m.bnkrmall.co.kr/mw/goods/category.do?cate=1576&cateName=%EA%B1%B4%ED%94%84%EB%9D%BC&endGoods=Y",
    "https://m.bnkrmall.co.kr/mw/goods/category.do?brandIdx=180,403&cate=1576&cateName=%EA%B1%B4%ED%94%84%EB%9D%BC&endGoods=Y",
]

def extract_modelsale_candidate_links(soup: BeautifulSoup, base_url: str) -> Set[str]:
    links: Set[str] = set()

    for a in soup.select("a[href]"):
        href = a.get("href", "").strip()
        if not href:
            continue

        full = urljoin(base_url, href)
        lower = full.lower()

        if "modelsale.co.kr" not in lower:
            continue

        if "/modelsale/poprec/detail.php" in lower and "no=" in lower:
            links.add(full)

    return links

def extract_gundamcity_candidate_links(soup: BeautifulSoup, base_url: str) -> Set[str]:
    links: Set[str] = set()

    for a in soup.select("a[href]"):
        href = a.get("href", "").strip()
        if not href:
            continue

        full = urljoin(base_url, href)
        lower = full.lower()

        if "gundamcity.co.kr" not in lower:
            continue

        strong_patterns = [
            "/m/product.html",
            "/shop/shopdetail.html",
            "branduid=",
            "goodsno=",
            "product_no=",
        ]

        weak_patterns = [
            "shopbrand.html",
            "product_list.html",
            "xcode=",
            "mcode=",
            "scode=",
            "sort=",
            "type=",
            "search",
            "page=",
        ]

        if any(p in lower for p in strong_patterns):
            links.add(full)
            continue

        if not any(p in lower for p in weak_patterns):
            links.add(full)

    return links

def extract_hobbyfactory_candidate_links(soup: BeautifulSoup, base_url: str) -> Set[str]:
    links: Set[str] = set()

    for a in soup.select("a[href]"):
        href = a.get("href", "").strip()
        if not href:
            continue

        full = urljoin(base_url, href)
        lower = full.lower()

        if "hobbyfactory.kr" not in lower:
            continue

        if "/shop/shopdetail.html" in lower and "branduid=" in lower:
            links.add(full)
            continue

        if "/m/product.html" in lower and "branduid=" in lower:
            links.add(full)
            continue

    return links

def extract_gundamshop_candidate_links(soup: BeautifulSoup, base_url: str) -> Set[str]:
    links: Set[str] = set()

    for a in soup.select("a[href]"):
        href = a.get("href", "").strip()
        if not href:
            continue

        full = urljoin(base_url, href)
        lower = full.lower()

        if "gundamshop.co.kr" not in lower:
            continue

        if any(x in lower for x in [
            "cate=",
            "sort=",
            "page=",
            "search",
            "reserve.html",
            "/theme/",
            "/search/",
            "/category/",
            "/list",
        ]):
            if not any(y in lower for y in [
                "/goods/view",
                "itemcode=",
                "goodsno=",
                "product_no=",
                "/goods/",
                "/item/",
            ]):
                continue

        if any(x in lower for x in [
            "/goods/view",
            "itemcode=",
            "goodsno=",
            "product_no=",
            "/goods/",
            "/item/",
        ]):
            links.add(full)

    return links


def extract_bnkr_candidate_links(soup: BeautifulSoup, base_url: str) -> Set[str]:
    links: Set[str] = set()

    def try_add(full: str):
        lower = full.lower()
        if "bnkrmall.co.kr" not in lower:
            return
        if "detail.do" in lower and ("gno=" in lower or "goodsno=" in lower):
            links.add(full)

    for a in soup.select("a[href]"):
        href = a.get("href", "").strip()
        if href:
            full = urljoin(base_url, href)
            try_add(full)

        onclick = a.get("onclick", "") or ""

        m = re.search(r"gno\s*=\s*['\"]?(\d+)", onclick, re.IGNORECASE)
        if m:
            gno = m.group(1)
            try_add(f"https://m.bnkrmall.co.kr/mw/goods/detail.do?gno={gno}")

        m2 = re.search(r"detail\.do\?[^'\"]*gno=(\d+)", onclick, re.IGNORECASE)
        if m2:
            gno = m2.group(1)
            try_add(f"https://m.bnkrmall.co.kr/mw/goods/detail.do?gno={gno}")

        for attr_name, attr_value in a.attrs.items():
            value = " ".join(attr_value) if isinstance(attr_value, list) else str(attr_value)
            if attr_name.lower() in {"data-gno", "data-no", "data-goodsno"}:
                m3 = re.search(r"\b(\d{4,})\b", value)
                if m3:
                    gno = m3.group(1)
                    try_add(f"https://m.bnkrmall.co.kr/mw/goods/detail.do?gno={gno}")

    html = str(soup)
    for gno in re.findall(r"detail\.do\?[^\"' >]*gno=(\d+)", html, re.IGNORECASE):
        try_add(f"https://m.bnkrmall.co.kr/mw/goods/detail.do?gno={gno}")

    return links

def parse_modelsale_detail(session: requests.Session, url: str) -> Optional[ItemRecord]:
    try:
        soup = soup_from_url(session, url)
        page_text = soup.get_text(" ", strip=True)
        title = parse_title_from_soup(soup)
        title_only = clean_product_name(title)
        joined = f"{title_only} {page_text}"

        if is_notice_like(title_only, page_text):
            status = "공지"

        if not title_only:
            return None

        if not is_valid_gundam_plamodel(title_only, joined):
            return None

        if is_excluded(title_only):
            return None

        price = parse_price_from_soup(soup)
        status = parse_status_from_soup(soup)
        stock_text = status

        parsed = urlparse(url)
        qs = parse_qs(parsed.query)
        no = qs.get("no", [""])[0]
        stable_key = no or url
        item_id = f"modelsale_{sha_id(stable_key)}"

        return ItemRecord(
            item_id=item_id,
            name=title_only,
            title=title_only,
            price=price,
            status=status,
            stock_text=stock_text,
            mall_name="모델세일",
            site="modelsale",
            source_page="kr_modelsale",
            url=url,
            product_url=url,
            detail_url=url,
        )
    except Exception as e:
        print(f"[모델세일 상세 파싱 실패] {url} / {type(e).__name__}: {e}")
        return None

def parse_gundambase_detail(session: requests.Session, url: str) -> Optional[ItemRecord]:
    try:
        soup = soup_from_url(session, url)
        page_text = soup.get_text(" ", strip=True)
        title = parse_title_from_soup(soup)
        title_only = normalize_space(title)
        joined = f"{title} {page_text}"

        if not title_only:
            return None

        if not looks_like_gundam(joined):
            return None

        if is_excluded(title_only):
            return None

        price = parse_price_from_soup(soup)
        status = parse_status_from_soup(soup)
        stock_text = status

        parsed = urlparse(url)
        qs = parse_qs(parsed.query)
        product_no = qs.get("product_no", [""])[0]
        stable_key = product_no or url
        item_id = f"gundambase_{sha_id(stable_key)}"

        return ItemRecord(
            item_id=item_id,
            name=title_only,
            title=title_only,
            price=price,
            status=status,
            stock_text=stock_text,
            mall_name="건담베이스",
            site="gundambase",
            source_page="kr_gundambase",
            url=url,
            product_url=url,
            detail_url=url,
        )
    except Exception as e:
        print(f"[건담베이스 상세 파싱 실패] {url} / {type(e).__name__}: {e}")
        return None

def parse_hobbyfactory_detail(session: requests.Session, url: str) -> Optional[ItemRecord]:
    try:
        soup = soup_from_url(session, url)
        page_text = soup.get_text(" ", strip=True)
        title = parse_title_from_soup(soup)
        title_only = clean_product_name(title)
        joined = f"{title_only} {page_text}"
        print(f"[하비팩토리 상세 진입] {url}")

        if not title_only:
            print(f"[하비팩토리 탈락:title없음] {url}")
            return None

        if not is_valid_gundam_plamodel(title_only, joined):
            return None

        if is_excluded(title_only):
            print(f"[하비팩토리 탈락:제외키워드] {title_only} / {url}")
            return None

        price = parse_price_from_soup(soup)
        status = parse_status_from_soup(soup)
        stock_text = status

        parsed = urlparse(url)
        qs = parse_qs(parsed.query)
        branduid = qs.get("branduid", [""])[0]
        stable_key = branduid or url
        item_id = f"hobbyfactory_{sha_id(stable_key)}"

        if DEBUG:
            print(f"[하비팩토리 통과] {title_only} / {price} / {status}")

        return ItemRecord(
            item_id=item_id,
            name=title_only,
            title=title_only,
            price=price,
            status=status,
            stock_text=stock_text,
            mall_name="하비팩토리",
            site="hobbyfactory",
            source_page="kr_hobbyfactory",
            url=url,
            product_url=url,
            detail_url=url,
        )
    except Exception as e:
        print(f"[하비팩토리 상세 파싱 실패] {url} / {type(e).__name__}: {e}")
        return None

def parse_gundamcity_detail(session: requests.Session, url: str) -> Optional[ItemRecord]:
    try:
        soup = soup_from_url(session, url)
        page_text = soup.get_text(" ", strip=True) if soup else ""

        raw_title = parse_bnkr_title_from_soup(soup) or ""
        title_only = clean_product_name(raw_title)
        joined = f"{title_only} {page_text}".strip()
        
        if not title_only:
            return None

        if is_excluded(title_only):
            return None

        if is_non_gundam_figure_like(title_only):
            return None

        if not looks_like_gundam(joined):
            return None

        price = parse_price_from_soup(soup) or ""
        status = parse_status_from_soup(soup) or "상태 확인중"
        stock_text = status

        parsed = urlparse(url)
        qs = parse_qs(parsed.query)

        branduid = qs.get("branduid", [""])[0]
        goodsno = qs.get("goodsno", [""])[0]
        product_no = qs.get("product_no", [""])[0]

        stable_key = branduid or goodsno or product_no or url
        item_id = f"gundamcity_{sha_id(stable_key)}"

        clean_title = clean_product_name(title_only)
        if not clean_title:
            return None
        # 잘못 잡힌 사이트명/잡문서 제거
        invalid_titles = {
            "건담시티",
            "gundamcity",
            "상품명",
        }

        if clean_title.strip().lower() in invalid_titles:
            return None

        parsed_price = price_to_int(price)
        if parsed_price is not None and parsed_price < 3000:
            return None
            
        return ItemRecord(
            item_id=item_id,
            name=clean_title,
            title=clean_title,
            price=price,
            status=status,
            stock_text=stock_text,
            mall_name="건담시티",
            site="gundamcity",
            source_page="kr_gundamcity",
            url=url,
            product_url=url,
            detail_url=url,
        )
    except Exception as e:
        print(f"[건담시티 상세 파싱 실패] {url} / {type(e).__name__}: {e}")
        return None

def parse_gundamshop_detail(session: requests.Session, url: str) -> Optional[ItemRecord]:
    try:
        soup = soup_from_url(session, url)
        page_text = soup.get_text(" ", strip=True)
        title = parse_title_from_soup(soup)
        title_only = clean_product_name(title)
        joined = f"{title_only} {page_text}"

        if not title:
            print(f"[건담샵 탈락:title없음] {url}")
            return None

        if not is_valid_gundam_plamodel(title_only, joined):
            return None

        if is_excluded(title_only):
            print(f"[건담샵 탈락:제외키워드] {title} / {url}")
            return None

        price = parse_price_from_soup(soup)
        status = parse_status_from_soup(soup)
        stock_text = status

        parsed = urlparse(url)
        qs = parse_qs(parsed.query)
        code = ""
        for key in ["ItemCode", "itemcode", "goodsNo", "goodsno", "product_no"]:
            if key in qs and qs[key]:
                code = qs[key][0]
                break

        stable_key = code or url
        item_id = f"gundamshop_{sha_id(stable_key)}"

        clean_title = clean_product_name(title_only)
        if not clean_title:
            print(f"[건담샵 탈락:정리후제목없음] {url}")
            return None

        if DEBUG:
            print(f"[건담샵 통과] {clean_title} / {price} / {status}")

        return ItemRecord(
            item_id=item_id,
            name=clean_title,
            title=clean_title,
            price=price,
            status=status,
            stock_text=stock_text,
            mall_name="건담샵",
            site="gundamshop",
            source_page="kr_gundamshop",
            url=url,
            product_url=url,
            detail_url=url,
        )
    except Exception as e:
        print(f"[건담샵 상세 파싱 실패] {url} / {type(e).__name__}: {e}")
        return None


def parse_bnkr_detail(session: requests.Session, url: str) -> Optional[ItemRecord]:
    try:
        soup = soup_from_url(session, url)
        page_text = soup.get_text(" ", strip=True) if soup else ""

        raw_title = parse_title_from_soup(soup) or ""
        title_only = clean_product_name(raw_title)
        joined = f"{title_only} {page_text}".strip()

        if not title_only:
            if DEBUG:
                print(f"[BNKR 탈락:title없음] {url}")
            return None

        # 사이트명/잡문서 제거
        invalid_titles = {
            "반다이남코코리아몰",
            "bnkrmall",
            "상품명",
        }
        if title_only.strip().lower() in invalid_titles:
            if DEBUG:
                print(f"[BNKR 탈락:잘못된제목] {title_only} / {url}")
            return None

        if is_excluded(title_only):
            if DEBUG:
                print(f"[BNKR 탈락:제외키워드] {title_only} / {url}")
            return None

        if is_non_gundam_figure_like(title_only):
            if DEBUG:
                print(f"[BNKR 탈락:피규어류] {title_only} / {url}")
            return None

        if not is_valid_gundam_plamodel(title_only, joined):
            if DEBUG:
                print(f"[BNKR 탈락:건담판별실패] {title_only} / {url}")
            return None

        price = parse_price_from_soup(soup) or ""
        status = parse_status_from_soup(soup) or "상태 확인중"
        stock_text = status

        parsed_price = price_to_int(price)
        if parsed_price is not None and parsed_price < 3000:
            if DEBUG:
                print(f"[BNKR 탈락:비정상가격] {title_only} / {price} / {url}")
            return None

        parsed = urlparse(url)
        qs = parse_qs(parsed.query)

        gno = qs.get("gno", [""])[0]
        goodsno = qs.get("goodsno", [""])[0]
        stable_key = gno or goodsno or url
        item_id = f"bnkr_{sha_id(stable_key)}"

        clean_title = clean_product_name(title_only)
        if not clean_title:
            return None

        if DEBUG:
            print(f"[BNKR 통과] {clean_title} / {price} / {status}")

        return ItemRecord(
            item_id=item_id,
            name=clean_title,
            title=clean_title,
            price=price,
            status=status,
            stock_text=stock_text,
            mall_name="반다이남코코리아몰",
            site="bnkrmall",
            source_page="kr_bnkrmall",
            url=url,
            product_url=url,
            detail_url=url,
        )
    except Exception as e:
        print(f"[BNKR 상세 파싱 실패] {url} / {type(e).__name__}: {e}")
        return None

def crawl_gundambase(session: requests.Session) -> List[ItemRecord]:
    all_links: Set[str] = set()

    for seed in GUNDAMBASE_SEEDS:
        try:
            print(f"[건담베이스 목록] {seed}")
            soup = soup_from_url(session, seed)
            links = extract_gundambase_candidate_links(soup, seed)
            print(f"  후보 링크 {len(links)}개")
            all_links.update(links)
            time.sleep(REQUEST_SLEEP)
        except Exception as e:
            print(f"[건담베이스 목록 실패] {seed} / {type(e).__name__}: {e}")

    print(f"[건담베이스] 상세 후보 총 {len(all_links)}개")

    results: List[ItemRecord] = []
    seen_ids: Set[str] = set()

    for idx, link in enumerate(sorted(all_links), start=1):
        try:
            item = parse_gundambase_detail(session, link)
            if item and item.item_id not in seen_ids:
                results.append(item)
                seen_ids.add(item.item_id)
        except Exception as e:
            print(f"[건담베이스 상세 루프 실패] {link} / {type(e).__name__}: {e}")

        if idx % 10 == 0:
            print(f"[건담베이스] 상세 진행 {idx}/{len(all_links)}")

        time.sleep(REQUEST_SLEEP)

    print(f"[건담베이스] 최종 수집 {len(results)}개")
    return results

def crawl_modelsale(session: requests.Session) -> List[ItemRecord]:
    all_links: Set[str] = set()

    for seed in MODELSALE_SEEDS:
        try:
            print(f"[모델세일 목록] {seed}")
            soup = soup_from_url(session, seed)
            links = extract_modelsale_candidate_links(soup, seed)
            print(f"  후보 링크 {len(links)}개")
            all_links.update(links)
            time.sleep(REQUEST_SLEEP)
        except Exception as e:
            print(f"[모델세일 목록 실패] {seed} / {type(e).__name__}: {e}")

    print(f"[모델세일] 상세 후보 총 {len(all_links)}개")

    results: List[ItemRecord] = []
    seen_ids: Set[str] = set()

    for idx, link in enumerate(sorted(all_links), start=1):
        try:
            item = parse_modelsale_detail(session, link)
            if item and item.item_id not in seen_ids:
                results.append(item)
                seen_ids.add(item.item_id)
        except Exception as e:
            print(f"[모델세일 상세 루프 실패] {link} / {type(e).__name__}: {e}")

        if idx % 10 == 0:
            print(f"[모델세일] 상세 진행 {idx}/{len(all_links)}")

        time.sleep(REQUEST_SLEEP)

    print(f"[모델세일] 최종 수집 {len(results)}개")
    return results

def crawl_hobbyfactory(session: requests.Session) -> List[ItemRecord]:
    all_links: Set[str] = set()

    for seed in HOBBYFACTORY_SEEDS:
        try:
            print(f"[하비팩토리 목록] {seed}")
            soup = soup_from_url(session, seed)
            links = extract_hobbyfactory_candidate_links(soup, seed)
            print(f"  후보 링크 {len(links)}개")
            all_links.update(links)
            time.sleep(REQUEST_SLEEP)
        except Exception as e:
            print(f"[하비팩토리 목록 실패] {seed} / {type(e).__name__}: {e}")

    print(f"[하비팩토리] 상세 후보 총 {len(all_links)}개")

    results: List[ItemRecord] = []
    seen_ids: Set[str] = set()

    for idx, link in enumerate(sorted(all_links), start=1):
        try:
            item = parse_hobbyfactory_detail(session, link)
            if item and item.item_id not in seen_ids:
                results.append(item)
                seen_ids.add(item.item_id)
        except Exception as e:
            print(f"[하비팩토리 상세 루프 실패] {link} / {type(e).__name__}: {e}")

        if DEBUG and idx % 10 == 0:
            print(f"[하비팩토리] 상세 진행 {idx}/{len(all_links)}")

        time.sleep(REQUEST_SLEEP)

    print(f"[하비팩토리] 최종 수집 {len(results)}개")
    return results

def crawl_gundamcity(session: requests.Session) -> List[ItemRecord]:
    all_links: Set[str] = set()

    for seed in GUNDAMCITY_SEEDS:
        try:
            print(f"[건담시티 목록] {seed}")
            soup = soup_from_url(session, seed)
            links = extract_gundamcity_candidate_links(soup, seed)
            print(f"  후보 링크 {len(links)}개")
            all_links.update(links)
            time.sleep(REQUEST_SLEEP)
        except Exception as e:
            print(f"[건담시티 목록 실패] {seed} / {type(e).__name__}: {e}")

    print(f"[건담시티] 상세 후보 총 {len(all_links)}개")

    results: List[ItemRecord] = []
    seen_ids: Set[str] = set()

    for idx, link in enumerate(sorted(all_links), start=1):
        try:
            item = parse_gundamcity_detail(session, link)
            if item and item.item_id not in seen_ids:
                results.append(item)
                seen_ids.add(item.item_id)
        except Exception as e:
            print(f"[건담시티 상세 루프 실패] {link} / {type(e).__name__}: {e}")

        if DEBUG and idx % 10 == 0:
            print(f"[건담시티] 상세 진행 {idx}/{len(all_links)}")

        time.sleep(REQUEST_SLEEP)

    print(f"[건담시티] 최종 수집 {len(results)}개")
    return results

def crawl_gundamshop(session: requests.Session) -> List[ItemRecord]:
    all_links: Set[str] = set()

    for seed in GUNDAMSHOP_SEEDS:
        try:
            print(f"[건담샵 목록] {seed}")
            soup = soup_from_url(session, seed)
            links = extract_gundamshop_candidate_links(soup, seed)
            print(f"  후보 링크 {len(links)}개")
            all_links.update(links)
            time.sleep(REQUEST_SLEEP)
        except Exception as e:
            print(f"[건담샵 목록 실패] {seed} / {type(e).__name__}: {e}")

    print(f"[건담샵] 상세 후보 총 {len(all_links)}개")

    results: List[ItemRecord] = []
    seen_ids: Set[str] = set()

    for idx, link in enumerate(sorted(all_links), start=1):
        try:
            item = parse_gundamshop_detail(session, link)
            if item and item.item_id not in seen_ids:
                results.append(item)
                seen_ids.add(item.item_id)
        except Exception as e:
            print(f"[건담샵 상세 루프 실패] {link} / {type(e).__name__}: {e}")

        if DEBUG and idx % 10 == 0:
            print(f"[건담샵] 상세 진행 {idx}/{len(all_links)}")

        time.sleep(REQUEST_SLEEP)

    print(f"[건담샵] 최종 수집 {len(results)}개")
    return results


def crawl_bnkrmall(session: requests.Session) -> List[ItemRecord]:
    results: List[ItemRecord] = []

    try:
        url = "https://m.bnkrmall.co.kr/mw/goods/list.do"

        params = {
            "cate": "1576",
            "page": "1",
            "pageSize": "100",
            "endGoods": "Y",
        }

        print("[BNKR API 호출 시작]")

        resp = session.get(url, params=params, timeout=TIMEOUT)
        data = resp.json()

        items = data.get("list", [])

        print(f"[BNKR API 결과] {len(items)}개")

        for item in items:
            name = clean_product_name(item.get("goodsNm", ""))
            price = f"{item.get('salePrice', 0):,}원"
            status = "판매중" if item.get("stockYn") == "Y" else "품절"

            if not name:
                continue

            if is_non_gundam_figure_like(name):
                continue

            if not looks_like_gundam(name):
                continue

            gno = item.get("goodsNo", "")
            url = f"https://m.bnkrmall.co.kr/mw/goods/detail.do?gno={gno}"

            item_id = f"bnkr_{sha_id(str(gno))}"

            results.append(
                ItemRecord(
                    item_id=item_id,
                    name=name,
                    title=name,
                    price=price,
                    status=status,
                    stock_text=status,
                    mall_name="반다이남코코리아몰",
                    site="bnkrmall",
                    source_page="kr_bnkrmall",
                    url=url,
                    product_url=url,
                    detail_url=url,
                )
            )

    except Exception as e:
        print(f"[BNKR API 실패] {type(e).__name__}: {e}")

    print(f"[BNKR 최종] {len(results)}개")
    return results


def status_score(status: str) -> int:
    if status == "판매중":
        return 4
    if status == "예약중":
        return 3
    if status == "입고예정":
        return 2
    if status == "품절":
        return 1
    return 0


def price_to_int(price: str) -> Optional[int]:
    digits = re.sub(r"[^0-9]", "", price or "")
    if not digits:
        return None
    try:
        return int(digits)
    except Exception:
        return None


def dedupe_records(records: List[ItemRecord]) -> List[ItemRecord]:
    by_key: Dict[str, ItemRecord] = {}

    for item in records:
        base_name = normalize_space(item.name).upper()
        base_name = re.sub(r"^\[[^\]]+\]\s*", "", base_name)
        base_name = re.sub(r"^\([^)]+\)\s*", "", base_name)
        base_name = re.sub(r"\s+", " ", base_name).strip()

        grade = "UNKNOWN"
        for g in ["MGEX", "MGSD", "PG", "MG", "RG", "HG", "SD", "RE/100"]:
            if g in base_name:
                grade = g
                break

        key = f"{grade}|{base_name}"
        old = by_key.get(key)

        if old is None:
            by_key[key] = item
            continue

        old_score = status_score(old.status)
        new_score = status_score(item.status)

        if new_score > old_score:
            by_key[key] = item
            continue

        if new_score == old_score:
            old_price = price_to_int(old.price)
            new_price = price_to_int(item.price)

            if old_price is None and new_price is not None:
                by_key[key] = item
            elif old_price is not None and new_price is not None and new_price < old_price:
                by_key[key] = item

    return list(by_key.values())


def sort_records(records: List[ItemRecord]) -> List[ItemRecord]:
    def key(item: ItemRecord):
        status_rank = {
            "판매중": 0,
            "예약중": 1,
            "입고예정": 2,
            "품절": 3,
            "상태 확인중": 4,
        }.get(item.status, 5)

        price = price_to_int(item.price)
        if price is None:
            price = 999999999

        return (status_rank, price, item.mall_name, item.name.lower())

    return sorted(records, key=key)


def to_firestore_doc(item: ItemRecord) -> Dict:
    return {
        "name": item.name,
        "title": item.title,
        "price": item.price,
        "status": item.status,
        "stockText": item.stock_text,
        "mallName": item.mall_name,
        "site": item.site,
        "sourcePage": item.source_page,
        "url": item.url,
        "productUrl": item.product_url,
        "detailUrl": item.detail_url,
        "updatedAt": firestore.SERVER_TIMESTAMP,
    }


def upload_to_firestore(db, items: List[ItemRecord]):
    col = db.collection(COLLECTION_NAME)

    existing_docs = col.stream()
    delete_count = 0
    batch = db.batch()
    batch_ops = 0

    for doc in existing_docs:
        batch.delete(doc.reference)
        batch_ops += 1
        delete_count += 1
        if batch_ops >= 400:
            batch.commit()
            batch = db.batch()
            batch_ops = 0

    if batch_ops > 0:
        batch.commit()

    print(f"[Firestore] 기존 문서 삭제 완료: {delete_count}개")

    batch = db.batch()
    batch_ops = 0
    write_count = 0

    for item in items:
        doc_ref = col.document(item.item_id)
        batch.set(doc_ref, to_firestore_doc(item))
        batch_ops += 1
        write_count += 1

        if batch_ops >= 400:
            batch.commit()
            batch = db.batch()
            batch_ops = 0

    if batch_ops > 0:
        batch.commit()

    print(f"[Firestore] 업로드 완료: {write_count}개")


def save_local_backup(items: List[ItemRecord], path: str = "kr_aggregated_debug.json"):
    payload = [asdict(x) for x in items]
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f"[로컬 백업 저장] {path} / {len(items)}개")


def main():
    print("=== KR 통합 크롤링 시작 ===")

    try:
        db = init_firestore()
        print("[초기화] Firestore 연결 성공")
    except Exception as e:
        print(f"[초기화 실패] {type(e).__name__}: {e}")
        raise

    session = make_session()
    print("[초기화] requests 세션 생성 성공")

    modelsale_items = crawl_modelsale(session)
    print(f"[모델세일 결과] {len(modelsale_items)}개")

    #gundambase_items = crawl_gundambase(session)
    #print(f"[건담베이스 결과] {len(gundambase_items)}개")

    hobbyfactory_items = crawl_hobbyfactory(session)
    print(f"[하비팩토리 결과] {len(hobbyfactory_items)}개")

    gundamcity_items = crawl_gundamcity(session)
    print(f"[건담시티 결과] {len(gundamcity_items)}개")

    gundamshop_items = crawl_gundamshop(session)
    print(f"[건담샵 결과] {len(gundamshop_items)}개")

    bnkr_items = crawl_bnkrmall_selenium()
    print(f"[BNKR 결과] {len(bnkr_items)}개")

    merged = (
        gundamshop_items
        + hobbyfactory_items
        + gundamcity_items
        + modelsale_items
        + bnkr_items
    )
    print(f"[병합 전] 총 {len(merged)}개")

    # 최저가 비교를 위해 판매처별 문서를 그대로 유지
    merged = sort_records(merged)
    print(f"[정렬 후] 총 {len(merged)}개")

    save_local_backup(merged)
    upload_to_firestore(db, merged)

    print("=== 완료 ===")


if __name__ == "__main__":
    main()