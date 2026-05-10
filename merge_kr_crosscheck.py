import json
import os
import re
import time
import hashlib
from datetime import datetime, timezone
from pathlib import Path
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
FAST_TEST_MODE = os.getenv("FAST_TEST_MODE", "0").strip().lower() in {"1", "true", "yes", "y"}
MAX_LINKS_PER_SITE = int(os.getenv("MAX_LINKS_PER_SITE", "9999"))
SERVICE_ACCOUNT_PATH = "serviceAccountKey.json"
COLLECTION_NAME = os.getenv("FIRESTORE_COLLECTION", "aggregated_items")

# ===== 크롤링 안전장치 / 건강검진 기준 =====
# 아래 기준보다 너무 적게 수집되면 Firestore 기존 데이터를 지우지 않고 업로드를 중단한다.
MIN_TOTAL_UPLOAD = int(os.getenv("MIN_TOTAL_UPLOAD", "250"))
MIN_EXISTING_RATIO = float(os.getenv("MIN_EXISTING_RATIO", "0.60"))
FORCE_UPLOAD_LOW_COUNT = os.getenv("FORCE_UPLOAD_LOW_COUNT", "0").strip().lower() in {"1", "true", "yes", "y"}

# 핵심 사이트: 이 사이트들이 크게 무너지면 데이터 품질에 직접 영향이 크다.
CRITICAL_SITE_MIN_COUNTS = {
    "건담샵": 120,
    "모델세일": 80,
    "하비팩토리": 120,
    "조이하비": 80,
}

# 보조 사이트: 0개 또는 급감 시 경고는 남기지만 전체 업로드를 반드시 막지는 않는다.
WARNING_SITE_MIN_COUNTS = {
    "건담시티": 5,
    "지온샵": 20,
    "프라모델매니아": 10,
    "반다이남코코리아몰": 10,
    "건담붐": 1,
    "건담몰": 1,
}

BAD_TITLE_EXACT = {
    "",
    "상품명",
    "상품상세",
    "상품상세 | 반다이남코코리아몰",
    "상품상세|반다이남코코리아몰",
    "반다이남코코리아몰",
    "bnkrmall",
    "건담시티",
    "gundamcity",
    "건담샵",
    "하비팩토리",
    "모델세일",
    "조이하비",
}

# ===== 운영 관측/실패 리포트 =====
FAILED_EVENTS: List[Dict[str, str]] = []


def record_failure(site: str, url: str, error: Exception | str):
    message = str(error)
    FAILED_EVENTS.append({
        "site": site,
        "url": url or "",
        "error": message[:500],
    })


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

def limit_links(all_links):
    links_to_crawl = sorted(all_links)

    if FAST_TEST_MODE:
        links_to_crawl = links_to_crawl[:MAX_LINKS_PER_SITE]

    return links_to_crawl

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
        "울트라맨",
        "ULTRAMAN",
        "스타워즈",
        "STAR WARS",
        "타이파이터",
        "TIE FIGHTER",
        "드래곤볼",
        "POKEMON",
        "포켓몬",
        "30MS",
        "30MM",
        "30MF",
        "옵션파츠",
        "OPTION PARTS",
        "데칼",
        "스티커",
        "마커",
        "도료",
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
            if title and not is_bad_title(title):
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
        "상품상세",
        "상품상세 | 반다이남코코리아몰",
    }

    # 1순위: 화면 본문 selector
    for selector in selectors:
        for el in soup.select(selector):
            text = normalize_space(el.get_text(" ", strip=True))
            if not text:
                continue
            lowered = text.strip().lower()
            if lowered in bad_titles:
                continue
            if "상품상세 |" in lowered or "반다이남코코리아몰" == lowered:
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


# 건담시티 파서에서 예전 이름으로 호출하던 함수 호환용
def parse_bnkr_title_from_soup(soup: BeautifulSoup) -> str:
    return parse_title_from_soup(soup)


def is_bad_title(title: str) -> bool:
    t = normalize_space(title).lower()
    if t in {x.lower() for x in BAD_TITLE_EXACT}:
        return True

    bad_contains = [
        "상품상세 |",
        "반다이남코코리아몰",
        "bnkrmall",
        "로그인",
        "회원가입",
        "장바구니",
    ]
    return any(k in t for k in bad_contains)


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

JOYHOBBY_SEEDS = [
    # 조이하비 건담/SF > 건담그레이드/작품별 카테고리
    "https://www.joyhobby.co.kr/mall/category.asp?catid=204&siteid=joyhobby",
    "https://www.joyhobby.co.kr/mall/category.asp?catid=827&siteid=joyhobby",   # MG
    "https://www.joyhobby.co.kr/mall/category.asp?catid=7618&siteid=joyhobby",  # RG 계열
    "https://www.joyhobby.co.kr/mall/category.asp?catid=2578&siteid=joyhobby",  # HG 계열
    "https://www.joyhobby.co.kr/mall/category.asp?catid=7523&siteid=joyhobby",  # 유니콘/NT 계열
    "https://www.joyhobby.co.kr/mall/category.asp?catid=2585&siteid=joyhobby",  # 역습의 샤아
    "https://www.joyhobby.co.kr/mall/category.asp?catid=9986&siteid=joyhobby",  # 건담/SF 루트
    "https://www.joyhobby.co.kr/mall/category.asp?catid=9493&siteid=joyhobby",  # 건담/SF 보조
]

GUNDAMBOOM_SEEDS = [
    "https://www.gundamboom.com/",
    "https://m.gundamboom.com/",
    "https://www.gundamboom.com/product/search.html?keyword=%EA%B1%B4%EB%8B%B4",
    "https://www.gundamboom.com/product/search.html?keyword=MG",
    "https://www.gundamboom.com/product/search.html?keyword=RG",
    "https://www.gundamboom.com/product/search.html?keyword=HG",
    "https://m.gundamboom.com/product/search.html?keyword=%EA%B1%B4%EB%8B%B4",
    "https://m.gundamboom.com/product/search.html?keyword=MG",
]

PLAMODELMANIA_SEEDS = [
    "https://plamodelmania.com/category/%EA%B1%B4%EB%8B%B4/42/",
    "https://plamodelmania.com/category/mg/98/",
    "https://plamodelmania.com/category/rg/99/",
    "https://plamodelmania.com/category/hg/100/",
    "https://plamodelmania.com/category/sdbb/104/",
]

ZEONSHOP_SEEDS = [
    "https://zeonshop.net/category/mg-1100/31/",
    "https://zeonshop.net/category/rg-1144/32/",
    "https://zeonshop.net/category/hg-1144/33/",
    "https://zeonshop.net/category/sd/34/",
    "https://zeonshop.net/product/search.html?keyword=%EA%B1%B4%EB%8B%B4",
]

GUNDAMALL_SEEDS = [
    "https://www.gundamall.com/",
    "https://m.gundamall.com/",
    "https://www.gundamall.com/product/search.html?keyword=%EA%B1%B4%EB%8B%B4",
    "https://www.gundamall.com/product/search.html?keyword=MG",
    "https://www.gundamall.com/product/search.html?keyword=RG",
    "https://www.gundamall.com/product/search.html?keyword=HG",
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


def extract_joyhobby_candidate_links(soup: BeautifulSoup, base_url: str) -> Set[str]:
    """조이하비 상품 상세 링크만 추출한다.

    실제 조이하비 상품 상세는 주로 아래 형태다.
    - /m/item.asp?productcode=BD5067231&siteid=joyhobby
    - /m/item.asp?catid=7523&itemid=541638
    - /m/item.asp?itemid=610339

    기존처럼 /mall/buying.asp 만 찾으면 결제/잡링크가 섞이거나 상품을 놓칠 수 있다.
    """
    links: Set[str] = set()

    exclude_patterns = [
        "/event/",
        "/member/",
        "/customer/",
        "/board",
        "/notice",
        "/cart",
        "/order",
        "/login",
        "/join",
        "/mypage",
        "/basket",
        "isp.htm",
        "inicis",
        "kcp",
        "javascript:",
        "mailto:",
    ]

    def normalize_joy_url(raw: str) -> Optional[str]:
        raw = (raw or "").strip()
        if not raw:
            return None

        # onclick="...item.asp?itemid=610339..." 같은 케이스 대응
        m = re.search(r"(?:https?://www\.joyhobby\.co\.kr)?(/m/item\.asp\?[^'\"\s<>]+)", raw, re.IGNORECASE)
        if m:
            raw = m.group(1)

        full = urljoin(base_url, raw)
        lower = full.lower()

        if "joyhobby.co.kr" not in lower:
            return None
        if any(x in lower for x in exclude_patterns):
            return None

        # 상품 상세 핵심 패턴
        if "/m/item.asp" in lower and any(k in lower for k in ["itemid=", "productcode="]):
            return full

        # 혹시 PC 상세 주소가 따로 섞이는 경우까지 대비
        if "/mall/item.asp" in lower and any(k in lower for k in ["itemid=", "productcode="]):
            return full

        return None

    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        full = normalize_joy_url(href)
        if full:
            links.add(full)

        onclick = (a.get("onclick") or "").strip()
        full = normalize_joy_url(onclick)
        if full:
            links.add(full)

    # HTML 내부에 직접 박힌 상품 링크도 스캔
    html = str(soup)
    for raw in re.findall(r"(?:https?://www\.joyhobby\.co\.kr)?/m/item\.asp\?[^'\"\s<>]+", html, re.IGNORECASE):
        full = normalize_joy_url(raw)
        if full:
            links.add(full)

    print(f"[조이하비 링크 디버그] 상품 후보 {len(links)}개")
    if DEBUG:
        for link in sorted(list(links))[:30]:
            print(f"  [조이하비 후보] {link}")

    return links


def extract_cafe24_candidate_links(soup: BeautifulSoup, base_url: str, domain: str, label: str) -> Set[str]:
    """
    Cafe24 계열 쇼핑몰에서 상품 상세 링크를 넓게 수집한다.
    - gundamboom: /product/detail.html?product_no=..., /product/detail.php?product_no=...
    - plamodelmania/zeonshop/gundamall: /product/.../상품번호/category/...
    """
    links: Set[str] = set()

    exclude_patterns = [
        '/board/', '/member/', '/order/', '/cart/', '/myshop/', '/exec/front/',
        '/article/', '/notice/', '/faq/', '/event/', '/shopinfo/', '/coupon/',
        'javascript:', 'mailto:', 'basket.html', 'login', 'review', 'qna.php',
        'popup', 'recommend', 'wishlist', 'add_basket', 'category/', 'search.html?'
    ]

    detail_patterns = [
        '/product/detail.html', '/product/detail.php', '/product/',
        'product_no=', 'itemno=', 'goodsno=', 'goods_no=', 'productcode=',
    ]

    def try_add(raw: str):
        raw = (raw or '').strip()
        if not raw or raw.startswith('#'):
            return
        full = urljoin(base_url, raw)
        lower = full.lower()
        if domain not in lower:
            return
        if any(x in lower for x in exclude_patterns):
            return
        if any(x in lower for x in detail_patterns):
            # Cafe24 /product/상품명/번호/category/번호/ 형태는 category 목록과 구분하기 위해 숫자 포함 확인
            if '/product/' in lower and not re.search(r'(product_no=|itemno=|goodsno=|/\d+(?:/|$|\?))', lower):
                return
            links.add(full)

    for a in soup.select('a[href]'):
        try_add(a.get('href') or '')

    html = str(soup)
    for raw in re.findall(r"(?:href|data-url|ec-data-href)=['\"]([^'\"]+)['\"]", html, re.IGNORECASE):
        try_add(raw)
    for raw in re.findall(r"(?:location\.href|window\.open)\(['\"]([^'\"]+)['\"]", html, re.IGNORECASE):
        try_add(raw)

    print(f"[{label} 링크 디버그] 상품 후보 {len(links)}개")
    if DEBUG:
        for link in sorted(list(links))[:30]:
            print(f"  [{label} 후보] {link}")

    return links


def parse_generic_shop_title(soup: BeautifulSoup, page_text: str, mall_label: str) -> str:
    candidates: List[str] = []

    selectors = [
        'meta[property="og:title"]', 'meta[name="title"]', 'meta[name="twitter:title"]',
        '.headingArea h2', '.headingArea h1', '.xans-product-detail .headingArea h2',
        '.infoArea h2', '.infoArea h3', '.prdInfo h3', '.product_name', '.name',
        '.goods_name', '.item_name', 'h1', 'h2', 'h3',
    ]

    for selector in selectors:
        for el in soup.select(selector):
            text = ''
            if el.name == 'meta':
                text = el.get('content') or ''
            else:
                text = el.get_text(' ', strip=True)
            text = normalize_space(text)
            if text:
                candidates.append(text)

    patterns = [
        r"상품명\s*[:：]?\s*([^\n\r]{2,180})",
        r"((?:\[[^\]]*(?:MGEX|MGSD|PG|MG|RG|HGUC|HG|SD|EG|RE/100|FULL MECHANICS)[^\]]*\]|(?:MGEX|MGSD|PG|MG|RG|HGUC|HG|SD|EG|RE/100|FULL MECHANICS))[^\n\r]{2,180})",
        r"((?:\d{6,}|BAN\d{4,}|BD\d{4,})\s+[^\n\r]{2,160}?(?:건담|GUNDAM|자쿠|ZAKU|사자비|SAZABI|유니콘|UNICORN|프리덤|FREEDOM|시난주|SINANJU)[^\n\r]{0,80})",
    ]
    for pat in patterns:
        m = re.search(pat, page_text, re.IGNORECASE)
        if m:
            candidates.append(m.group(1))

    bad = {
        mall_label.lower(), '상품상세', '상품명', '상세정보', '로그인', '장바구니',
        'gundam', '건담', '프라모델', '쇼핑몰',
    }

    for c in candidates:
        c = normalize_space(c)
        c = re.sub(r"\s*[-|:]\s*" + re.escape(mall_label) + r"\s*$", "", c, flags=re.IGNORECASE)
        c = re.sub(r"\s*(판매가|소비자가|적립금|제조사|배송비)\s*[:：]?.*$", "", c).strip()
        if not c or c.strip().lower() in bad:
            continue
        if len(c) < 2 or len(c) > 220:
            continue
        # 상품명일 가능성이 높은 것 우선
        if looks_like_gundam(c) or re.search(r'(MGEX|MGSD|PG|MG|RG|HGUC|HG|SD|EG|RE/100|FULL MECHANICS)', c, re.I):
            return c

    return ''


def parse_generic_shop_detail(
    session: requests.Session,
    url: str,
    *,
    site: str,
    mall_name: str,
    source_page: str,
    id_prefix: str,
    encoding: Optional[str] = None,
) -> Optional[ItemRecord]:
    try:
        soup = soup_from_url(session, url, encoding=encoding)
        page_text = soup.get_text(' ', strip=True) if soup else ''

        raw_title = parse_generic_shop_title(soup, page_text, mall_name)
        title_only = clean_product_name(raw_title)
        joined = f"{title_only} {page_text}".strip()

        if not title_only:
            if DEBUG:
                print(f"[{mall_name} 탈락:title없음] {url}")
                print(f"  TEXT_HEAD: {page_text[:300]}")
            return None

        extra_exclude = [
            '데칼', '습식데칼', '스티커', '마커', '도료', '스프레이', '서페이서', '신너',
            '니퍼', '공구', '툴', '베이스', '스탠드', '옵션파츠', 'OPTION PARTS',
            'DISPLAY BASE', '피규어라이즈', 'FIGURE-RISE', 'FIGURERISE', '프라모델매니아',
            '건담마커', '건담 스프레이', '패널라인', '먹선', '도색', '붓', '접착제',
        ]
        upper_title = title_only.upper()
        if any(k.upper() in upper_title for k in extra_exclude):
            if DEBUG:
                print(f"[{mall_name} 탈락:소모품/옵션] {title_only} / {url}")
            return None

        if is_excluded(title_only):
            return None
        if is_non_gundam_figure_like(title_only):
            return None
        if not looks_like_gundam(joined):
            if DEBUG:
                print(f"[{mall_name} 탈락:건담판별실패] {title_only} / {url}")
            return None

        price = parse_price_from_soup(soup) or ''
        status = parse_status_from_soup(soup) or '상태 확인중'
        stock_text = status

        parsed_price = price_to_int(price)
        if parsed_price is not None and parsed_price < 3000:
            if DEBUG:
                print(f"[{mall_name} 탈락:비정상가격] {title_only} / {price} / {url}")
            return None

        parsed = urlparse(url)
        qs = parse_qs(parsed.query)
        stable_key = (
            qs.get('product_no', [''])[0]
            or qs.get('itemno', [''])[0]
            or qs.get('goodsno', [''])[0]
            or qs.get('goods_no', [''])[0]
            or qs.get('productcode', [''])[0]
        )
        if not stable_key:
            m = re.search(r'/product/[^/]+/(\d+)', parsed.path)
            if m:
                stable_key = m.group(1)
        stable_key = stable_key or url
        item_id = f"{id_prefix}_{sha_id(stable_key)}"

        clean_title = clean_product_name(title_only)
        if not clean_title:
            return None

        if DEBUG:
            print(f"[{mall_name} 통과] {clean_title} / {price} / {status}")

        return ItemRecord(
            item_id=item_id,
            name=clean_title,
            title=clean_title,
            price=price,
            status=status,
            stock_text=stock_text,
            mall_name=mall_name,
            site=site,
            source_page=source_page,
            url=url,
            product_url=url,
            detail_url=url,
        )
    except requests.exceptions.HTTPError as e:
        record_failure(mall_name, url, e)
        if DEBUG:
            print(f"[{mall_name} HTTP 탈락] {url} / {e}")
        return None
    except Exception as e:
        record_failure(mall_name, url, e)
        print(f"[{mall_name} 상세 파싱 실패] {url} / {type(e).__name__}: {e}")
        return None


def crawl_generic_shop(
    session: requests.Session,
    *,
    seeds: List[str],
    domain: str,
    label: str,
    site: str,
    source_page: str,
    id_prefix: str,
    encoding: Optional[str] = None,
) -> List[ItemRecord]:
    all_links: Set[str] = set()

    for seed in seeds:
        try:
            print(f"[{label} 목록] {seed}")
            soup = soup_from_url(session, seed, encoding=encoding)
            links = extract_cafe24_candidate_links(soup, seed, domain, label)
            print(f"  후보 링크 {len(links)}개")
            all_links.update(links)
            time.sleep(REQUEST_SLEEP)
        except Exception as e:
            print(f"[{label} 목록 실패] {seed} / {type(e).__name__}: {e}")

    links_to_crawl = sorted(all_links)

    if FAST_TEST_MODE:
        links_to_crawl = links_to_crawl[:MAX_LINKS_PER_SITE]

    print(f"[{label}] 상세 후보 총 {len(links_to_crawl)}개")

    results: List[ItemRecord] = []
    seen_ids: Set[str] = set()

    for idx, link in enumerate(links_to_crawl, start=1):
        try:
            item = parse_generic_shop_detail(
                session,
                link,
                site=site,
                mall_name=label,
                source_page=source_page,
                id_prefix=id_prefix,
                encoding=encoding,
            )
            if item and item.item_id not in seen_ids:
                results.append(item)
                seen_ids.add(item.item_id)
        except Exception as e:
            print(f"[{label} 상세 루프 실패] {link} / {type(e).__name__}: {e}")

        if idx % 10 == 0:
            print(f"[{label}] 상세 진행 {idx}/{len(links_to_crawl)}")

        time.sleep(REQUEST_SLEEP)

    print(f"[{label}] 최종 수집 {len(results)}개")
    return results


def crawl_gundamboom(session: requests.Session) -> List[ItemRecord]:
    return crawl_generic_shop(
        session,
        seeds=GUNDAMBOOM_SEEDS,
        domain='gundamboom.com',
        label='건담붐',
        site='gundamboom',
        source_page='kr_gundamboom',
        id_prefix='gundamboom',
    )


def crawl_plamodelmania(session: requests.Session) -> List[ItemRecord]:
    return crawl_generic_shop(
        session,
        seeds=PLAMODELMANIA_SEEDS,
        domain='plamodelmania.com',
        label='프라모델매니아',
        site='plamodelmania',
        source_page='kr_plamodelmania',
        id_prefix='plamodelmania',
    )


def crawl_zeonshop(session: requests.Session) -> List[ItemRecord]:
    return crawl_generic_shop(
        session,
        seeds=ZEONSHOP_SEEDS,
        domain='zeonshop.net',
        label='지온샵',
        site='zeonshop',
        source_page='kr_zeonshop',
        id_prefix='zeonshop',
    )


def crawl_gundamall(session: requests.Session) -> List[ItemRecord]:
    return crawl_generic_shop(
        session,
        seeds=GUNDAMALL_SEEDS,
        domain='gundamall.com',
        label='건담몰',
        site='gundamall',
        source_page='kr_gundamall',
        id_prefix='gundamall',
    )


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
        record_failure("모델세일", url, e)
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
        record_failure("건담베이스", url, e)
        print(f"[건담베이스 상세 파싱 실패] {url} / {type(e).__name__}: {e}")
        return None

def parse_hobbyfactory_detail(session: requests.Session, url: str) -> Optional[ItemRecord]:
    try:
        soup = soup_from_url(session, url)
        page_text = soup.get_text(" ", strip=True)
        title = parse_title_from_soup(soup)
        title_only = clean_product_name(title)
        joined = f"{title_only} {page_text}"
        if DEBUG:
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
        record_failure("하비팩토리", url, e)
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
        record_failure("건담시티", url, e)
        print(f"[건담시티 상세 파싱 실패] {url} / {type(e).__name__}: {e}")
        return None

def parse_joyhobby_title(soup: BeautifulSoup, page_text: str) -> str:
    """조이하비 상세 페이지에서 상품명을 최대한 안정적으로 뽑는다."""
    title = parse_title_from_soup(soup)
    title = normalize_space(title)

    bad_titles = {
        "조이하비",
        "joyhobby",
        "상품명",
        "상품상세",
    }

    if title and title.strip().lower() not in bad_titles:
        # 브라우저 타이틀에 쇼핑몰명이 붙는 경우 제거
        title = re.sub(r"\s*[-|:]\s*조이하비\s*$", "", title, flags=re.IGNORECASE).strip()
        if title and title.strip().lower() not in bad_titles:
            return title

    # 검색 색인/페이지 본문에서 보이는 조이하비 상품명 패턴
    patterns = [
        r"((?:\[[^\]]*(?:MGEX|MGSD|PG|MG|RG|HGUC|HG|SD|EG|RE/100|FULL MECHANICS)[^\]]*\]|(?:MGEX|MGSD|PG|MG|RG|HGUC|HG|SD|EG|RE/100|FULL MECHANICS))[^\n\r]{2,160}?(?:건담|GUNDAM|자쿠|ZAKU|사자비|SAZABI|유니콘|UNICORN|프리덤|FREEDOM|스트라이크|STRIKE|시난주|SINANJU|뉴건담|NU GUNDAM|더블오|엑시아|발바토스|바르바토스|에어리얼|AERIAL|즈고크|돔|구프|백식|제타|ZETA)[^\n\r]{0,120})",
        r"((?:BD|MG|RG|HG|PG|SD)[A-Z0-9\-]{3,}\s+[^\n\r]{2,160}?(?:건담|GUNDAM|자쿠|ZAKU|사자비|SAZABI|유니콘|UNICORN)[^\n\r]{0,80})",
    ]

    for pat in patterns:
        m = re.search(pat, page_text, re.IGNORECASE)
        if m:
            candidate = normalize_space(m.group(1))
            candidate = re.sub(r"\s*(판매가|판매가격|적립금|제조사)\s*[:：]?.*$", "", candidate).strip()
            if candidate:
                return candidate

    return ""


def parse_joyhobby_detail(session: requests.Session, url: str) -> Optional[ItemRecord]:
    try:
        soup = soup_from_url(session, url, encoding="cp949")
        page_text = soup.get_text(" ", strip=True) if soup else ""

        raw_title = parse_joyhobby_title(soup, page_text)
        title_only = clean_product_name(raw_title)
        joined = f"{title_only} {page_text}".strip()

        if not title_only:
            if DEBUG:
                print(f"[조이하비 탈락:title없음] {url}")
                print(f"  TEXT_HEAD: {page_text[:300]}")
            return None

        invalid_titles = {
            "조이하비",
            "joyhobby",
            "상품명",
            "상품상세",
        }
        if title_only.strip().lower() in invalid_titles:
            return None

        # 조이하비는 데칼/공구/옵션파츠에도 건담명이 들어가는 경우가 있어서 별도 차단
        joy_exclude_keywords = [
            "데칼",
            "습식데칼",
            "스티커",
            "마커",
            "도료",
            "스프레이",
            "서페이서",
            "신너",
            "니퍼",
            "공구",
            "툴",
            "베이스",
            "스탠드",
            "옵션파츠",
            "OPTION PARTS",
            "DISPLAY BASE",
            "피규어라이즈",
            "FIGURE-RISE",
            "FIGURERISE",
        ]
        upper_title = title_only.upper()
        if any(k.upper() in upper_title for k in joy_exclude_keywords):
            if DEBUG:
                print(f"[조이하비 탈락:옵션/공구/피규어라이즈] {title_only} / {url}")
            return None

        if is_excluded(title_only):
            return None

        if is_non_gundam_figure_like(title_only):
            return None

        # 조이하비는 상품명이 코드/영문 위주로 잡힐 수 있어서 looks_like_gundam 기준으로 완화
        if not looks_like_gundam(joined):
            if DEBUG:
                print(f"[조이하비 탈락:건담판별실패] {title_only} / {url}")
            return None

        price = parse_price_from_soup(soup) or ""
        status = parse_status_from_soup(soup) or "상태 확인중"
        stock_text = status

        parsed_price = price_to_int(price)
        if parsed_price is not None and parsed_price < 3000:
            if DEBUG:
                print(f"[조이하비 탈락:비정상가격] {title_only} / {price} / {url}")
            return None

        parsed = urlparse(url)
        qs = parse_qs(parsed.query)
        product_code = qs.get("productcode", [""])[0]
        item_id_raw = qs.get("itemid", [""])[0]
        stable_key = product_code or item_id_raw or url
        item_id = f"joyhobby_{sha_id(stable_key)}"

        clean_title = clean_product_name(title_only)
        if not clean_title:
            return None

        if DEBUG:
            print(f"[조이하비 통과] {clean_title} / {price} / {status}")

        return ItemRecord(
            item_id=item_id,
            name=clean_title,
            title=clean_title,
            price=price,
            status=status,
            stock_text=stock_text,
            mall_name="조이하비",
            site="joyhobby",
            source_page="kr_joyhobby",
            url=url,
            product_url=url,
            detail_url=url,
        )
    except requests.exceptions.HTTPError as e:
        record_failure("조이하비", url, e)
        if DEBUG:
            print(f"[조이하비 HTTP 탈락] {url} / {e}")
        return None
    except Exception as e:
        record_failure("조이하비", url, e)
        print(f"[조이하비 상세 파싱 실패] {url} / {type(e).__name__}: {e}")
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
        record_failure("건담샵", url, e)
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
        record_failure("반다이남코코리아몰", url, e)
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

    links_to_crawl = limit_links(all_links)
    print(f"[건담베이스] 상세 후보 총 {len(links_to_crawl)}개")

    results: List[ItemRecord] = []
    seen_ids: Set[str] = set()

    for idx, link in enumerate(links_to_crawl, start=1):
        try:
            item = parse_gundambase_detail(session, link)
            if item and item.item_id not in seen_ids:
                results.append(item)
                seen_ids.add(item.item_id)
        except Exception as e:
            print(f"[건담베이스 상세 루프 실패] {link} / {type(e).__name__}: {e}")

        if idx % 10 == 0:
            print(f"[건담베이스] 상세 진행 {idx}/{len(links_to_crawl)}")

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

    links_to_crawl = limit_links(all_links)
    print(f"[모델세일] 상세 후보 총 {len(links_to_crawl)}개")
    results: List[ItemRecord] = []
    seen_ids: Set[str] = set()

    for idx, link in enumerate(links_to_crawl, start=1):
        try:
            item = parse_modelsale_detail(session, link)
            if item and item.item_id not in seen_ids:
                results.append(item)
                seen_ids.add(item.item_id)
        except Exception as e:
            print(f"[모델세일 상세 루프 실패] {link} / {type(e).__name__}: {e}")

        if idx % 10 == 0:
            print(f"[모델세일] 상세 진행 {idx}/{len(links_to_crawl)}")

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

    links_to_crawl = limit_links(all_links)
    results: List[ItemRecord] = []
    seen_ids: Set[str] = set()

    for idx, link in enumerate(links_to_crawl, start=1):
        try:
            item = parse_hobbyfactory_detail(session, link)
            if item and item.item_id not in seen_ids:
                results.append(item)
                seen_ids.add(item.item_id)
        except Exception as e:
            print(f"[하비팩토리 상세 루프 실패] {link} / {type(e).__name__}: {e}")

        if DEBUG and idx % 10 == 0:
            print(f"[하비팩토리] 상세 진행 {idx}/{len(links_to_crawl)}")

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

    links_to_crawl = limit_links(all_links)
    print(f"[건담시티] 상세 후보 총 {len(links_to_crawl)}개")
    results: List[ItemRecord] = []
    seen_ids: Set[str] = set()

    for idx, link in enumerate(links_to_crawl, start=1):
        try:
            item = parse_gundamcity_detail(session, link)
            if item and item.item_id not in seen_ids:
                results.append(item)
                seen_ids.add(item.item_id)
        except Exception as e:
            print(f"[건담시티 상세 루프 실패] {link} / {type(e).__name__}: {e}")

        if DEBUG and idx % 10 == 0:
            print(f"[건담시티] 상세 진행 {idx}/{len(links_to_crawl)}")

        time.sleep(REQUEST_SLEEP)

    print(f"[건담시티] 최종 수집 {len(results)}개")
    return results

def crawl_joyhobby(session: requests.Session) -> List[ItemRecord]:
    all_links: Set[str] = set()

    for seed in JOYHOBBY_SEEDS:
        try:
            print(f"[조이하비 목록] {seed}")
            soup = soup_from_url(session, seed, encoding="cp949")
            links = extract_joyhobby_candidate_links(soup, seed)
            print(f"  후보 링크 {len(links)}개")
            all_links.update(links)
            time.sleep(REQUEST_SLEEP)
        except Exception as e:
            print(f"[조이하비 목록 실패] {seed} / {type(e).__name__}: {e}")

    links_to_crawl = limit_links(all_links)
    print(f"[조이하비] 상세 후보 총 {len(links_to_crawl)}개")

    results: List[ItemRecord] = []
    seen_ids: Set[str] = set()

    for idx, link in enumerate(links_to_crawl, start=1):
        try:
            item = parse_joyhobby_detail(session, link)
            if item and item.item_id not in seen_ids:
                results.append(item)
                seen_ids.add(item.item_id)
        except Exception as e:
            print(f"[조이하비 상세 루프 실패] {link} / {type(e).__name__}: {e}")

        if idx % 10 == 0:
            print(f"[조이하비] 상세 진행 {idx}/{len(links_to_crawl)}")

        time.sleep(REQUEST_SLEEP)

    print(f"[조이하비] 최종 수집 {len(results)}개")
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

    links_to_crawl = limit_links(all_links)
    print(f"[건담샵] 상세 후보 총 {len(links_to_crawl)}개")
    results: List[ItemRecord] = []
    seen_ids: Set[str] = set()

    for idx, link in enumerate(links_to_crawl, start=1):
        try:
            item = parse_gundamshop_detail(session, link)
            if item and item.item_id not in seen_ids:
                results.append(item)
                seen_ids.add(item.item_id)
        except Exception as e:
            print(f"[건담샵 상세 루프 실패] {link} / {type(e).__name__}: {e}")

        if DEBUG and idx % 10 == 0:
            print(f"[건담샵] 상세 진행 {idx}/{len(links_to_crawl)}")

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



def extract_grade(name: str) -> str:
    t = (name or "").upper()

    # 긴 등급을 반드시 먼저 검사한다. MGSD 안의 SD, MGEX 안의 MG가 잡히면 안 된다.
    grade_patterns = [
        ("MGEX", r"(?<![A-Z0-9])MG\s*EX(?![A-Z0-9])|\[\s*MG\s*EX\s*\]"),
        ("MGSD", r"(?<![A-Z0-9])MG\s*SD(?![A-Z0-9])|\[\s*MG\s*SD\s*\]"),
        ("FULL MECHANICS", r"FULL\s*MECHANICS|풀\s*메카닉스"),
        ("RE/100", r"RE\s*/\s*100"),
        ("PG", r"(?<![A-Z0-9])PG(?![A-Z0-9])|\[\s*PG\s*\]"),
        ("RG", r"(?<![A-Z0-9])RG(?:\s*\d+)?(?![A-Z0-9])|\[\s*RG[^\]]*\]"),
        ("MG", r"(?<![A-Z0-9])MG(?:\s*\d+)?(?![A-Z0-9])|\[\s*MG[^\]]*\]"),
        ("HG", r"(?<![A-Z0-9])HG(?:UC|CE|BF|AC|FC|GW|WM)?(?:\s*\d+)?(?![A-Z0-9])|\[\s*HG[^\]]*\]"),
        ("EG", r"(?<![A-Z0-9])EG(?![A-Z0-9])|\[\s*EG\s*\]"),
        ("SD", r"(?<!MG)(?<![A-Z0-9])SD(?:\s*[- ]?\s*EX(?:\s*[- ]?\s*STANDARD)?)?(?![A-Z0-9])|\[\s*SD[^\]]*\]|SD삼국|SD건담|SD건담월드"),
    ]

    for grade, pattern in grade_patterns:
        if re.search(pattern, t, re.IGNORECASE):
            return grade

    return "UNKNOWN"


CANONICAL_ALIAS_RULES = [
    (r"AERIAL\s*REBUILD|에어리얼\s*개수형", "건담 에어리얼 개수형"),
    (r"GUNDAM\s*AERIAL|건담\s*에어리얼|에어리얼", "건담 에어리얼"),
    (r"CALIBARN|캘리번", "건담 캘리번"),
    (r"SCHWARZETTE|슈바르제테", "건담 슈바르제테"),
    (r"LFRITH\s*UR|르브리스\s*울", "건담 르브리스 울"),
    (r"LFRITH\s*THORN|르브리스\s*손", "건담 르브리스 손"),
    (r"LFRITH|르브리스", "건담 르브리스"),
    (r"MIGHTY\s*STRIKE\s*FREEDOM|마이티\s*스트라이크\s*프리덤", "마이티 스트라이크 프리덤 건담"),
    (r"RISING\s*FREEDOM|라이징\s*프리덤", "라이징 프리덤 건담"),
    (r"IMMORTAL\s*JUSTICE|임모탈\s*저스티스", "임모탈 저스티스 건담"),
    (r"STRIKE\s*FREEDOM|스트라이크\s*프리덤", "스트라이크 프리덤 건담"),
    (r"FREEDOM\s*GUNDAM|프리덤\s*건담", "프리덤 건담"),
    (r"AILE\s*STRIKE|에일\s*스트라이크", "에일 스트라이크 건담"),
    (r"BUILD\s*STRIKE|빌드\s*스트라이크", "빌드 스트라이크 건담"),
    (r"STRIKE\s*GUNDAM|스트라이크\s*건담", "스트라이크 건담"),
    (r"NU\s*GUNDAM|ν\s*GUNDAM|뉴\s*건담", "뉴 건담"),
    (r"HI[-\s]*NU|하이\s*뉴|HI\s*ν", "하이 뉴 건담"),
    (r"SAZABI|사자비", "사자비"),
    (r"SINANJU\s*STEIN|시난주\s*스타인", "시난주 스타인"),
    (r"SINANJU|시난주", "시난주"),
    (r"UNICORN.*BANSHEE\s*NORN|밴시\s*노른|밴시노른", "유니콘 건담 2호기 밴시 노른"),
    (r"UNICORN.*DESTROY|유니콘.*디스트로이", "유니콘 건담 디스트로이 모드"),
    (r"UNICORN\s*GUNDAM|유니콘\s*건담", "유니콘 건담"),
    (r"BARBATOS\s*LUPUS\s*REX|발바토스\s*루프스\s*렉스", "건담 발바토스 루프스 렉스"),
    (r"BARBATOS\s*LUPUS|발바토스\s*루프스", "건담 발바토스 루프스"),
    (r"BARBATOS|발바토스", "건담 발바토스"),
    (r"EXIA\s*REPAIR\s*II|엑시아\s*리페어", "건담 엑시아 리페어 II"),
    (r"EXIA|엑시아", "건담 엑시아"),
    (r"RX[-\s]*78[-\s]*2|퍼스트\s*건담", "퍼스트 건담"),
    (r"ZETA\s*GUNDAM|제타\s*건담", "제타 건담"),
    (r"ZZ\s*GUNDAM|더블제타", "ZZ 건담"),
    (r"GOD\s*GUNDAM|갓\s*건담", "갓 건담"),
    (r"WING\s*GUNDAM\s*ZERO|윙\s*건담\s*제로", "윙 건담 제로"),
    (r"WING\s*GUNDAM|윙\s*건담", "윙 건담"),
    (r"DEATHSCYTHE|데스사이즈", "건담 데스사이즈"),
    (r"HEAVY\s*ARMS|헤비암즈", "건담 헤비암즈"),
    (r"GUNTANK|건탱크", "건탱크"),
    (r"GUNCANNON|건캐논", "건캐논"),
    (r"CHAR.*ZAKU|샤아.*자쿠", "샤아 전용 자쿠 II"),
    (r"ZAKU\s*II|자쿠\s*2|자쿠\s*II", "자쿠 II"),
]


def _normalize_alias_source(text: str) -> str:
    t = normalize_space(text)
    t = t.replace("ν", "NU")
    t = re.sub(r"VER\s*\.?\s*KA|버카|브이카", "Ver.Ka", t, flags=re.IGNORECASE)
    t = re.sub(r"\bRX[-\s]*O\b", "RX-0", t, flags=re.IGNORECASE)
    t = re.sub(r"\bO2\b", "02", t, flags=re.IGNORECASE)
    return t


def canonical_core_name(name: str) -> str:
    source = _normalize_alias_source(name)
    probe = source.upper()

    for pattern, replacement in CANONICAL_ALIAS_RULES:
        if re.search(pattern, probe, flags=re.IGNORECASE):
            core = replacement
            break
    else:
        core = source

    # 등급, 스케일, 상품번호, 쇼핑몰 홍보 문구 제거
    core = re.sub(r"\[[^\]]*\]", " ", core)
    core = re.sub(r"\([^)]*\)", " ", core)
    core = re.sub(r"\b1\s*/\s*(60|72|100|144|220|400|550)\b", " ", core)
    core = re.sub(r"\b(BAN|BD)\s*\d{4,}\b", " ", core, flags=re.IGNORECASE)
    core = re.sub(r"\b\d{6,}\b", " ", core)
    core = re.sub(r"\[[0-9]{1,4}\]", " ", core)
    core = re.sub(r"\b(MGEX|MGSD|FULL\s*MECHANICS|RE\s*/\s*100|HGUC|HGCE|HGBF|HGAC|HGFC|HGGW|HGWM|PG|MG|RG|HG|EG|SD)\b", " ", core, flags=re.IGNORECASE)

    remove_words = [
        "재입고", "예약판매", "예약", "입고예정", "판매중", "품절", "일시품절", "강력추천", "MD추천", "추천",
        "한정판", "한정", "프라모델", "건프라", "기동전사", "수성의 마녀", "수성의마녀", "섬광의 하사웨이",
        "GUNDAM", "건담 건담",
    ]
    for word in remove_words:
        core = re.sub(re.escape(word), " ", core, flags=re.IGNORECASE)

    # 대표명에 이미 건담이 포함된 경우는 살리고, 중복만 줄인다.
    core = normalize_space(core)
    core = re.sub(r"건담\s+건담", "건담", core)
    core = re.sub(r"\s+", " ", core).strip(" -_/[]()")
    return normalize_space(core)


def standardize_product_name(name: str) -> str:
    original = clean_product_name(name)
    if not original:
        return ""
    grade = extract_grade(original)
    core = canonical_core_name(original)

    if not core:
        return ""

    # Ver.Ka 표기는 대표명 뒤에 통일해서 붙인다.
    if re.search(r"VER\s*\.?\s*KA|버카|브이카", original, re.IGNORECASE) and "Ver.Ka" not in core:
        core = f"{core} Ver.Ka"

    # 너무 일반적인 결과는 원본 정리값을 사용하되 prefix는 붙인다.
    if core.upper() in {"GUNDAM", "건담", "MODEL", "KIT"}:
        core = clean_product_name(original)

    prefix = f"[{grade}] " if grade != "UNKNOWN" else ""
    return normalize_space(f"{prefix}{core}")


def normalize_product_key(name: str) -> str:
    """동일 상품 묶기/최저가 비교용 내부 키. displayName과 분리해서 더 공격적으로 정규화한다."""
    grade = extract_grade(name)
    core = canonical_core_name(name)
    if re.search(r"VER\s*\.?\s*KA|버카|브이카", name or "", re.IGNORECASE) and "Ver.Ka" not in core:
        core = f"{core} Ver.Ka"

    key = core.upper()
    key = key.replace("VER.KA", "VERKA")
    key = re.sub(r"[^0-9A-Z가-힣]+", " ", key)
    key = normalize_space(key)

    # 혼동 방지: 프리덤/스트라이크 프리덤/마이티 스트라이크 프리덤은 core가 다르게 남아야 한다.
    if not key:
        return f"{grade}|"
    return f"{grade}|{key}"

def is_bad_record(item: ItemRecord) -> bool:
    name = normalize_space(item.name or item.title)
    joined = f"{item.name} {item.title} {item.stock_text} {item.url}"

    if is_bad_title(name):
        return True

    # 가격이 너무 낮으면 굿즈/옵션/부품일 가능성이 높음
    price = price_to_int(item.price)
    if price is not None and price < 3000:
        return True

    if is_excluded(name):
        return True

    if is_non_gundam_figure_like(name):
        return True

    if not is_valid_gundam_plamodel(name, joined):
        return True

    key = normalize_product_key(name)
    if key.endswith("|"):
        return True

    return False


def filter_bad_records(records: List[ItemRecord]) -> List[ItemRecord]:
    clean: List[ItemRecord] = []
    removed = 0

    for item in records:
        if is_bad_record(item):
            removed += 1
            if DEBUG:
                print(f"[필터 제거] {item.mall_name} / {item.name} / {item.price} / {item.url}")
            continue

        # 상태값 다시 한 번 통일
        item.status = normalize_status(item.status or item.stock_text)
        item.stock_text = item.status

        # 앱 표시용 이름과 내부 비교용 이름을 통일한다.
        display_name = standardize_product_name(item.name or item.title)
        if not display_name or is_bad_title(display_name):
            removed += 1
            if DEBUG:
                print(f"[필터 제거:표시명 실패] {item.mall_name} / {item.name} / {item.url}")
            continue

        item.name = display_name
        item.title = display_name

        clean.append(item)

    print(f"[품질 필터] 제거 {removed}개 / 유지 {len(clean)}개")
    return clean


def dedupe_records(records: List[ItemRecord]) -> List[ItemRecord]:
    """
    안전한 중복 제거.
    같은 판매처 안에서 item_id 또는 상세 URL이 같은 경우만 중복으로 본다.
    productKey만으로 합치면 조이하비/SD/HG 계열이 과하게 합쳐질 수 있어서 업로드 전 데이터 손실을 막는다.
    """
    by_key: Dict[str, ItemRecord] = {}

    for item in records:
        stable = item.item_id or item.detail_url or item.product_url or item.url
        key = f"{item.site}|{stable}"

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

    print(f"[중복 제거:안전모드] {len(records)}개 -> {len(by_key)}개")
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


def _doc_get_int(data: Optional[Dict], keys: List[str]) -> Optional[int]:
    if not data:
        return None
    for key in keys:
        value = data.get(key)
        if value is None:
            continue
        if isinstance(value, int):
            return value
        try:
            digits = re.sub(r"[^0-9]", "", str(value))
            if digits:
                return int(digits)
        except Exception:
            pass
    return None


def _doc_get_str(data: Optional[Dict], keys: List[str]) -> str:
    if not data:
        return ""
    for key in keys:
        value = data.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def _is_restock_transition(old_status: str, new_status: str) -> bool:
    old = normalize_status(old_status)
    new = normalize_status(new_status)
    return new == "판매중" and old in {"품절", "입고예정", "상태 확인중"}


def to_firestore_doc(item: ItemRecord, previous: Optional[Dict] = None) -> Dict:
    price_int = price_to_int(item.price)
    product_key = normalize_product_key(item.name)
    grade = extract_grade(item.name)

    old_status = _doc_get_str(previous, ["status", "stockText", "stock_text"])
    old_price_int = _doc_get_int(previous, ["priceInt", "price_int", "price"])

    is_new = previous is None
    is_restock = False if is_new else _is_restock_transition(old_status, item.status)
    is_price_drop = (
        old_price_int is not None
        and price_int is not None
        and price_int > 0
        and old_price_int > price_int
    )

    display_name = standardize_product_name(item.name or item.title) or item.name

    return {
        "name": display_name,
        "title": display_name,
        "displayName": display_name,
        "normalizedName": product_key,
        "rawName": item.name,
        "rawTitle": item.title,
        "price": item.price,
        "priceInt": price_int,
        "grade": grade,
        "productKey": product_key,
        "isNew": is_new,
        "isRestock": is_restock,
        "isRestocked": is_restock,
        "isPriceDrop": is_price_drop,
        "previousPriceInt": old_price_int,
        "previousStatus": old_status,
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

    if len(items) < MIN_TOTAL_UPLOAD and not FORCE_UPLOAD_LOW_COUNT:
        raise RuntimeError(
            f"[업로드 중단] 최종 수집 {len(items)}개가 MIN_TOTAL_UPLOAD={MIN_TOTAL_UPLOAD}보다 적습니다. "
            "기존 Firestore 데이터를 보호하기 위해 삭제/업로드를 중단합니다."
        )

    # 기존 문서를 먼저 읽어서 NEW/재입고/가격하락 여부를 계산하고, 급감 안전장치를 적용한다.
    previous_by_id: Dict[str, Dict] = {}
    previous_by_site_key: Dict[str, Dict] = {}
    existing_refs = []

    for doc in col.stream():
        data = doc.to_dict() or {}
        previous_by_id[doc.id] = data
        existing_refs.append(doc.reference)

        site = _doc_get_str(data, ["site", "source"])
        product_key = _doc_get_str(data, ["productKey", "normalizedName"])
        if site and product_key:
            previous_by_site_key[f"{site}|{product_key}"] = data

    existing_count = len(existing_refs)
    if (
        existing_count >= MIN_TOTAL_UPLOAD
        and len(items) < int(existing_count * MIN_EXISTING_RATIO)
        and not FORCE_UPLOAD_LOW_COUNT
    ):
        raise RuntimeError(
            f"[업로드 중단] 기존 {existing_count}개 대비 새 수집 {len(items)}개로 급감했습니다. "
            f"허용 비율 MIN_EXISTING_RATIO={MIN_EXISTING_RATIO}. 기존 Firestore 데이터를 유지합니다."
        )

    delete_count = 0
    batch = db.batch()
    batch_ops = 0

    for ref in existing_refs:
        batch.delete(ref)
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
    new_count = 0
    restock_count = 0
    price_drop_count = 0

    for item in items:
        product_key = normalize_product_key(item.name)
        previous = previous_by_id.get(item.item_id)
        if previous is None:
            previous = previous_by_site_key.get(f"{item.site}|{product_key}")

        payload = to_firestore_doc(item, previous)
        if payload.get("isNew"):
            new_count += 1
        if payload.get("isRestock"):
            restock_count += 1
        if payload.get("isPriceDrop"):
            price_drop_count += 1

        doc_ref = col.document(item.item_id)
        batch.set(doc_ref, payload)
        batch_ops += 1
        write_count += 1

        if batch_ops >= 400:
            batch.commit()
            batch = db.batch()
            batch_ops = 0

    if batch_ops > 0:
        batch.commit()

    print(f"[Firestore] 업로드 완료: {write_count}개")
    print(f"[변화 감지] NEW {new_count}개 / 재입고 {restock_count}개 / 가격하락 {price_drop_count}개")

def save_local_backup(items: List[ItemRecord], path: str = "kr_aggregated_debug.json"):
    payload = [asdict(x) for x in items]
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f"[로컬 백업 저장] {path} / {len(items)}개")

    backup_dir = Path("backups")
    backup_dir.mkdir(exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    latest_path = backup_dir / "kr_aggregated_latest.json"
    ts_path = backup_dir / f"kr_aggregated_backup_{ts}.json"
    for out in [latest_path, ts_path]:
        with open(out, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f"[백업 저장] {latest_path} / {ts_path}")


def run_crawler_safely(label: str, func, *args) -> List[ItemRecord]:
    started = time.time()
    try:
        items = func(*args)
        elapsed = time.time() - started
        print(f"[사이트 완료] {label}: {len(items)}개 / {elapsed:.1f}s")
        return items
    except Exception as e:
        elapsed = time.time() - started
        record_failure(label, "SITE_LEVEL", e)
        print(f"[사이트 실패] {label}: {type(e).__name__}: {e} / {elapsed:.1f}s")
        return []


def crawl_bnkrmall_resilient(session: requests.Session) -> List[ItemRecord]:
    # Selenium이 실패하거나 제목이 전부 상품상세로 잡히면 API 방식으로 한 번 더 시도한다.
    selenium_items = run_crawler_safely("BNKR Selenium", crawl_bnkrmall_selenium)
    good = [x for x in selenium_items if not is_bad_title(x.name or x.title)]
    if len(good) >= 10:
        return good

    if selenium_items and len(good) < len(selenium_items):
        print(f"[BNKR 경고] 잘못된 제목 제거: {len(selenium_items)}개 -> {len(good)}개")

    api_items = run_crawler_safely("BNKR API fallback", crawl_bnkrmall, session)
    api_good = [x for x in api_items if not is_bad_title(x.name or x.title)]
    if len(api_good) >= len(good):
        return api_good
    return good


def count_by_mall(items: List[ItemRecord]):
    from collections import Counter
    return Counter(item.mall_name for item in items)


def print_site_counts(title: str, items: List[ItemRecord]):
    counter = count_by_mall(items)
    print(title)
    for mall, count in sorted(counter.items()):
        print(f"{mall}: {count}")
    print("==================================")
    return counter



def save_failed_events_report(path: str = "failed_links_report.json"):
    backup_dir = Path("backups")
    backup_dir.mkdir(exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    grouped: Dict[str, List[Dict[str, str]]] = {}
    for event in FAILED_EVENTS:
        grouped.setdefault(event.get("site", "unknown"), []).append(event)

    report = {
        "generatedAtUtc": datetime.now(timezone.utc).isoformat(),
        "totalFailures": len(FAILED_EVENTS),
        "failuresBySite": {site: len(events) for site, events in sorted(grouped.items())},
        "events": FAILED_EVENTS,
    }

    targets = [Path(path), backup_dir / "failed_links_latest.json", backup_dir / f"failed_links_{ts}.json"]
    for out in targets:
        with open(out, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)

    print(f"[실패 URL 저장] {path} / 실패 {len(FAILED_EVENTS)}건")


def save_health_history(items: List[ItemRecord], upload_allowed: bool, path: str = "crawler_health_history.json"):
    backup_dir = Path("backups")
    backup_dir.mkdir(exist_ok=True)
    counter = count_by_mall(items)
    now = datetime.now(timezone.utc).isoformat()
    entry = {
        "generatedAtUtc": now,
        "total": len(items),
        "uploadAllowed": upload_allowed,
        "fastTestMode": FAST_TEST_MODE,
        "countsByMall": dict(sorted(counter.items())),
        "failureCount": len(FAILED_EVENTS),
        "criticalThresholds": CRITICAL_SITE_MIN_COUNTS,
        "warningThresholds": WARNING_SITE_MIN_COUNTS,
    }

    history: List[Dict] = []
    latest = backup_dir / path
    if latest.exists():
        try:
            history = json.loads(latest.read_text(encoding="utf-8"))
            if not isinstance(history, list):
                history = []
        except Exception:
            history = []
    history.append(entry)
    history = history[-100:]

    for out in [Path(path), latest]:
        with open(out, "w", encoding="utf-8") as f:
            json.dump(history, f, ensure_ascii=False, indent=2)

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    snapshot = backup_dir / f"crawler_health_{ts}.json"
    with open(snapshot, "w", encoding="utf-8") as f:
        json.dump(entry, f, ensure_ascii=False, indent=2)

    print(f"[건강 기록 저장] {path} / {snapshot}")


def validate_crawl_health(items: List[ItemRecord]) -> bool:
    counter = count_by_mall(items)
    total = len(items)
    errors: List[str] = []
    warnings: List[str] = []

    if total < MIN_TOTAL_UPLOAD:
        errors.append(f"총 수집량 {total}개 < 최소 기준 {MIN_TOTAL_UPLOAD}개")

    for mall, minimum in CRITICAL_SITE_MIN_COUNTS.items():
        count = counter.get(mall, 0)
        if count < minimum:
            errors.append(f"{mall} {count}개 < 핵심 기준 {minimum}개")

    for mall, minimum in WARNING_SITE_MIN_COUNTS.items():
        count = counter.get(mall, 0)
        if count < minimum:
            warnings.append(f"{mall} {count}개 < 권장 기준 {minimum}개")

    print("===== 크롤링 건강 상태 =====")
    print(f"총 수집량: {total}개")
    for mall in sorted(set(CRITICAL_SITE_MIN_COUNTS) | set(WARNING_SITE_MIN_COUNTS) | set(counter.keys())):
        count = counter.get(mall, 0)
        if mall in CRITICAL_SITE_MIN_COUNTS:
            minimum = CRITICAL_SITE_MIN_COUNTS[mall]
            mark = "OK" if count >= minimum else "DANGER"
            print(f"{mall}: {count}개 / 기준 {minimum}개 / {mark}")
        elif mall in WARNING_SITE_MIN_COUNTS:
            minimum = WARNING_SITE_MIN_COUNTS[mall]
            mark = "OK" if count >= minimum else "WARN"
            print(f"{mall}: {count}개 / 권장 {minimum}개 / {mark}")
        else:
            print(f"{mall}: {count}개")

    if warnings:
        print("----- 경고 -----")
        for w in warnings:
            print(f"[경고] {w}")

    if errors:
        print("----- 위험 -----")
        for e in errors:
            print(f"[위험] {e}")
        if not FORCE_UPLOAD_LOW_COUNT:
            print("[업로드 판단] 중단: 기존 Firestore 데이터를 보호합니다.")
            return False
        print("[업로드 판단] FORCE_UPLOAD_LOW_COUNT=1 이므로 강제 진행합니다.")

    print("[업로드 판단] 진행 가능")
    print("============================")
    return True

def main():
    print("=== KR 통합 크롤링 시작 ===")
    print(f"[실행 모드] FAST_TEST_MODE={FAST_TEST_MODE} / MAX_LINKS_PER_SITE={MAX_LINKS_PER_SITE}")

    try:
        db = init_firestore()
        print("[초기화] Firestore 연결 성공")
        print(f"[Firestore 컬렉션] {COLLECTION_NAME}")
    except Exception as e:
        print(f"[초기화 실패] {type(e).__name__}: {e}")
        raise

    session = make_session()
    print("[초기화] requests 세션 생성 성공")
    print("[활성 사이트] 건담샵, 하비팩토리, 건담시티, 모델세일, 조이하비, 건담붐, 프라모델매니아, 지온샵, 건담몰, BNKR")

    site_results: Dict[str, List[ItemRecord]] = {}
    site_results["모델세일"] = run_crawler_safely("모델세일", crawl_modelsale, session)
    site_results["하비팩토리"] = run_crawler_safely("하비팩토리", crawl_hobbyfactory, session)
    site_results["건담시티"] = run_crawler_safely("건담시티", crawl_gundamcity, session)
    site_results["조이하비"] = run_crawler_safely("조이하비", crawl_joyhobby, session)
    site_results["건담붐"] = run_crawler_safely("건담붐", crawl_gundamboom, session)
    site_results["프라모델매니아"] = run_crawler_safely("프라모델매니아", crawl_plamodelmania, session)
    site_results["지온샵"] = run_crawler_safely("지온샵", crawl_zeonshop, session)
    site_results["건담몰"] = run_crawler_safely("건담몰", crawl_gundamall, session)
    site_results["건담샵"] = run_crawler_safely("건담샵", crawl_gundamshop, session)
    site_results["반다이남코코리아몰"] = crawl_bnkrmall_resilient(session)
    print(f"[사이트 완료] 반다이남코코리아몰: {len(site_results['반다이남코코리아몰'])}개")

    merged: List[ItemRecord] = []
    for items in site_results.values():
        merged.extend(items)

    print_site_counts("===== 사이트별 병합 개수 =====", merged)

    merged = filter_bad_records(merged)
    print_site_counts("===== 품질 필터 후 사이트별 개수 =====", merged)

    merged = dedupe_records(merged)
    print_site_counts("===== 중복 제거 후 사이트별 개수 =====", merged)

    merged = sort_records(merged)
    print(f"[정렬 후] 총 {len(merged)}개")

    save_local_backup(merged)
    save_failed_events_report()

    upload_allowed = validate_crawl_health(merged)
    save_health_history(merged, upload_allowed)

    if not upload_allowed:
        raise RuntimeError("크롤링 건강 상태가 기준 미달이라 Firestore 업로드를 중단했습니다.")

    upload_to_firestore(db, merged)

    print("=== 완료 ===")


if __name__ == "__main__":
    main()