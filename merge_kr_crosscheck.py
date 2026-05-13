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
# Selenium은 BNKR 보조 수집에서만 사용한다.
# GitHub Actions 환경에 selenium/webdriver_manager가 없더라도 스크립트 전체가 import 단계에서 죽지 않도록 선택 의존성으로 둔다.
try:
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service
    from webdriver_manager.chrome import ChromeDriverManager
    from selenium.webdriver.common.by import By
except Exception:
    webdriver = None
    Service = None
    ChromeDriverManager = None
    By = None

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
    image_url: str = ""
    image_source: str = ""

def crawl_bnkrmall_selenium() -> List[ItemRecord]:
    if webdriver is None or Service is None or ChromeDriverManager is None or By is None:
        print("[BNKR Selenium 건너뜀] selenium/webdriver_manager 미설치 - API fallback으로 진행")
        return []

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
                        image_url=parse_image_from_soup(soup, link),
                        image_source="bnkrmall_selenium",
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
# 최후 안전장치: 테스트/저수량 업로드는 명시적으로 허용하지 않으면 절대 Firestore를 덮어쓰지 않는다.
ALLOW_TEST_UPLOAD = os.getenv("ALLOW_TEST_UPLOAD", "0").strip().lower() in {"1", "true", "yes", "y"}
ABSOLUTE_MIN_UPLOAD = int(os.getenv("ABSOLUTE_MIN_UPLOAD", "250"))

# 핵심 사이트: 이 사이트들이 크게 무너지면 데이터 품질에 직접 영향이 크다.
CRITICAL_SITE_MIN_COUNTS = {
    # 정말 핵심인 두 사이트만 업로드 차단 기준으로 둔다.
    # 나머지 사이트는 순간적인 차단/인코딩/구조 변경이 잦아서 경고로만 관리한다.
    "건담샵": 120,
    "모델세일": 80,
}

# 보조 사이트: 0개 또는 급감 시 경고는 남기지만 전체 업로드를 막지는 않는다.
# 조이하비/하비팩토리는 수집은 되는데 필터/접속 상태에 따라 흔들릴 수 있어 차단 조건에서 제외한다.
WARNING_SITE_MIN_COUNTS = {
    "하비팩토리": 70,
    "건담시티": 5,
    "조이하비": 0,
    "지온샵": 20,
    "프라모델매니아": 5,
    "반다이남코코리아몰": 0,
    "건담붐": 0,
    "건담몰": 0,
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
    "회원게시글검색",
    "회원게시글검색 재고 문의드립니다",
    "재고 문의드립니다",
    "문의드립니다",
}

BAD_BOARD_PATTERNS = [
    r"회원\s*게시글\s*검색",
    r"게시글\s*검색",
    r"재고\s*문의",
    r"문의\s*드립니다",
    r"문의\s*드려요",
    r"질문\s*드립니다",
    r"재고\s*있나요",
    r"재고\s*문의드립니다",
    r"입고\s*문의",
    r"예약\s*문의",
]

def is_board_or_qna_noise(text: str) -> bool:
    t = normalize_space(text or "")
    if not t:
        return False
    for pat in BAD_BOARD_PATTERNS:
        if re.search(pat, t, flags=re.IGNORECASE):
            return True
    # 날짜가 붙은 게시판/문의성 제목도 제거
    if re.search(r"\b20\d{2}[./-]?(0?[1-9]|1[0-2])[./-]?(0?[1-9]|[12]\d|3[01])", t) and re.search(r"문의|게시글|검색|재고", t):
        return True
    return False

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
        "하츠네",
        "미쿠",
        "파이널 판타지",
        "FINAL FANTASY",
        "소프비",
        "데스크탑 페어리",
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
    if is_board_or_qna_noise(text):
        return True
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
    """Firestore 초기화.

    우선순위:
    1) serviceAccountKey.json 파일
    2) GitHub Actions/Railway Secret의 FIREBASE_SERVICE_ACCOUNT_JSON 값

    기존 파일 방식은 그대로 유지하고, CI 환경에서 secret만 있는 경우도 안전하게 지원한다.
    """
    service_account_source = SERVICE_ACCOUNT_PATH

    if not os.path.exists(SERVICE_ACCOUNT_PATH):
        raw_secret = os.getenv("FIREBASE_SERVICE_ACCOUNT_JSON", "").strip()
        if raw_secret:
            try:
                parsed = json.loads(raw_secret)
                temp_path = Path("serviceAccountKey.from_env.json")
                temp_path.write_text(json.dumps(parsed, ensure_ascii=False), encoding="utf-8")
                service_account_source = str(temp_path)
                print("[초기화] FIREBASE_SERVICE_ACCOUNT_JSON secret으로 Firestore 인증 파일 생성")
            except Exception as e:
                raise RuntimeError(
                    "FIREBASE_SERVICE_ACCOUNT_JSON secret을 JSON으로 읽지 못했습니다. "
                    "GitHub Secrets 값이 서비스 계정 JSON 전체인지 확인하세요."
                ) from e
        else:
            raise FileNotFoundError(
                f"{SERVICE_ACCOUNT_PATH} 파일이 없고 FIREBASE_SERVICE_ACCOUNT_JSON secret도 비어 있습니다. "
                f"로컬에서는 {SERVICE_ACCOUNT_PATH}를 같은 폴더에 넣고, GitHub Actions에서는 secret을 설정해줘."
            )

    if not firebase_admin._apps:
        cred = credentials.Certificate(service_account_source)
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
    if is_board_or_qna_noise(title):
        return True
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


def _looks_like_product_image_url(url: str) -> bool:
    lower = (url or "").lower()
    if not lower or lower.startswith("data:"):
        return False
    bad_parts = [
        "logo", "icon", "ico_", "btn_", "banner", "loading", "blank", "noimage",
        "spacer", "sns", "kakao", "naver", "facebook", "instagram", "youtube",
        "common/", "/skin/", "/layout/", "/board/", "/event/", "review", "star",
    ]
    if any(x in lower for x in bad_parts):
        return False
    good_parts = [
        "product", "goods", "item", "shopimages", "web/product", "big", "detail",
        "thumbnail", "thumb", ".jpg", ".jpeg", ".png", ".webp",
    ]
    return any(x in lower for x in good_parts)


def parse_image_from_soup(soup: BeautifulSoup, base_url: str) -> str:
    """상세 페이지에서 대표 상품 이미지를 추출한다.
    실제 이미지 다운로드/검증은 하지 않고 URL 기반으로 안전하게 후보를 고른다.
    """
    candidates: List[str] = []

    meta_selectors = [
        ('meta[property="og:image"]', 'content'),
        ('meta[property="og:image:secure_url"]', 'content'),
        ('meta[name="twitter:image"]', 'content'),
        ('meta[itemprop="image"]', 'content'),
    ]
    for selector, attr in meta_selectors:
        for el in soup.select(selector):
            value = normalize_space(el.get(attr) or "")
            if value:
                candidates.append(value)

    img_selectors = [
        ".keyImg img", ".thumbnail img", ".thumb img", ".prdImg img", ".product_image img",
        ".xans-product-image img", ".detailArea img", ".goods_img img", ".goodsImg img",
        "img[src]",
    ]
    for selector in img_selectors:
        for img in soup.select(selector):
            for attr in ["data-origin", "data-original", "data-src", "ec-data-src", "src"]:
                value = normalize_space(img.get(attr) or "")
                if value:
                    candidates.append(value)

    html = str(soup)
    for raw in re.findall(r'https?://[^\'"\\s<>]+(?:jpg|jpeg|png|webp)(?:\\?[^\'"\\s<>]*)?', html, re.IGNORECASE):
        candidates.append(raw)

    normalized: List[str] = []
    seen: Set[str] = set()
    for raw in candidates:
        full = urljoin(base_url, raw.strip())
        full = full.replace("&amp;", "&")
        if full in seen:
            continue
        seen.add(full)
        if _looks_like_product_image_url(full):
            normalized.append(full)

    if not normalized:
        return ""

    def score(u: str) -> int:
        l = u.lower()
        sc = 0
        if "og:image" in l or "big" in l or "detail" in l:
            sc += 12
        if "shopimages" in l or "web/product" in l or "/product/" in l or "/goods/" in l:
            sc += 10
        if "thumb" in l or "thumbnail" in l:
            sc += 4
        if re.search(r"(500|600|700|800|900|1000|1200)", l):
            sc += 5
        if re.search(r"(80x80|100x100|120x120|small|tiny)", l):
            sc -= 12
        if l.endswith(".webp"):
            sc += 3
        return sc

    normalized.sort(key=score, reverse=True)
    return normalized[0]

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

        # 게시판/메인/커뮤니티 링크가 상품처럼 들어오면 앱에 잡문이 노출될 수 있어 제외
        if any(x in lower for x in [
            "/community", "/board", "/notice", "/member", "/login", "/join",
            "/basket", "/cart", "/order", "/mypage", "/main.html",
            "search_board", "board_no", "article",
        ]):
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
            image_url=image_url if 'image_url' in locals() else "",
            image_source=site if 'site' in locals() else "",
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


def extract_gundambase_candidate_links(soup: BeautifulSoup, base_url: str) -> Set[str]:
    """건담베이스 상품 상세 링크 추출.

    현재 main에서는 건담베이스를 기본 실행하지 않지만, 함수가 없으면 나중에 다시 활성화했을 때
    NameError로 해당 사이트 전체가 0개 처리될 수 있어 호환용으로 보강한다.
    """
    links: Set[str] = set()

    exclude_patterns = [
        "/board/", "/member/", "/order/", "/cart/", "/myshop/",
        "/article/", "/notice/", "/faq/", "/event/", "javascript:", "mailto:",
        "basket.html", "login", "review", "qna", "search.html?",
    ]

    def try_add(raw: str):
        raw = (raw or "").strip()
        if not raw or raw.startswith("#"):
            return
        full = urljoin(base_url, raw)
        lower = full.lower()
        if "thegundambase.co.kr" not in lower:
            return
        if any(x in lower for x in exclude_patterns):
            return
        if "/product/detail.html" in lower or "product_no=" in lower:
            links.add(full)

    for a in soup.select("a[href]"):
        try_add(a.get("href") or "")

    html = str(soup)
    for raw in re.findall(r"(?:href|data-url|ec-data-href)=['\"]([^'\"]+)['\"]", html, re.IGNORECASE):
        try_add(raw)

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
        image_url = parse_image_from_soup(soup, url)

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
            image_url=image_url if 'image_url' in locals() else "",
            image_source=site if 'site' in locals() else "",
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
        image_url = parse_image_from_soup(soup, url)

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
            image_url=image_url if 'image_url' in locals() else "",
            image_source=site if 'site' in locals() else "",
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
        image_url = parse_image_from_soup(soup, url)

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
            image_url=image_url if 'image_url' in locals() else "",
            image_source=site if 'site' in locals() else "",
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
        image_url = parse_image_from_soup(soup, url)

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
            image_url=image_url if 'image_url' in locals() else "",
            image_source=site if 'site' in locals() else "",
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
            image_url=image_url if 'image_url' in locals() else "",
            image_source=site if 'site' in locals() else "",
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
        image_url = parse_image_from_soup(soup, url)

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
            image_url=image_url if 'image_url' in locals() else "",
            image_source=site if 'site' in locals() else "",
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
            image_url=image_url if 'image_url' in locals() else "",
            image_source=site if 'site' in locals() else "",
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
        resp.raise_for_status()
        try:
            data = resp.json()
        except Exception as e:
            raise RuntimeError(f"BNKR API 응답이 JSON이 아닙니다: {resp.text[:200]}") from e

        items = data.get("list", []) if isinstance(data, dict) else []

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
            image_url = item.get("goodsImg") or item.get("imageUrl") or item.get("imgUrl") or item.get("listImg") or ""
            if image_url:
                image_url = urljoin(url, str(image_url))

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
                    image_url=image_url if 'image_url' in locals() else "",
                    image_source="bnkrmall_api",
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
    is_joyhobby = item.mall_name == "조이하비" or item.site == "joyhobby"

    # 게시판/문의글/검색 결과가 상품명으로 잘못 들어온 케이스 제거
    if is_board_or_qna_noise(joined):
        return True

    if is_bad_title(name):
        return True

    # 가격이 너무 낮으면 굿즈/옵션/부품일 가능성이 높음
    price = price_to_int(item.price)
    if price is not None and price < 3000:
        return True

    if is_excluded(name):
        return True

    # 조이하비는 상품명이 짧거나 코드형으로 잡히는 경우가 있어 기존 검증을 너무 세게 걸면
    # 정상 상품까지 전부 제거될 수 있다. 명백한 비건담/옵션류만 제거하고 나머지는 리포트에서 검토한다.
    if is_joyhobby:
        joy_block_words = [
            "데칼", "습식데칼", "스티커", "마커", "도료", "스프레이", "서페이서", "신너",
            "니퍼", "공구", "툴", "베이스", "스탠드", "옵션파츠", "OPTION PARTS",
            "DISPLAY BASE", "메탈빌드", "METAL BUILD", "로봇혼", "초합금",
            "울트라맨", "ULTRAMAN", "스타워즈", "STAR WARS", "포켓몬", "POKEMON",
            "하츠네", "미쿠", "파이널 판타지", "FINAL FANTASY",
        ]
        up = name.upper()
        if any(w.upper() in up for w in joy_block_words):
            return True
        # 조이하비는 상세명 파싱이 사이트 특성상 짧게 잡히는 경우가 많다.
        # 여기서 looks_like_gundam으로 다시 걸면 정상 상품 188개가 전부 제거될 수 있으므로
        # 명백한 옵션/비건담류만 제거하고 나머지는 match_report/needsReview에서 검토한다.
        return False

    if is_non_gundam_figure_like(name):
        return True

    if not is_valid_gundam_plamodel(name, joined):
        return True

    key = normalize_product_key(name)
    if key.endswith("|"):
        return True

    return False


def filter_bad_records(records: List[ItemRecord]) -> List[ItemRecord]:
    from collections import Counter

    clean: List[ItemRecord] = []
    removed = 0
    removed_by_mall = Counter()

    for item in records:
        if is_bad_record(item):
            removed += 1
            removed_by_mall[item.mall_name] += 1
            if DEBUG:
                print(f"[필터 제거] {item.mall_name} / {item.name} / {item.price} / {item.url}")
            continue

        # 상태값 다시 한 번 통일
        item.status = normalize_status(item.status or item.stock_text)
        item.stock_text = item.status

        # 앱 표시용 이름과 내부 비교용 이름을 통일한다.
        display_name = standardize_product_name(item.name or item.title)

        # 조이하비는 공식명 매칭이 실패해도 원본명을 살려둔다.
        # 완전 제거보다 needsReview/match_report에서 검토하는 편이 데이터 손실을 막는다.
        if (not display_name or is_bad_title(display_name)) and (item.mall_name == "조이하비" or item.site == "joyhobby"):
            display_name = clean_product_name(item.name or item.title)

        if not display_name or is_bad_title(display_name):
            removed += 1
            removed_by_mall[item.mall_name] += 1
            if DEBUG:
                print(f"[필터 제거:표시명 실패] {item.mall_name} / {item.name} / {item.url}")
            continue

        item.name = display_name
        item.title = display_name

        clean.append(item)

    print(f"[품질 필터] 제거 {removed}개 / 유지 {len(clean)}개")
    if removed_by_mall:
        print("[품질 필터 제거 내역]")
        for mall, count in sorted(removed_by_mall.items()):
            print(f"{mall}: {count}개 제거")
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




def calculate_image_quality(item: ItemRecord, *, quality: Dict, type_info: Dict) -> Dict:
    image_url = normalize_space(getattr(item, "image_url", "") or "")
    flags: List[str] = []
    reasons: List[str] = []

    if not image_url:
        score = 25
        flags.append("image_missing")
    else:
        score = 58
        reasons.append("image_url_present")
        lower = image_url.lower()

        if lower.startswith("https://"):
            score += 7
            reasons.append("https_image")
        elif lower.startswith("http://"):
            flags.append("http_image")

        if any(ext in lower for ext in [".jpg", ".jpeg", ".png", ".webp"]):
            score += 8
            reasons.append("valid_image_extension")
        else:
            flags.append("unknown_image_extension")

        if any(x in lower for x in ["shopimages", "web/product", "/product/", "/goods/", "big", "detail"]):
            score += 12
            reasons.append("likely_product_image")

        if any(x in lower for x in ["thumb", "thumbnail"]):
            score += 3
            reasons.append("thumbnail_image")

        if any(x in lower for x in ["logo", "icon", "banner", "noimage", "blank", "loading"]):
            score -= 35
            flags.append("possible_non_product_image")

        if re.search(r"(80x80|100x100|120x120|small|tiny)", lower):
            score -= 18
            flags.append("possible_low_resolution")

    if quality.get("score", 0) >= 80:
        score += 5
        reasons.append("high_data_quality")
    if type_info.get("typeCategory") == "gunpla":
        score += 5
        reasons.append("main_gunpla_item")
    if type_info.get("typeCategory") in {"decal", "tool", "paint", "option_parts"}:
        score -= 8
        flags.append("non_main_item_image")

    score = int(max(0, min(100, score)))
    if score >= 85:
        grade = "A"
    elif score >= 70:
        grade = "B"
    elif score >= 50:
        grade = "C"
    else:
        grade = "REVIEW"

    return {
        "imageUrl": image_url,
        "imageSource": getattr(item, "image_source", "") or item.site,
        "hasImage": bool(image_url),
        "imageQualityScore": score,
        "imageQualityGrade": grade,
        "imageQualityReasons": sorted(set(reasons))[:20],
        "imageQualityFlags": sorted(set(flags))[:20],
        "isImageQualityLow": score < 50,
        "isImageQualityHigh": score >= 80,
    }

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
    # 절대 안전장치: 테스트 모드/저수량 결과가 기존 Firestore를 삭제하지 못하게 막는다.
    if FAST_TEST_MODE and not ALLOW_TEST_UPLOAD:
        raise RuntimeError(
            '[업로드 중단] FAST_TEST_MODE=True 상태입니다. 테스트 결과는 Firestore에 업로드하지 않습니다. '
            'GitHub Actions env에서 FAST_TEST_MODE, MAX_LINKS_PER_SITE를 확인하세요.'
        )
    if len(items) < ABSOLUTE_MIN_UPLOAD and not FORCE_UPLOAD_LOW_COUNT:
        raise RuntimeError(
            f'[업로드 중단] 최종 수집 {len(items)}개가 절대 최소 기준 ABSOLUTE_MIN_UPLOAD={ABSOLUTE_MIN_UPLOAD}보다 적습니다. '
            '기존 Firestore 데이터를 보호하기 위해 삭제/업로드를 중단합니다.'
        )

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

    if FAST_TEST_MODE and not ALLOW_TEST_UPLOAD:
        errors.append('FAST_TEST_MODE=True 상태입니다. 테스트 결과 업로드를 차단합니다.')
    if total < ABSOLUTE_MIN_UPLOAD:
        errors.append(f"총 수집량 {total}개 < 절대 최소 기준 {ABSOLUTE_MIN_UPLOAD}개")
    elif total < MIN_TOTAL_UPLOAD:
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


# ===== 데이터 정규화 운영 보정: 수동 alias / blocked match / 신뢰도 / 자동 리포트 =====
ALIAS_PATH = os.getenv("ALIAS_PATH", "aliases.json")
BLOCKED_MATCHES_PATH = os.getenv("BLOCKED_MATCHES_PATH", "blocked_matches.json")
MATCH_REPORT_PATH = os.getenv("MATCH_REPORT_PATH", "match_report.json")

DEFAULT_ALIASES = {
    "aliases": {
        "AERIAL REBUILD": "건담 에어리얼 개수형",
        "GUNDAM AERIAL REBUILD": "건담 에어리얼 개수형",
        "SAZABI VER.KA": "사자비 Ver.Ka",
        "MSN-04 SAZABI VER.KA": "사자비 Ver.Ka",
        "MSN-04 SAZABI": "사자비",
        "NU GUNDAM": "뉴 건담",
        "RX-93 NU GUNDAM": "뉴 건담",
        "HI-NU GUNDAM": "하이 뉴 건담",
        "UNICORN GUNDAM DESTROY MODE": "유니콘 건담 디스트로이 모드",
        "AILE STRIKE GUNDAM": "에일 스트라이크 건담",
        "STRIKE FREEDOM GUNDAM": "스트라이크 프리덤 건담",
        "MIGHTY STRIKE FREEDOM GUNDAM": "마이티 스트라이크 프리덤 건담",
        "RISING FREEDOM GUNDAM": "라이징 프리덤 건담",
        "IMMORTAL JUSTICE GUNDAM": "임모탈 저스티스 건담",
        "GUNDAM EXIA": "건담 엑시아",
        "RX-78-2 GUNDAM": "퍼스트 건담",
        "ZETA GUNDAM": "제타 건담",
        "WING GUNDAM ZERO": "윙 건담 제로",
        "GUNDAM BARBATOS": "건담 발바토스"
    },
    "notes": [
        "왼쪽에는 사이트에서 들어올 수 있는 별칭, 오른쪽에는 앱에서 쓰고 싶은 대표 이름을 넣으면 됩니다.",
        "예: \"MSN-04 SAZABI\": \"사자비\"",
        "이 파일은 없으면 자동 생성되고, 직접 수정해도 다음 크롤링 때 반영됩니다."
    ]
}

DEFAULT_BLOCKED_MATCHES = {
    "blocked": {
        "프리덤 건담": ["스트라이크 프리덤 건담", "마이티 스트라이크 프리덤 건담", "라이징 프리덤 건담"],
        "스트라이크 프리덤 건담": ["마이티 스트라이크 프리덤 건담"],
        "유니콘 건담": ["유니콘 건담 2호기 밴시", "밴시 노른"],
        "뉴 건담": ["하이 뉴 건담"],
        "사자비": ["나이팅게일"],
        "건담 에어리얼": ["건담 에어리얼 개수형", "건담 캘리번"]
    },
    "notes": [
        "서로 이름이 비슷하지만 절대 같은 상품으로 묶으면 안 되는 조합입니다.",
        "왼쪽 이름과 오른쪽 목록 중 하나가 같은 productKey 그룹에 들어오면 match_report.json에 위험으로 표시합니다."
    ]
}

_MANUAL_ALIAS_CACHE = None
_BLOCKED_MATCH_CACHE = None


def _read_json_with_default(path: str, default: Dict) -> Dict:
    p = Path(path)
    if not p.exists():
        try:
            p.write_text(json.dumps(default, ensure_ascii=False, indent=2), encoding="utf-8")
            print(f"[정규화 관리 파일 생성] {path}")
        except Exception as e:
            print(f"[정규화 관리 파일 생성 실패] {path} / {type(e).__name__}: {e}")
        return default
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else default
    except Exception as e:
        print(f"[정규화 관리 파일 읽기 실패] {path} / {type(e).__name__}: {e}")
        return default


def load_manual_aliases() -> Dict[str, str]:
    global _MANUAL_ALIAS_CACHE
    if _MANUAL_ALIAS_CACHE is not None:
        return _MANUAL_ALIAS_CACHE
    data = _read_json_with_default(ALIAS_PATH, DEFAULT_ALIASES)
    aliases = data.get("aliases", data)
    if not isinstance(aliases, dict):
        aliases = {}
    normalized: Dict[str, str] = {}
    for k, v in aliases.items():
        if isinstance(v, str) and str(k).strip() and v.strip():
            normalized[_alias_probe(str(k))] = normalize_space(v)
    _MANUAL_ALIAS_CACHE = normalized
    print(f"[수동 alias] {len(normalized)}개 로드")
    return normalized


def load_blocked_matches() -> List[tuple[str, str]]:
    global _BLOCKED_MATCH_CACHE
    if _BLOCKED_MATCH_CACHE is not None:
        return _BLOCKED_MATCH_CACHE
    data = _read_json_with_default(BLOCKED_MATCHES_PATH, DEFAULT_BLOCKED_MATCHES)
    raw = data.get("blocked", data)
    pairs: List[tuple[str, str]] = []
    if isinstance(raw, dict):
        for left, rights in raw.items():
            if isinstance(rights, str):
                rights = [rights]
            if not isinstance(rights, list):
                continue
            for right in rights:
                if str(left).strip() and str(right).strip():
                    pairs.append((_alias_probe(str(left)), _alias_probe(str(right))))
    elif isinstance(raw, list):
        for entry in raw:
            if isinstance(entry, list) and len(entry) >= 2:
                pairs.append((_alias_probe(str(entry[0])), _alias_probe(str(entry[1]))))
            elif isinstance(entry, dict):
                left = entry.get("left") or entry.get("a")
                right = entry.get("right") or entry.get("b")
                if left and right:
                    pairs.append((_alias_probe(str(left)), _alias_probe(str(right))))
    _BLOCKED_MATCH_CACHE = pairs
    print(f"[금지 매칭] {len(pairs)}쌍 로드")
    return pairs


def _alias_probe(text: str) -> str:
    t = normalize_space(text or "")
    t = t.replace("ν", "NU")
    t = re.sub(r"VER\s*\.?\s*KA|버카|브이카", "VERKA", t, flags=re.IGNORECASE)
    t = re.sub(r"[^0-9A-Z가-힣]+", " ", t.upper())
    return normalize_space(t)


def _strip_grade_prefix(text: str) -> str:
    t = normalize_space(text)
    t = re.sub(r"^\[(MGEX|MGSD|FULL MECHANICS|RE/100|PG|MG|RG|HG|EG|SD|UNKNOWN)\]\s*", "", t, flags=re.IGNORECASE)
    return normalize_space(t)


def apply_manual_alias(text: str) -> str:
    aliases = load_manual_aliases()
    if not aliases:
        return ""
    probe = _alias_probe(text)
    if not probe:
        return ""
    # 긴 alias를 먼저 검사해야 STRIKE FREEDOM이 FREEDOM보다 먼저 잡힌다.
    for alias_probe, canonical in sorted(aliases.items(), key=lambda kv: len(kv[0]), reverse=True):
        if alias_probe and (probe == alias_probe or alias_probe in probe):
            return _strip_grade_prefix(canonical)
    return ""


def _base_canonical_core_name_before_manual_alias(name: str) -> str:
    source = _normalize_alias_source(name)
    probe = source.upper()

    for pattern, replacement in CANONICAL_ALIAS_RULES:
        if re.search(pattern, probe, flags=re.IGNORECASE):
            core = replacement
            break
    else:
        core = source

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

    core = normalize_space(core)
    core = re.sub(r"건담\s+건담", "건담", core)
    core = re.sub(r"\s+", " ", core).strip(" -_/[]()")
    return normalize_space(core)


# 기존 canonical_core_name/standardize_product_name/normalize_product_key를 운영형 alias 규칙으로 덮어쓴다.
def canonical_core_name(name: str) -> str:
    manual = apply_manual_alias(name)
    if manual:
        return manual
    core = _base_canonical_core_name_before_manual_alias(name)
    manual_core = apply_manual_alias(core)
    return manual_core or core


def standardize_product_name(name: str) -> str:
    original = clean_product_name(name)
    if not original:
        return ""
    grade = extract_grade(original)
    core = canonical_core_name(original)
    if not core:
        return ""

    if re.search(r"VER\s*\.?\s*KA|버카|브이카", original, re.IGNORECASE) and "Ver.Ka" not in core:
        core = f"{core} Ver.Ka"

    if core.upper() in {"GUNDAM", "건담", "MODEL", "KIT"}:
        core = clean_product_name(original)

    prefix = f"[{grade}] " if grade != "UNKNOWN" else ""
    return normalize_space(f"{prefix}{core}")


def normalize_product_key(name: str) -> str:
    grade = extract_grade(name)
    core = canonical_core_name(name)
    if re.search(r"VER\s*\.?\s*KA|버카|브이카", name or "", re.IGNORECASE) and "Ver.Ka" not in core:
        core = f"{core} Ver.Ka"
    key = core.upper().replace("VER.KA", "VERKA")
    key = re.sub(r"[^0-9A-Z가-힣]+", " ", key)
    key = normalize_space(key)
    if not key:
        return f"{grade}|"
    return f"{grade}|{key}"


def _core_token_set(text: str) -> Set[str]:
    probe = _alias_probe(text)
    tokens = {t for t in probe.split() if len(t) >= 2}
    noise = {"GUNDAM", "건담", "VERKA", "MODEL", "KIT", "HG", "MG", "RG", "PG", "SD", "EG"}
    return {t for t in tokens if t not in noise}


def is_blocked_match(a: str, b: str) -> bool:
    pa = _alias_probe(a)
    pb = _alias_probe(b)
    if not pa or not pb:
        return False
    for left, right in load_blocked_matches():
        if (left in pa and right in pb) or (left in pb and right in pa):
            return True
    return False


def calculate_match_confidence_for_name(name: str, price: Optional[str] = None, status: Optional[str] = None) -> Dict:
    grade = extract_grade(name)
    core = canonical_core_name(name)
    product_key = normalize_product_key(name)
    score = 0.48
    reasons: List[str] = []

    if grade != "UNKNOWN":
        score += 0.18
        reasons.append("grade_detected")
    else:
        reasons.append("grade_unknown")

    if apply_manual_alias(name) or apply_manual_alias(core):
        score += 0.18
        reasons.append("manual_alias")

    tokens = _core_token_set(core)
    if len(tokens) >= 2:
        score += 0.10
        reasons.append("specific_core")
    elif len(tokens) == 1:
        score -= 0.08
        reasons.append("short_core")
    else:
        score -= 0.22
        reasons.append("empty_core")

    if price_to_int(price or "") is not None:
        score += 0.04
        reasons.append("price_ok")

    if normalize_status(status or "") != "상태 확인중":
        score += 0.03
        reasons.append("status_ok")

    if is_bad_title(name):
        score = min(score, 0.20)
        reasons.append("bad_placeholder")

    if product_key.endswith("|"):
        score = min(score, 0.25)
        reasons.append("empty_product_key")

    score = max(0.05, min(0.99, score))
    return {
        "score": round(score, 2),
        "reasons": reasons,
        "core": core,
        "productKey": product_key,
    }


def to_firestore_doc(item: ItemRecord, previous: Optional[Dict] = None) -> Dict:
    price_int = price_to_int(item.price)
    display_name = standardize_product_name(item.name or item.title) or item.name
    product_key = normalize_product_key(display_name)
    grade = extract_grade(display_name)
    confidence = calculate_match_confidence_for_name(display_name, item.price, item.status)

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
        "matchConfidence": confidence["score"],
        "matchConfidenceReasons": confidence["reasons"],
        "canonicalCore": confidence["core"],
        "needsReview": confidence["score"] < 0.72,
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


def build_match_report(items: List[ItemRecord], path: str = MATCH_REPORT_PATH) -> Dict:
    groups: Dict[str, List[ItemRecord]] = {}
    for item in items:
        key = normalize_product_key(item.name or item.title)
        groups.setdefault(key, []).append(item)

    suspicious_groups: List[Dict] = []
    low_confidence_items: List[Dict] = []
    alias_candidates: List[Dict] = []
    blocked_warnings: List[Dict] = []

    for key, group in groups.items():
        names = sorted({standardize_product_name(i.name or i.title) or i.name for i in group})
        sellers = sorted({i.mall_name for i in group})
        prices = [price_to_int(i.price) for i in group if price_to_int(i.price) is not None]
        confidences = [calculate_match_confidence_for_name(i.name or i.title, i.price, i.status)["score"] for i in group]
        min_conf = min(confidences) if confidences else 0
        max_price = max(prices) if prices else None
        min_price = min(prices) if prices else None
        price_spread_ratio = round(max_price / min_price, 2) if min_price and max_price and min_price > 0 else None

        blocked_pairs = []
        for idx, left in enumerate(names):
            for right in names[idx + 1:]:
                if is_blocked_match(left, right):
                    blocked_pairs.append([left, right])
                    blocked_warnings.append({"productKey": key, "left": left, "right": right, "sellers": sellers})

        reasons = []
        if key.endswith("|"):
            reasons.append("empty_product_key")
        if min_conf < 0.72:
            reasons.append("low_confidence")
        if blocked_pairs:
            reasons.append("blocked_match_violation")
        if price_spread_ratio is not None and price_spread_ratio >= 3.0 and len(group) >= 2:
            reasons.append("price_spread_large")

        if reasons:
            suspicious_groups.append({
                "productKey": key,
                "count": len(group),
                "sellers": sellers,
                "names": names[:12],
                "minConfidence": min_conf,
                "priceRange": [min_price, max_price],
                "priceSpreadRatio": price_spread_ratio,
                "blockedPairs": blocked_pairs,
                "reasons": reasons,
            })

        for item in group:
            c = calculate_match_confidence_for_name(item.name or item.title, item.price, item.status)
            if c["score"] < 0.72:
                low_confidence_items.append({
                    "name": item.name,
                    "mallName": item.mall_name,
                    "price": item.price,
                    "status": item.status,
                    "productKey": c["productKey"],
                    "confidence": c["score"],
                    "reasons": c["reasons"],
                    "url": item.url,
                })

    # 같은 core인데 grade만 다르거나 key가 갈라진 후보를 잡는다.
    by_core: Dict[str, List[str]] = {}
    for key in groups.keys():
        parts = key.split("|", 1)
        core = parts[1] if len(parts) > 1 else key
        if core:
            by_core.setdefault(core, []).append(key)
    for core, keys in by_core.items():
        unique_keys = sorted(set(keys))
        if len(unique_keys) >= 2:
            alias_candidates.append({
                "canonicalCore": core,
                "productKeys": unique_keys,
                "reason": "same_core_different_grade_or_key",
            })

    report = {
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "totalItems": len(items),
        "totalGroups": len(groups),
        "suspiciousGroupCount": len(suspicious_groups),
        "lowConfidenceItemCount": len(low_confidence_items),
        "aliasCandidateCount": len(alias_candidates),
        "blockedWarningCount": len(blocked_warnings),
        "suspiciousGroups": suspicious_groups[:200],
        "lowConfidenceItems": low_confidence_items[:300],
        "aliasCandidates": alias_candidates[:200],
        "blockedWarnings": blocked_warnings[:200],
        "howToUse": {
            "aliases.json": "같은 상품으로 강제 통일하고 싶은 이름을 추가하세요.",
            "blocked_matches.json": "절대 같은 상품으로 묶으면 안 되는 조합을 추가하세요.",
            "matchConfidence": "0.72 미만이면 검토 권장, 0.85 이상이면 비교적 안정적입니다."
        }
    }
    try:
        Path(path).write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[매칭 리포트 저장] {path} / 의심 그룹 {len(suspicious_groups)}개 / 낮은 신뢰도 {len(low_confidence_items)}개")
    except Exception as e:
        print(f"[매칭 리포트 저장 실패] {path} / {type(e).__name__}: {e}")
    return report



# ===== 공식명 마스터 시스템 =====
OFFICIAL_PRODUCTS_PATH = os.getenv("OFFICIAL_PRODUCTS_PATH", "official_products.json")
_OFFICIAL_PRODUCTS_CACHE = None
_OFFICIAL_ALIAS_INDEX_CACHE = None

DEFAULT_OFFICIAL_PRODUCTS = {
    "products": {},
    "notes": [
        "official_products.json이 없을 때 자동 생성되는 빈 템플릿입니다.",
        "실제 운영에서는 별도 official_products.json 파일을 같이 커밋하세요."
    ]
}


def load_official_products() -> Dict:
    global _OFFICIAL_PRODUCTS_CACHE
    if _OFFICIAL_PRODUCTS_CACHE is not None:
        return _OFFICIAL_PRODUCTS_CACHE
    data = _read_json_with_default(OFFICIAL_PRODUCTS_PATH, DEFAULT_OFFICIAL_PRODUCTS)
    products = data.get("products", data)
    if not isinstance(products, dict):
        products = {}
    _OFFICIAL_PRODUCTS_CACHE = products
    print(f"[공식명 마스터] {len(products)}개 로드")
    return products


def _official_probe(text: str) -> str:
    t = normalize_space(text or "")
    t = t.replace("ν", "NU")
    t = re.sub(r"VER\s*\.?\s*KA|버카|브이카", "VERKA", t, flags=re.IGNORECASE)
    t = re.sub(r"\b1\s*/\s*(60|72|100|144|220|400|550)\b", " ", t, flags=re.IGNORECASE)
    t = re.sub(r"\b(MGEX|MGSD|FULL\s*MECHANICS|RE\s*/\s*100|HGUC|HGCE|HGBF|HGAC|HGFC|HGGW|HGWM|PG|MG|RG|HG|EG|SD)\b", " ", t, flags=re.IGNORECASE)
    t = re.sub(r"\b(BAN|BD)\s*\d{4,}\b", " ", t, flags=re.IGNORECASE)
    t = re.sub(r"\b\d{5,}\b", " ", t)
    t = re.sub(r"[^0-9A-Z가-힣]+", " ", t.upper())
    return normalize_space(t)


def build_official_alias_index() -> List[Dict]:
    global _OFFICIAL_ALIAS_INDEX_CACHE
    if _OFFICIAL_ALIAS_INDEX_CACHE is not None:
        return _OFFICIAL_ALIAS_INDEX_CACHE

    index: List[Dict] = []
    for key, info in load_official_products().items():
        if not isinstance(info, dict):
            continue
        official_name = normalize_space(str(info.get("officialName", "")))
        if not official_name:
            continue
        grade = normalize_space(str(info.get("grade", ""))) or extract_grade(official_name)
        aliases = list(info.get("aliases", []) or [])
        aliases.extend([official_name, _strip_grade_prefix(official_name), key])

        for alias in aliases:
            probe = _official_probe(str(alias))
            if not probe or len(probe) < 2:
                continue
            index.append({
                "probe": probe,
                "officialName": official_name,
                "grade": grade,
                "key": key,
                "series": info.get("series", ""),
            })

    # 긴 별칭 우선: STRIKE FREEDOM이 FREEDOM보다 먼저 매칭되어야 한다.
    index.sort(key=lambda x: len(x["probe"]), reverse=True)
    _OFFICIAL_ALIAS_INDEX_CACHE = index
    print(f"[공식명 alias index] {len(index)}개 생성")
    return index


def lookup_official_product_name(name: str, *, grade_hint: Optional[str] = None) -> Optional[Dict]:
    raw = normalize_space(name or "")
    if not raw:
        return None
    grade = grade_hint or extract_grade(raw)
    raw_probe = _official_probe(raw)
    core_probe = _official_probe(_base_canonical_core_name_before_manual_alias(raw))
    manual_probe = _official_probe(apply_manual_alias(raw))
    probes = [p for p in [raw_probe, core_probe, manual_probe] if p]

    for entry in build_official_alias_index():
        entry_grade = entry.get("grade") or extract_grade(entry.get("officialName", ""))
        # 등급이 확실히 다르면 섞지 않는다. UNKNOWN은 예외적으로 허용.
        if grade != "UNKNOWN" and entry_grade and entry_grade != "UNKNOWN" and grade != entry_grade:
            continue
        alias_probe = entry["probe"]
        for p in probes:
            if p == alias_probe or alias_probe in p or p in alias_probe:
                return entry
    return None


def apply_official_product_name(name: str) -> str:
    original = clean_product_name(name)
    if not original:
        return ""
    hit = lookup_official_product_name(original)
    if hit:
        return normalize_space(hit["officialName"])
    return standardize_product_name(original)


# 공식명 마스터 적용 버전으로 최종 함수 재정의
_PRE_OFFICIAL_STANDARDIZE_PRODUCT_NAME = standardize_product_name
_PRE_OFFICIAL_NORMALIZE_PRODUCT_KEY = normalize_product_key


def standardize_product_name(name: str) -> str:
    original = clean_product_name(name)
    if not original:
        return ""
    hit = lookup_official_product_name(original)
    if hit:
        return normalize_space(hit["officialName"])
    return _PRE_OFFICIAL_STANDARDIZE_PRODUCT_NAME(original)


def normalize_product_key(name: str) -> str:
    display = standardize_product_name(name)
    grade = extract_grade(display or name)
    core = _strip_grade_prefix(display or canonical_core_name(name))
    core = re.sub(r"VER\s*\.\s*KA", "VERKA", core, flags=re.IGNORECASE)
    key = _official_probe(core)
    if not key:
        return f"{grade}|"
    return f"{grade}|{key}"


def calculate_match_confidence_for_name(name: str, price: Optional[str] = None, status: Optional[str] = None) -> Dict:
    grade = extract_grade(name)
    official_hit = lookup_official_product_name(name)
    core = _strip_grade_prefix(official_hit["officialName"]) if official_hit else canonical_core_name(name)
    product_key = normalize_product_key(name)
    score = 0.50
    reasons: List[str] = []

    if official_hit:
        score += 0.28
        reasons.append("official_master")
    if grade != "UNKNOWN":
        score += 0.16
        reasons.append("grade_detected")
    else:
        score -= 0.12
        reasons.append("grade_unknown")
    if apply_manual_alias(name) or apply_manual_alias(core):
        score += 0.08
        reasons.append("manual_alias")
    tokens = _core_token_set(core)
    if len(tokens) >= 2:
        score += 0.06
        reasons.append("specific_core")
    elif len(tokens) <= 0:
        score -= 0.22
        reasons.append("empty_core")
    if price_to_int(price or "") is not None:
        score += 0.03
        reasons.append("price_ok")
    if normalize_status(status or "") != "상태 확인중":
        score += 0.02
        reasons.append("status_ok")
    if is_bad_title(name):
        score = min(score, 0.15)
        reasons.append("bad_placeholder")
    if product_key.endswith("|"):
        score = min(score, 0.25)
        reasons.append("empty_product_key")
    score = max(0.05, min(0.99, score))
    return {"score": round(score, 2), "reasons": reasons, "core": core, "productKey": product_key}




# ===== 데이터 품질 시스템 v2/v3: 상품 타입 분류 + 모델 중심 통합 키 =====
# v1 qualityScore를 유지하면서 Firestore에 추가 필드만 얹는다.
# 기존 앱이 이 필드를 아직 읽지 않아도 동작은 변하지 않는다.

EXTRA_NICKNAME_ALIAS_RULES = [
    (r"스프덤|스트프|STRIKE\s*FREE", "스트라이크 프리덤 건담"),
    (r"마프덤|마이티\s*프리덤|MIGHTY\s*FREE", "마이티 스트라이크 프리덤 건담"),
    (r"라프덤|라이징\s*프리덤|RISING\s*FREE", "라이징 프리덤 건담"),
    (r"임저|임모탈\s*저스티스|IMMORTAL\s*JUSTICE", "임모탈 저스티스 건담"),
    (r"뉴건담|뉴\s*건|NU\s*GUNDAM|RX[-\s]*93", "뉴 건담"),
    (r"하이뉴|하이\s*뉴|HI[-\s]*NU|HI\s*ν", "하이 뉴 건담"),
    (r"에어리얼\s*개수|AERIAL\s*REBUILD", "건담 에어리얼 개수형"),
    (r"캘리번|CALIBARN", "건담 캘리번"),
    (r"발바토스\s*루프스\s*렉스|BARBATOS\s*LUPUS\s*REX", "건담 발바토스 루프스 렉스"),
    (r"발바토스\s*루프스|BARBATOS\s*LUPUS", "건담 발바토스 루프스"),
    (r"발바토스|BARBATOS", "건담 발바토스"),
    (r"사자비|SAZABI|MSN[-\s]*04", "사자비"),
    (r"시난주\s*스타인|SINANJU\s*STEIN", "시난주 스타인"),
    (r"시난주|SINANJU", "시난주"),
    (r"유니콘.*밴시|밴시\s*노른|BANSHEE\s*NORN", "유니콘 건담 2호기 밴시 노른"),
    (r"유니콘|UNICORN", "유니콘 건담"),
]

_PRE_STAGE3_CANONICAL_CORE_NAME = canonical_core_name


def canonical_core_name(name: str) -> str:
    """기존 공식명/alias 시스템 위에 약칭 alias만 얇게 추가한다."""
    source = normalize_space(name or "")
    probe = source.upper().replace("ν", "NU")
    for pattern, replacement in EXTRA_NICKNAME_ALIAS_RULES:
        if re.search(pattern, source, flags=re.IGNORECASE) or re.search(pattern, probe, flags=re.IGNORECASE):
            return replacement
    return _PRE_STAGE3_CANONICAL_CORE_NAME(source)


def detect_brand(name: str) -> str:
    t = (name or "").upper()
    if any(k in t for k in ["BANDAI SPIRITS", "반다이 스피리츠", "반다이스피리츠"]):
        return "BANDAI SPIRITS"
    if any(k in t for k in ["BANDAI", "반다이"]):
        return "BANDAI"
    if any(k in t for k in ["KOTOBUKIYA", "코토부키야"]):
        return "KOTOBUKIYA"
    return "UNKNOWN"


def extract_scale(name: str) -> str:
    t = name or ""
    m = re.search(r"1\s*/\s*(60|72|100|144|220|400|550)", t, flags=re.IGNORECASE)
    if m:
        return f"1/{m.group(1)}"
    grade = extract_grade(t)
    if grade in {"PG"}:
        return "1/60"
    if grade in {"MG", "MGEX", "MGSD", "RE/100", "FULL MECHANICS"}:
        return "1/100"
    if grade in {"RG", "HG", "EG"}:
        return "1/144"
    return "UNKNOWN"


def extract_series(name: str) -> str:
    t = (name or "").upper()
    series_rules = [
        ("WITCH_FROM_MERCURY", ["수성의 마녀", "수성의마녀", "AERIAL", "CALIBARN", "SCHWARZETTE", "LFRITH", "르브리스", "에어리얼", "캘리번", "슈바르제테"]),
        ("SEED", ["SEED", "시드", "STRIKE", "FREEDOM", "JUSTICE", "스트라이크", "프리덤", "저스티스"]),
        ("UC", ["UNICORN", "유니콘", "BANSHEE", "밴시", "SINANJU", "시난주"]),
        ("CCA", ["NU GUNDAM", "뉴 건담", "뉴건담", "SAZABI", "사자비", "하이 뉴", "HI-NU"]),
        ("IBO", ["BARBATOS", "발바토스", "LUPUS", "루프스", "GUSION", "구시온"]),
        ("00", ["EXIA", "엑시아", "더블오", "00 GUNDAM", "DYNAMES", "듀나메스"]),
        ("WING", ["WING", "윙 건담", "DEATHSCYTHE", "데스사이즈", "HEAVYARMS", "헤비암즈"]),
        ("ZETA_ZZ", ["ZETA", "제타", "ZZ", "더블제타", "백식"]),
        ("FIRST", ["RX-78", "퍼스트", "GUNCANNON", "건캐논", "GUNTANK", "건탱크", "ZAKU", "자쿠", "DOM", " 돔"]),
    ]
    raw = name or ""
    for series, keys in series_rules:
        if any(k.upper() in t or k in raw for k in keys):
            return series
    return "UNKNOWN"


def classify_product_type(name: str, *, price: Optional[str] = None) -> Dict:
    """상품 타입 분류. 삭제/차단이 아니라 Firestore 분류 필드와 qualityFlags에 사용한다."""
    raw = normalize_space(name or "")
    t = raw.upper()
    flags: List[str] = []
    confidence = 0.55

    rules = [
        ("decal", "water_decal", ["데칼", "습식데칼", "WATER DECAL", "DECAL"]),
        ("paint", "paint_marker", ["도료", "마커", "스프레이", "서페이서", "신너", "PANEL LINE", "MARKER", "PAINT"]),
        ("tool", "build_tool", ["니퍼", "공구", "사포", "핀셋", "접착제", "커터", "TOOL", "NIPPER"]),
        ("option_parts", "display_base", ["액션베이스", "ACTION BASE", "스탠드", "DISPLAY BASE", "베이스"]),
        ("option_parts", "effect_parts", ["이펙트", "EFFECT", "LED", "확장", "EXPANSION", "옵션", "OPTION PARTS", "파츠 세트", "무장", "웨폰"]),
        ("figure", "completed_toy", ["메탈빌드", "METAL BUILD", "로봇혼", "ROBOT魂", "초합금", "완성품", "피규어", "FIGURE", "S.H.FIGUARTS", "피규아츠"]),
    ]
    for category, subtype, keywords in rules:
        if any(k.upper() in t for k in keywords):
            flags.append(f"type_{category}")
            return {
                "typeCategory": category,
                "subType": subtype,
                "typeConfidence": 0.9,
                "typeFlags": flags,
                "isGunplaMainItem": False,
            }

    grade = extract_grade(raw)
    looks_gunpla = looks_like_gundam(raw) and grade != "UNKNOWN"
    price_int = price_to_int(price or "")
    if looks_gunpla:
        confidence = 0.84
        if price_int is not None and price_int >= 3000:
            confidence += 0.06
        return {
            "typeCategory": "gunpla",
            "subType": "main_kit",
            "typeConfidence": round(min(0.98, confidence), 2),
            "typeFlags": flags,
            "isGunplaMainItem": True,
        }

    if looks_like_gundam(raw):
        flags.append("type_gundam_related_uncertain")
        return {
            "typeCategory": "gundam_related",
            "subType": "uncertain",
            "typeConfidence": 0.58,
            "typeFlags": flags,
            "isGunplaMainItem": False,
        }

    flags.append("type_unknown")
    return {
        "typeCategory": "etc",
        "subType": "unknown",
        "typeConfidence": 0.35,
        "typeFlags": flags,
        "isGunplaMainItem": False,
    }


def _safe_key_fragment(text: str) -> str:
    key = _official_probe(text)
    key = key.lower()
    key = re.sub(r"[^0-9a-z가-힣]+", "_", key)
    key = re.sub(r"_+", "_", key).strip("_")
    return key[:80] or "unknown"


def build_model_identity(name: str, official_hit: Optional[Dict] = None) -> Dict:
    """사이트 상품을 모델 중심으로 묶기 위한 통합 키."""
    display = standardize_product_name(name) or clean_product_name(name)
    official_name = normalize_space(official_hit.get("officialName", "")) if official_hit else ""
    model_name = official_name or display
    grade = extract_grade(model_name or name)
    core = _strip_grade_prefix(model_name or canonical_core_name(name))
    series = official_hit.get("series", "") if official_hit else ""
    if not series:
        series = extract_series(model_name or name)
    scale = extract_scale(model_name or name)
    brand = detect_brand(name)

    core_key = _safe_key_fragment(core)
    grade_key = grade.lower().replace("/", "_").replace(" ", "_") if grade else "unknown"
    master_model_id = f"{grade_key}_{core_key}" if core_key else f"{grade_key}_unknown"
    model_group_key = normalize_product_key(model_name or name)

    return {
        "masterModelId": master_model_id,
        "modelGroupKey": model_group_key,
        "modelCoreName": normalize_space(core),
        "modelDisplayName": normalize_space(model_name),
        "series": series or "UNKNOWN",
        "scale": scale,
        "brand": brand,
    }


# ===== 데이터 품질 점수 시스템 v1 =====
# 기존 matchConfidence는 "상품명/공식명 매칭 신뢰도"에 가깝고,
# qualityScore는 앱에서 바로 쓸 수 있는 "종합 데이터 품질 점수"다.
# 크롤링/필터/업로드 구조는 건드리지 않고 Firestore 필드만 추가한다.
SITE_QUALITY_BONUS = {
    "gundamshop": 4,
    "modelsale": 4,
    "joyhobby": 2,
    "hobbyfactory": 2,
    "gundamcity": 1,
    "zeonshop": 1,
    "plamodelmania": 0,
    "bnkrmall": 3,
    "gundamboom": 0,
    "gundamall": 0,
}


def calculate_quality_score(
    item: ItemRecord,
    *,
    display_name: str,
    official_hit: Optional[Dict],
    confidence: Dict,
) -> Dict:
    """Firestore/앱 표시용 종합 품질 점수.

    점수 기준:
    - 80~100: 안정적
    - 60~79 : 사용 가능하지만 일부 검토 권장
    - 0~59  : 검토 필요

    이 함수는 데이터를 삭제하지 않는다. 점수와 이유만 남긴다.
    """
    reasons: List[str] = []
    flags: List[str] = []

    score = 35.0

    confidence_score = float(confidence.get("score", 0) or 0)
    score += confidence_score * 35.0
    reasons.append(f"match_confidence_{confidence_score:.2f}")

    if official_hit:
        score += 12
        reasons.append("official_matched")
    else:
        score -= 4
        flags.append("official_unmatched")

    grade = extract_grade(display_name or item.name or item.title)
    if grade != "UNKNOWN":
        score += 7
        reasons.append("grade_detected")
    else:
        score -= 5
        flags.append("grade_unknown")

    price_int = price_to_int(item.price)
    if price_int is None:
        score -= 7
        flags.append("price_missing")
    elif price_int < 3000:
        score -= 25
        flags.append("price_too_low")
    elif price_int > 500000:
        score -= 6
        flags.append("price_too_high")
    else:
        score += 8
        reasons.append("price_valid")

    status = normalize_status(item.status or item.stock_text)
    if status != "상태 확인중":
        score += 6
        reasons.append("status_known")
    else:
        score -= 4
        flags.append("status_unknown")

    if item.product_url or item.detail_url or item.url:
        score += 4
        reasons.append("url_present")
    else:
        score -= 10
        flags.append("url_missing")

    site_bonus = SITE_QUALITY_BONUS.get((item.site or "").lower(), 0)
    if site_bonus:
        score += site_bonus
        reasons.append(f"site_bonus_{site_bonus}")

    name_probe = normalize_space(display_name or item.name or item.title)
    if is_bad_title(name_probe):
        score -= 35
        flags.append("bad_title")
    if is_board_or_qna_noise(f"{name_probe} {item.url}"):
        score -= 35
        flags.append("board_or_qna_noise")
    if is_non_gundam_figure_like(name_probe):
        score -= 25
        flags.append("non_gundam_or_figure_like")

    if confidence_score < 0.72:
        flags.append("low_match_confidence")
    if confidence.get("productKey", "").endswith("|"):
        flags.append("empty_product_key")

    type_info = classify_product_type(display_name or item.name or item.title, price=item.price)
    type_category = type_info.get("typeCategory", "etc")
    if type_category == "gunpla" and type_info.get("isGunplaMainItem"):
        score += 8
        reasons.append("type_gunpla_main_item")
    elif type_category in {"decal", "option_parts", "tool", "paint", "figure"}:
        score -= 18
        flags.append(f"type_{type_category}")
    elif type_category != "gunpla":
        score -= 5
        flags.append(f"type_{type_category}")

    score_int = int(round(max(0, min(100, score))))

    if score_int >= 80:
        grade_label = "A"
    elif score_int >= 70:
        grade_label = "B"
    elif score_int >= 60:
        grade_label = "C"
    else:
        grade_label = "REVIEW"

    return {
        "score": score_int,
        "grade": grade_label,
        "reasons": reasons[:20],
        "flags": flags[:20],
    }



# ===== 데이터 엔진 4~6단계: 가격 인텔리전스 / 재고 변화 / 추천 분석 =====
# 원칙: 기존 크롤링/필터/업로드 구조는 건드리지 않고 Firestore 필드만 추가한다.
DATA_ENGINE_CONTEXT: Dict[str, Dict] = {}


def _avg_int(values: List[int]) -> Optional[int]:
    clean = [v for v in values if isinstance(v, int) and v > 0]
    if not clean:
        return None
    return int(round(sum(clean) / len(clean)))


def _median_int(values: List[int]) -> Optional[int]:
    clean = sorted(v for v in values if isinstance(v, int) and v > 0)
    if not clean:
        return None
    mid = len(clean) // 2
    if len(clean) % 2:
        return clean[mid]
    return int(round((clean[mid - 1] + clean[mid]) / 2))


def _is_available_status(status: str) -> bool:
    return normalize_status(status) in {"판매중", "예약중", "입고예정"}


def _price_state(price_int: Optional[int], lowest: Optional[int], avg: Optional[int], median: Optional[int]) -> str:
    if price_int is None or price_int <= 0:
        return "unknown"
    ref = median or avg
    if lowest and price_int == lowest:
        return "lowest"
    if ref and price_int <= int(ref * 0.90):
        return "good_deal"
    if ref and price_int >= int(ref * 1.25):
        return "expensive"
    return "normal"


def _price_score(price_int: Optional[int], lowest: Optional[int], avg: Optional[int], median: Optional[int]) -> int:
    if price_int is None or price_int <= 0:
        return 0
    ref = median or avg or price_int
    if not ref or ref <= 0:
        return 50
    # 낮을수록 좋지만 비정상 저가는 이미 qualityFlags에서 다룬다.
    ratio = price_int / ref
    if lowest and price_int == lowest:
        base = 95
    elif ratio <= 0.90:
        base = 88
    elif ratio <= 1.05:
        base = 76
    elif ratio <= 1.20:
        base = 58
    elif ratio <= 1.40:
        base = 42
    else:
        base = 25
    return int(max(0, min(100, base)))


def build_data_engine_context(items: List[ItemRecord]) -> Dict[str, Dict]:
    """현재 크롤링 결과 전체를 모델 중심으로 묶어 가격/재고/추천 계산에 필요한 집계를 만든다."""
    global DATA_ENGINE_CONTEXT
    grouped: Dict[str, List[ItemRecord]] = {}

    for item in items:
        display = apply_official_product_name(item.name or item.title) or item.name
        official_hit = lookup_official_product_name(item.name or item.title) or lookup_official_product_name(display)
        identity = build_model_identity(display or item.name or item.title, official_hit=official_hit)
        grouped.setdefault(identity["masterModelId"], []).append(item)

    context: Dict[str, Dict] = {}
    for master_model_id, group in grouped.items():
        prices = [price_to_int(i.price) for i in group]
        prices = [p for p in prices if p is not None and p > 0]
        lowest = min(prices) if prices else None
        highest = max(prices) if prices else None
        avg = _avg_int(prices)
        median = _median_int(prices)
        sellers = sorted({i.mall_name for i in group if i.mall_name})
        available_items = [i for i in group if _is_available_status(i.status or i.stock_text)]
        in_stock_items = [i for i in group if normalize_status(i.status or i.stock_text) == "판매중"]
        preorder_items = [i for i in group if normalize_status(i.status or i.stock_text) == "예약중"]

        # 모델 단위 희소성: 판매처가 적고 판매중이 적으면 더 높게 본다.
        seller_count = len(sellers)
        available_count = len(available_items)
        rarity_score = 100
        rarity_score -= min(50, seller_count * 8)
        rarity_score -= min(35, available_count * 10)
        rarity_score = int(max(0, min(100, rarity_score)))

        context[master_model_id] = {
            "marketLowestPrice": lowest,
            "marketHighestPrice": highest,
            "marketAveragePrice": avg,
            "marketMedianPrice": median,
            "marketPriceCount": len(prices),
            "sellerCountForModel": seller_count,
            "availableSellerCount": len({i.mall_name for i in available_items if i.mall_name}),
            "inStockSellerCount": len({i.mall_name for i in in_stock_items if i.mall_name}),
            "preorderSellerCount": len({i.mall_name for i in preorder_items if i.mall_name}),
            "rarityScore": rarity_score,
            "modelSellers": sellers[:20],
        }

    DATA_ENGINE_CONTEXT = context
    print(f"[데이터 엔진 집계] 모델 그룹 {len(context)}개 / 가격·재고·추천 컨텍스트 생성")
    return context


def calculate_price_intelligence(item: ItemRecord, model_identity: Dict) -> Dict:
    master_model_id = model_identity.get("masterModelId", "")
    ctx = DATA_ENGINE_CONTEXT.get(master_model_id, {})
    price_int = price_to_int(item.price)
    lowest = ctx.get("marketLowestPrice")
    avg = ctx.get("marketAveragePrice")
    median = ctx.get("marketMedianPrice")

    price_state = _price_state(price_int, lowest, avg, median)
    price_score = _price_score(price_int, lowest, avg, median)
    discount_rate_vs_avg = None
    if price_int and avg and avg > 0:
        discount_rate_vs_avg = round((avg - price_int) / avg * 100, 1)

    return {
        "marketLowestPrice": lowest,
        "marketHighestPrice": ctx.get("marketHighestPrice"),
        "marketAveragePrice": avg,
        "marketMedianPrice": median,
        "marketPriceCount": ctx.get("marketPriceCount", 0),
        "sellerCountForModel": ctx.get("sellerCountForModel", 0),
        "availableSellerCount": ctx.get("availableSellerCount", 0),
        "inStockSellerCount": ctx.get("inStockSellerCount", 0),
        "preorderSellerCount": ctx.get("preorderSellerCount", 0),
        "isLowestPrice": bool(price_int and lowest and price_int == lowest),
        "priceScore": price_score,
        "priceState": price_state,
        "discountRateVsAverage": discount_rate_vs_avg,
        "modelSellers": ctx.get("modelSellers", []),
        "rarityScore": ctx.get("rarityScore", 0),
    }


def calculate_stock_intelligence(item: ItemRecord, previous: Optional[Dict], *, price_info: Dict, quality: Dict) -> Dict:
    status = normalize_status(item.status or item.stock_text)
    old_status = _doc_get_str(previous, ["status", "stockText", "stock_text", "stockState"])
    old_stock_state = _doc_get_str(previous, ["stockState"])
    old_restock_count = _doc_get_int(previous, ["restockCount"]) or 0

    if status == "판매중":
        if old_status in {"품절", "입고예정", "상태 확인중"}:
            stock_state = "restocked"
            restock_count = old_restock_count + 1
        elif old_status == "판매중" or old_stock_state == "in_stock":
            stock_state = "in_stock"
            restock_count = old_restock_count
        else:
            stock_state = "in_stock"
            restock_count = old_restock_count
    elif status == "예약중":
        stock_state = "preorder"
        restock_count = old_restock_count
    elif status == "입고예정":
        stock_state = "coming_soon"
        restock_count = old_restock_count
    elif status == "품절":
        stock_state = "sold_out"
        restock_count = old_restock_count
    else:
        stock_state = "unknown"
        restock_count = old_restock_count

    confidence = 45
    if status != "상태 확인중":
        confidence += 25
    if quality.get("score", 0) >= 70:
        confidence += 15
    if price_info.get("sellerCountForModel", 0) >= 2:
        confidence += 8
    if stock_state == "restocked":
        confidence += 7
    if quality.get("score", 0) < 60:
        confidence -= 20
    confidence = int(max(0, min(100, confidence)))

    return {
        "stockState": stock_state,
        "stockConfidence": confidence,
        "restockCount": restock_count,
        "wasPreviouslyAvailable": normalize_status(old_status) == "판매중",
        "isCurrentlyAvailable": status in {"판매중", "예약중", "입고예정"},
        "isReliableRestock": stock_state == "restocked" and confidence >= 70,
    }


def calculate_recommendation_intelligence(item: ItemRecord, *, quality: Dict, type_info: Dict, price_info: Dict, stock_info: Dict, model_identity: Dict) -> Dict:
    score = 0.0
    tags: List[str] = []
    reasons: List[str] = []

    score += quality.get("score", 0) * 0.42
    score += price_info.get("priceScore", 0) * 0.28
    score += stock_info.get("stockConfidence", 0) * 0.18
    score += max(0, min(100, price_info.get("rarityScore", 0))) * 0.12

    if type_info.get("isGunplaMainItem"):
        score += 6
        tags.append("main_kit")
    if price_info.get("isLowestPrice"):
        score += 8
        tags.append("lowest_price")
    if price_info.get("priceState") == "good_deal":
        score += 5
        tags.append("good_deal")
    if stock_info.get("stockState") == "restocked":
        score += 8
        tags.append("restocked")
    if stock_info.get("stockState") == "in_stock":
        tags.append("in_stock")
    if quality.get("score", 0) >= 80:
        tags.append("high_quality")
    if price_info.get("rarityScore", 0) >= 65:
        tags.append("rare_or_low_supply")

    if type_info.get("typeCategory") not in {"gunpla", "gundam_related"}:
        score -= 25
        reasons.append("not_main_gunpla")
    if quality.get("score", 0) < 60:
        score -= 20
        reasons.append("low_quality")
    if stock_info.get("stockState") in {"sold_out", "unknown"}:
        score -= 8
        reasons.append("not_available_now")

    if price_info.get("priceState") in {"lowest", "good_deal"}:
        reasons.append("price_attractive")
    if stock_info.get("stockConfidence", 0) >= 75:
        reasons.append("stock_reliable")
    if model_identity.get("series") and model_identity.get("series") != "UNKNOWN":
        reasons.append("series_detected")

    recommendation_score = int(round(max(0, min(100, score))))
    if recommendation_score >= 82:
        level = "strong_recommend"
    elif recommendation_score >= 68:
        level = "recommend"
    elif recommendation_score >= 50:
        level = "normal"
    else:
        level = "review_first"

    return {
        "recommendationScore": recommendation_score,
        "recommendationLevel": level,
        "recommendationTags": sorted(set(tags))[:20],
        "recommendationReasons": sorted(set(reasons))[:20],
    }

def to_firestore_doc(item: ItemRecord, previous: Optional[Dict] = None) -> Dict:
    price_int = price_to_int(item.price)
    display_name = apply_official_product_name(item.name or item.title) or item.name
    official_hit = lookup_official_product_name(item.name or item.title) or lookup_official_product_name(display_name)
    product_key = normalize_product_key(display_name)
    grade = extract_grade(display_name)
    confidence = calculate_match_confidence_for_name(display_name, item.price, item.status)
    quality = calculate_quality_score(item, display_name=display_name, official_hit=official_hit, confidence=confidence)
    type_info = classify_product_type(display_name or item.name or item.title, price=item.price)
    model_identity = build_model_identity(display_name or item.name or item.title, official_hit=official_hit)
    price_info = calculate_price_intelligence(item, model_identity)
    stock_info = calculate_stock_intelligence(item, previous, price_info=price_info, quality=quality)
    recommendation_info = calculate_recommendation_intelligence(
        item,
        quality=quality,
        type_info=type_info,
        price_info=price_info,
        stock_info=stock_info,
        model_identity=model_identity,
    )
    image_info = calculate_image_quality(item, quality=quality, type_info=type_info)

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

    return {
        "name": display_name,
        "title": display_name,
        "displayName": display_name,
        "officialName": official_hit.get("officialName", display_name) if official_hit else display_name,
        "officialMatched": official_hit is not None,
        "officialProductKey": official_hit.get("key", "") if official_hit else "",
        "officialSeries": official_hit.get("series", "") if official_hit else "",
        "normalizedName": product_key,
        "rawName": item.name,
        "rawTitle": item.title,
        "price": item.price,
        "priceInt": price_int,
        "grade": grade,
        "productKey": product_key,
        "matchConfidence": confidence["score"],
        "matchConfidenceReasons": confidence["reasons"],
        "canonicalCore": confidence["core"],
        "masterModelId": model_identity["masterModelId"],
        "modelGroupKey": model_identity["modelGroupKey"],
        "modelCoreName": model_identity["modelCoreName"],
        "modelDisplayName": model_identity["modelDisplayName"],
        "series": model_identity["series"],
        "scale": model_identity["scale"],
        "brand": model_identity["brand"],
        "typeCategory": type_info["typeCategory"],
        "subType": type_info["subType"],
        "typeConfidence": type_info["typeConfidence"],
        "typeFlags": type_info["typeFlags"],
        "isGunplaMainItem": type_info["isGunplaMainItem"],
        "needsReview": confidence["score"] < 0.72 or quality["score"] < 60 or type_info["typeCategory"] not in {"gunpla", "gundam_related"},
        "qualityScore": quality["score"],
        "qualityGrade": quality["grade"],
        "qualityReasons": quality["reasons"],
        "qualityFlags": quality["flags"],
        "isQualityLow": quality["score"] < 60,
        "isQualityHigh": quality["score"] >= 80,
        "marketLowestPrice": price_info["marketLowestPrice"],
        "marketHighestPrice": price_info["marketHighestPrice"],
        "marketAveragePrice": price_info["marketAveragePrice"],
        "marketMedianPrice": price_info["marketMedianPrice"],
        "marketPriceCount": price_info["marketPriceCount"],
        "sellerCountForModel": price_info["sellerCountForModel"],
        "availableSellerCount": price_info["availableSellerCount"],
        "inStockSellerCount": price_info["inStockSellerCount"],
        "preorderSellerCount": price_info["preorderSellerCount"],
        "isLowestPrice": price_info["isLowestPrice"],
        "priceScore": price_info["priceScore"],
        "priceState": price_info["priceState"],
        "discountRateVsAverage": price_info["discountRateVsAverage"],
        "modelSellers": price_info["modelSellers"],
        "rarityScore": price_info["rarityScore"],
        "stockState": stock_info["stockState"],
        "stockConfidence": stock_info["stockConfidence"],
        "restockCount": stock_info["restockCount"],
        "wasPreviouslyAvailable": stock_info["wasPreviouslyAvailable"],
        "isCurrentlyAvailable": stock_info["isCurrentlyAvailable"],
        "isReliableRestock": stock_info["isReliableRestock"],
        "recommendationScore": recommendation_info["recommendationScore"],
        "recommendationLevel": recommendation_info["recommendationLevel"],
        "recommendationTags": recommendation_info["recommendationTags"],
        "recommendationReasons": recommendation_info["recommendationReasons"],
        "imageUrl": image_info["imageUrl"],
        "imageSource": image_info["imageSource"],
        "hasImage": image_info["hasImage"],
        "imageQualityScore": image_info["imageQualityScore"],
        "imageQualityGrade": image_info["imageQualityGrade"],
        "imageQualityReasons": image_info["imageQualityReasons"],
        "imageQualityFlags": image_info["imageQualityFlags"],
        "isImageQualityLow": image_info["isImageQualityLow"],
        "isImageQualityHigh": image_info["isImageQualityHigh"],
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


def build_match_report(items: List[ItemRecord], path: str = MATCH_REPORT_PATH) -> Dict:
    groups: Dict[str, List[ItemRecord]] = {}
    for item in items:
        key = normalize_product_key(item.name or item.title)
        groups.setdefault(key, []).append(item)

    suspicious_groups: List[Dict] = []
    low_confidence_items: List[Dict] = []
    official_unmatched_items: List[Dict] = []
    alias_candidates: List[Dict] = []
    blocked_warnings: List[Dict] = []

    for key, group in groups.items():
        names = sorted({standardize_product_name(i.name or i.title) or i.name for i in group})
        sellers = sorted({i.mall_name for i in group})
        prices = [price_to_int(i.price) for i in group if price_to_int(i.price) is not None]
        confidences = [calculate_match_confidence_for_name(i.name or i.title, i.price, i.status)["score"] for i in group]
        min_conf = min(confidences) if confidences else 0
        max_price = max(prices) if prices else None
        min_price = min(prices) if prices else None
        price_spread_ratio = round(max_price / min_price, 2) if min_price and max_price and min_price > 0 else None

        blocked_pairs = []
        for idx, left in enumerate(names):
            for right in names[idx + 1:]:
                if is_blocked_match(left, right):
                    blocked_pairs.append([left, right])
                    blocked_warnings.append({"productKey": key, "left": left, "right": right, "sellers": sellers})

        reasons = []
        if key.endswith("|"):
            reasons.append("empty_product_key")
        if min_conf < 0.72:
            reasons.append("low_confidence")
        if blocked_pairs:
            reasons.append("blocked_match_violation")
        if price_spread_ratio is not None and price_spread_ratio >= 3.0 and len(group) >= 2:
            reasons.append("price_spread_large")

        if reasons:
            suspicious_groups.append({
                "productKey": key,
                "count": len(group),
                "sellers": sellers,
                "names": names[:12],
                "minConfidence": min_conf,
                "priceRange": [min_price, max_price],
                "priceSpreadRatio": price_spread_ratio,
                "blockedPairs": blocked_pairs,
                "reasons": reasons,
            })

        for item in group:
            original_name = item.name or item.title
            c = calculate_match_confidence_for_name(original_name, item.price, item.status)
            if c["score"] < 0.72:
                low_confidence_items.append({
                    "name": item.name,
                    "mallName": item.mall_name,
                    "price": item.price,
                    "status": item.status,
                    "productKey": c["productKey"],
                    "confidence": c["score"],
                    "reasons": c["reasons"],
                    "url": item.url,
                })
            if not lookup_official_product_name(original_name):
                official_unmatched_items.append({
                    "name": item.name,
                    "standardizedName": standardize_product_name(original_name),
                    "mallName": item.mall_name,
                    "grade": extract_grade(original_name),
                    "price": item.price,
                    "url": item.url,
                    "suggestedOfficialKey": normalize_product_key(original_name),
                })

    by_core: Dict[str, List[str]] = {}
    for key in groups.keys():
        parts = key.split("|", 1)
        core = parts[1] if len(parts) > 1 else key
        if core:
            by_core.setdefault(core, []).append(key)
    for core, keys in by_core.items():
        unique_keys = sorted(set(keys))
        if len(unique_keys) >= 2:
            alias_candidates.append({"canonicalCore": core, "productKeys": unique_keys, "reason": "same_core_different_grade_or_key"})

    report = {
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "totalItems": len(items),
        "totalGroups": len(groups),
        "suspiciousGroupCount": len(suspicious_groups),
        "lowConfidenceItemCount": len(low_confidence_items),
        "officialUnmatchedItemCount": len(official_unmatched_items),
        "aliasCandidateCount": len(alias_candidates),
        "blockedWarningCount": len(blocked_warnings),
        "suspiciousGroups": suspicious_groups[:200],
        "lowConfidenceItems": low_confidence_items[:300],
        "officialUnmatchedItems": official_unmatched_items[:300],
        "aliasCandidates": alias_candidates[:200],
        "blockedWarnings": blocked_warnings[:200],
        "howToUse": {
            "official_products.json": "공식명으로 확정하고 싶은 상품을 추가하세요. officialName이 앱 표시명으로 우선 적용됩니다.",
            "aliases.json": "같은 상품으로 강제 통일하고 싶은 이름을 추가하세요.",
            "blocked_matches.json": "절대 같은 상품으로 묶으면 안 되는 조합을 추가하세요.",
            "matchConfidence": "0.72 미만이면 검토 권장, official_master reason이 있으면 공식명 DB로 매칭된 것입니다."
        }
    }
    try:
        Path(path).write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[매칭 리포트 저장] {path} / 의심 그룹 {len(suspicious_groups)}개 / 공식명 미매칭 {len(official_unmatched_items)}개")
    except Exception as e:
        print(f"[매칭 리포트 저장 실패] {path} / {type(e).__name__}: {e}")
    return report

def main():
    print("=== KR 통합 크롤링 시작 ===")
    print("[패치 버전] board_noise_filter_v1")
    print("[패치 버전] joyhobby_filter_final_v2 / official_master 유지 / health 차단 완화")
    print("[패치 버전] low_count_upload_guard_v1 / 50개 덮어쓰기 방지")
    print("[패치 버전] quality_score_v1 / Firestore qualityScore 추가")
    print("[패치 버전] product_type_classifier_v2 / typeCategory·subType 추가")
    print("[패치 버전] model_identity_engine_v3 / masterModelId·modelGroupKey 추가")
    print("[패치 버전] data_engine_stage4_6 / 가격·재고·추천 분석 필드 추가")
    print("[패치 버전] ci_secret_and_optional_selenium_guard_v1 / CI 인증·선택 의존성 안정화")
    print("[패치 버전] image_quality_v1 / 대표 이미지 URL·이미지 품질 점수 추가")
    print(f"[실행 모드] FAST_TEST_MODE={FAST_TEST_MODE} / MAX_LINKS_PER_SITE={MAX_LINKS_PER_SITE}")
    print(f"[업로드 보호] MIN_TOTAL_UPLOAD={MIN_TOTAL_UPLOAD} / ABSOLUTE_MIN_UPLOAD={ABSOLUTE_MIN_UPLOAD} / FORCE_UPLOAD_LOW_COUNT={FORCE_UPLOAD_LOW_COUNT} / ALLOW_TEST_UPLOAD={ALLOW_TEST_UPLOAD}")
    load_manual_aliases()
    load_blocked_matches()
    load_official_products()

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

    build_data_engine_context(merged)

    build_match_report(merged)
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