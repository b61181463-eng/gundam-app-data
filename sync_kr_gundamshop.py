import hashlib
import re
from urllib.parse import urlparse, parse_qs

import requests
from bs4 import BeautifulSoup
import firebase_admin
from firebase_admin import credentials, firestore
from firebase_admin.firestore import ArrayUnion

SERVICE_ACCOUNT_PATH = "serviceAccountKey.json"

LIST_URLS = [
    "https://www.gundamshop.co.kr/",
    "https://www.gundamshop.co.kr/theme/Reserve.html?cate=0001&ordr=wol_panme&sort=DESC",
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/134.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8",
}

MAX_ITEMS = 150

ALLOWED_KEYWORDS = [
    "mgsd", "mgex", "pg", "mg", "rg", "hg", "sd", "bb", "eg", "re/100",
    "full mechanics", "figure-rise", "figure rise", "30ms", "30mm",
    "건담", "자쿠", "유니콘", "프리덤", "스트라이크", "에어리얼",
    "즈고크", "사자비", "뉴건담", "시난주", "mk ii", "mk-ii",
    "rx 78", "rx-78", "건캐논", "건탱크", "돔", "겔구그", "구프",
    "바르바토스", "캘리번", "데미", "루브리스", "저스티스", "데스티니",
    "엑시아", "더블오", "아스트레이", "톨기스", "윙건담", "짐",
    "짐 스나이퍼", "풀아머", "더블 제타", "더블제타", "알트리아",
    "건담데칼", "bb전사", "sdw", "삼국창걸전",
]

BLOCKED_KEYWORDS = [
    "프라모델", "건프라", "모형",
    "티셔츠", "후드", "의류", "의상",
    "머그컵", "텀블러", "보틀", "컵",
    "키링", "열쇠고리", "아크릴", "스탠드", "스티커",
    "포스터", "브로마이드", "엽서",
    "노트", "문구", "필통", "펜", "볼펜",
    "쿠션", "담요", "타월", "수건",
    "케이스", "파우치", "가방",
    "과자", "식품", "음료",
    "잡지", "도서", "책",
    "카드", "트레이딩 카드",
    "넨도로이드", "봉제", "인형",
    "유의사항", "공지", "안내", "이벤트", "출처",
    "문의", "게시판", "운영시간", "영업시간",
    "로그인", "회원가입", "장바구니", "주문조회", "고객센터",
    "regular", "sale price", "sale price now", "price now", "copyright",
    "zagtoon", "method", "samg", "toei animation", "level-5", "hisago amazake-no",
    "예약중인", "예약상품", "예약 상품", "건담샵에서 예약중인",
    "건담샵에서 예약중인 예약상품",
    "상품보기", "더보기", "바로가기", "상세보기", "옵션선택", "선택하기",
    "www", "http", "https", ".com", ".kr",
]

BROKEN_PATTERNS = [
    "�", "Ã", "Â", "°ç", "´ã", "¿", "½", "À", "Ã¬", "Ã¥",
    "¼", "¹", "º", "³", "²", "¾", "Ð", "Ñ", "Õ", "Ö",
]

GENERIC_BLOCK_EXACT = {
    "프라모델",
    "건프라",
    "모형",
    "건담 프라모델",
    "건담프라모델",
    "프라모델 키트",
    "건담 키트",
    "mgsd",
    "mgex",
    "hg",
    "mg",
    "rg",
    "pg",
    "sd",
    "eg",
    "bb",
    "re/100",
    "30ms",
    "30mm",
    "regular",
    "sale price now",
    "sale price",
    "price now",
}

STOPWORDS = [
    "1", "144", "100", "60", "scale", "model", "kit",
    "건담베이스", "건담샵", "루리웹",
]

BAD_EXACT_NAMES = {
    "26.95 regular",
    "16.95 sale price now",
    "sale price now",
    "sale price",
    "regular",
    "건담샵에서 예약중인 예약상품 - 건담샵",
    "건담샵에서 예약중인 예약상품",
    "[hg]1/144 ¼¼¹óºñ gnhw/b",
}

LINE_BLOCK_PATTERNS = [
    r"유의사항",
    r"출처",
    r"공지",
    r"안내",
    r"이벤트",
    r"문의",
    r"게시판",
    r"운영시간",
    r"영업시간",
    r"고객센터",
    r"이용약관",
    r"개인정보",
    r"청소년보호정책",
    r"copyright",
    r"예약\s*안내",
    r"입고\s*안내",
    r"회원",
    r"로그인",
    r"장바구니\s*보기",
    r"주문조회",
    r"sale\s*price",
    r"price\s*now",
    r"regular",
    r"ⓒ",
    r"©",
    r"toei\s*animation",
    r"level-5",
    r"zagtoon",
    r"samg",
    r"예약중인",
    r"예약상품",
    r"건담샵에서\s*예약중인",
]

SENTENCE_PATTERNS = [
    r"올라오고\s*있습니다",
    r"확인.*부탁",
    r"확인.*바랍니다",
    r"참고.*바랍니다",
    r"판매.*진행",
    r"매장.*상이",
    r"점포.*상이",
    r"구매.*가능",
    r"방문.*부탁",
    r"공지.*확인",
    r"예정입니다",
    r"부탁드립니다",
    r"확인해주세요",
    r"진행됩니다",
    r"안내드립니다",
    r"안내됩니다",
    r"판매됩니다",
    r"판매중입니다",
    r"준비중입니다",
    r"건담샵에서.*예약중",
    r"예약중인.*예약상품",
    r"sale\s*price",
    r"price\s*now",
]

DETAIL_PARAM_HINTS = {
    "it_id", "goodsno", "goods_no", "product_no", "item_no", "no", "idx", "id"
}

DETAIL_PATH_HINTS = [
    "/shop/item.php",
    "/shop/item.view.php",
    "/shop/goods_view.php",
    "/goods/view",
    "/product/view",
    "/item/view",
    "/shop/detail",
]


def sha1(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


def normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def normalize_line_breaks(text: str) -> str:
    return (
        (text or "")
        .replace("\r", "\n")
        .replace("\u00a0", " ")
        .replace("⠀", " ")
        .replace("\t", " ")
    )


def fix_broken_korean(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\ufffd", " ")
    text = text.replace("�", " ")
    text = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", " ", text)
    return normalize_line_breaks(text)


def fetch_html(url: str) -> str:
    res = requests.get(url, headers=HEADERS, timeout=20)
    res.raise_for_status()

    candidates = []
    if res.encoding:
        candidates.append(res.encoding)
    if res.apparent_encoding:
        candidates.append(res.apparent_encoding)
    candidates.extend(["utf-8", "cp949", "euc-kr"])

    tried = set()

    for enc in candidates:
        if not enc:
            continue
        enc_lower = enc.lower()
        if enc_lower in tried:
            continue
        tried.add(enc_lower)

        try:
            text = res.content.decode(enc, errors="replace")
            text = fix_broken_korean(text)
            korean_chars = len(re.findall(r"[가-힣]", text))
            broken_chars = text.count("�")
            if korean_chars >= 10 and broken_chars < 5:
                return text
        except Exception:
            continue

    return fix_broken_korean(res.text)


def init_firestore():
    if firebase_admin is None or credentials is None or firestore is None:
        print("[경고] firebase_admin 미설치 - Firestore 저장 생략")
        return None

    try:
        from pathlib import Path

        if not Path(SERVICE_ACCOUNT_PATH).exists():
            print(f"[경고] {SERVICE_ACCOUNT_PATH} 없음 - Firestore 저장 생략")
            return None

        if not firebase_admin._apps:
            cred = credentials.Certificate(SERVICE_ACCOUNT_PATH)
            firebase_admin.initialize_app(cred)

        return firestore.client()
    except Exception as e:
        print(f"[경고] Firestore 초기화 건너뜀: {e}")
        return None


def normalize_name(name: str) -> str:
    text = (name or "").lower().strip()
    text = text.replace("ver.", "ver")
    text = text.replace("version", "ver")
    text = re.sub(r"\b1/\d+\b", "", text)
    text = re.sub(r"\b\d+/\d+\b", "", text)
    text = re.sub(r"[\[\]\(\)\-_/.,:+]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def tokenize_name(name: str) -> list:
    text = normalize_name(name)
    tokens = text.split()

    result = []
    for token in tokens:
        if token in STOPWORDS:
            continue
        if len(token) <= 1:
            continue
        result.append(token)

    return result


def looks_broken_text(text: str) -> bool:
    if not text:
        return False

    lower = text.lower()

    if any(p.lower() in lower for p in BROKEN_PATTERNS):
        return True

    # 깨진 한글 인코딩 흔적이 일정 개수 이상 있으면 차단
    broken_char_count = sum(lower.count(ch.lower()) for ch in ["¼", "¹", "º", "³", "²", "¾"])
    if broken_char_count >= 2:
        return True

    # 영문/숫자/기본 기호 외 이상한 조합인데 건담 키워드도 없으면 차단
    if re.search(r"[¼¹º³²¾ÐÑÕÖ]", text):
        return True

    return False


def is_too_generic_product_name(name: str) -> bool:
    text = normalize_name(name)
    if not text:
        return True

    if text in GENERIC_BLOCK_EXACT:
        return True

    if "프라모델" in text:
        return True

    tokens = tokenize_name(name)
    if len(tokens) <= 1:
        return True

    return False


def is_probable_product_href(href: str) -> bool:
    if not href:
        return False

    lower = href.lower().strip()

    if lower.startswith("javascript:") or lower.startswith("mailto:") or lower.startswith("#"):
        return False

    # ❌ 무조건 차단
    blocked_bits = [
        "/bbs/",
        "/board/",
        "/search",
        "/member/",
        "/mypage/",
        "/cart",
        "/login",
        "/join",
        "/theme/",
        "/category",
        "cate=",
        "sort=",
        "ordr=",
        "list=",
    ]
    if any(bit in lower for bit in blocked_bits):
        return False

    # ✅ 경로 자체가 상세페이지인 경우만 허용
    allowed_path_bits = [
        "/shop/item.php",
        "/shop/item.view.php",
        "/shop/goods_view.php",
        "/goods/view",
        "/product/view",
        "/item/view",
        "/shop/detail",
    ]
    if any(bit in lower for bit in allowed_path_bits):
        parsed = urlparse(lower)
        query = parse_qs(parsed.query)

        detail_keys = {"it_id", "goodsno", "goods_no", "product_no", "item_no"}
        if any(key in query for key in detail_keys):
            return True

    # 숫자형 상세 패턴만 허용
    if re.search(r"/product/\d+", lower):
        return True

    if re.search(r"/item/\d+", lower):
        return True

    return False


def absolute_url(href: str) -> str:
    if not href:
        return ""
    if href.startswith("http://") or href.startswith("https://"):
        return href
    if href.startswith("//"):
        return "https:" + href
    if href.startswith("/"):
        return "https://www.gundamshop.co.kr" + href
    return "https://www.gundamshop.co.kr/" + href.lstrip("/")


def detect_stock_text(text: str) -> str:
    t = normalize_space(text).lower()

    sold_out_keywords = [
        "품절",
        "일시품절",
        "sold out",
        "out of stock",
        "재고없음",
        "재고 없음",
    ]
    if any(k in t for k in sold_out_keywords):
        return "품절"

    reserve_keywords = [
        "예약",
        "예약중",
        "예약 판매",
        "입고예정",
        "입고 예정",
        "pre-order",
        "preorder",
    ]
    if any(k in t for k in reserve_keywords):
        return "예약/입고예정"

    selling_keywords = [
        "판매중",
        "장바구니",
        "구매",
        "구매하기",
        "바로구매",
        "buy now",
        "add to cart",
    ]
    if any(k in t for k in selling_keywords):
        return "판매중"

    return "상태 확인 필요"

def extract_price(text: str) -> str:
    m = re.search(r"([\d,]+)\s*원", text)
    return f"{m.group(1)}원" if m else ""


def cleanup_candidate_name(text: str) -> str:
    name = normalize_space(text)

    name = re.sub(r"^20\d{2}\s*년\s*\d{1,2}\s*월\s*\d{1,2}\s*일?\s*입고\s*", "", name)
    name = re.sub(r"^20\d{2}[.\-/]\d{1,2}[.\-/]\d{1,2}\s*입고\s*", "", name)
    name = re.sub(r"^\d{4}\s*년\s*\d{1,2}\s*월\s*\d{1,2}\s*일?\s*입고\s*", "", name)
    name = re.sub(r"^\d{4}\s*년\s*\d{1,2}\s*월\d{1,2}\s*입고\s*", "", name)
    name = re.sub(r"^\d{4}\.\d{1,2}\.\d{1,2}\s*입고\s*", "", name)
    name = re.sub(r"^\d{4}-\d{1,2}-\d{1,2}\s*입고\s*", "", name)
    name = re.sub(r"^\d{4}/\d{1,2}/\d{1,2}\s*입고\s*", "", name)
    name = re.sub(r"^입고\s*", "", name)

    name = re.sub(r"\[\d+\]\s*$", "", name).strip()
    name = re.sub(r"[\d,]+\s*원.*$", "", name).strip()
    name = re.sub(r"^No\.\d+\s*", "", name).strip()
    name = re.sub(r"^[✔✓•●·\-\*\[\]▶▷☞]+", "", name).strip()

    name = re.sub(r"\(.*?(한정|제한|별매|안내|예약).*?\)", "", name)
    name = re.sub(r"\[.*?(한정|제한|별매|안내|예약).*?\]", "", name)
    name = re.sub(
        r"^\s*[\d.,]+\s*(regular|sale price now|sale price|price now)\s*$",
        "",
        name,
        flags=re.IGNORECASE,
    )

    name = re.sub(r"\s{2,}", " ", name).strip()
    return normalize_space(name)


def line_looks_like_product(line: str) -> bool:
    text = normalize_space(line)
    lower = text.lower()

    if not text:
        return False

    if text.lower() in BAD_EXACT_NAMES:
        return False

    if "ⓒ" in text or "©" in text:
        return False

    if len(text) < 4 or len(text) > 100:
        return False

    if looks_broken_text(text):
        return False

    if re.fullmatch(r"[\d.,]+\s*(regular|sale price now|sale price|price now)", lower):
        return False

    if re.search(r"[\d.,]+\s*(regular|sale price|price now)", lower):
        return False

    if re.fullmatch(r"[a-z0-9\s.,/\-]+", lower):
        if not any(ok in lower for ok in ["gundam", "hg", "mg", "rg", "pg", "sd", "mgsd", "mgex"]):
            return False

    if any(block in lower for block in BLOCKED_KEYWORDS):
        return False

    for pattern in LINE_BLOCK_PATTERNS:
        if re.search(pattern, text, re.IGNORECASE):
            return False

    for pattern in SENTENCE_PATTERNS:
        if re.search(pattern, text, re.IGNORECASE):
            return False

    if text.endswith((
        "합니다",
        "합니다.",
        "있습니다",
        "있습니다.",
        "드립니다",
        "드립니다.",
        "바랍니다",
        "바랍니다.",
        "입니다",
        "입니다.",
        "됩니다",
        "됩니다.",
    )):
        return False

    if is_too_generic_product_name(text):
        return False

    if text.count(" ") >= 9:
        return False

    grade_keywords = ["mgsd", "mgex", "hg", "mg", "rg", "pg", "sd", "bb", "eg", "re/100", "30ms", "30mm"]
    model_name_keywords = [
        "건담", "에어리얼", "자쿠", "유니콘", "프리덤", "스트라이크",
        "즈고크", "사자비", "뉴건담", "시난주", "엑시아",
        "바르바토스", "캘리번", "루브리스", "데스티니",
        "저스티스", "아스트레이", "더블오", "톨기스",
        "윙건담", "짐", "짐 스나이퍼", "건캐논", "건탱크",
        "더블 제타", "더블제타", "건담 mk", "mk-ii", "알트리아",
        "bb전사", "삼국창걸전",
    ]

    has_grade = any(k in lower for k in grade_keywords)
    has_model_code = bool(re.search(r"\b(rx|msn|zgmf|gnt|gn|xxxg|oz|mbf)[- ]?\d", lower))
    has_specific_name = any(k.lower() in lower for k in model_name_keywords)

    if not (has_grade or has_model_code or has_specific_name):
        return False

    return True


def is_gundam_product_name(name: str) -> bool:
    text = normalize_space(name).lower()
    if not text:
        return False

    if text in BAD_EXACT_NAMES:
        return False

    if "ⓒ" in text or "©" in text:
        return False

    if looks_broken_text(text):
        return False

    if "프라모델" in text:
        return False

    if any(k in text for k in BLOCKED_KEYWORDS):
        return False

    if is_too_generic_product_name(name):
        return False

    if len(text) < 4 or len(text) > 100:
        return False

    if any(k in text for k in ALLOWED_KEYWORDS):
        return True

    return False


def find_parent_card(a):
    return a.find_parent(["li", "div", "dd", "dl"])


def pick_best_name_from_anchor(a) -> str:
    candidates = []

    selectors = [
        ".item_name", ".prd_name", ".goods_name", ".product_name",
        ".name", ".tit", ".title", "strong", "b", "h3", "h4",
    ]
    for selector in selectors:
        for node in a.select(selector):
            text = normalize_space(node.get_text(" ", strip=True))
            if text:
                candidates.append(text)

    direct_text = normalize_space(a.get_text(" ", strip=True))
    if direct_text:
        candidates.append(direct_text)

    parent = find_parent_card(a)
    if parent:
        for selector in selectors:
            node = parent.select_one(selector)
            if node:
                text = normalize_space(node.get_text(" ", strip=True))
                if text:
                    candidates.append(text)

    for text in candidates:
        cleaned = cleanup_candidate_name(text)
        if not cleaned:
            continue
        if len(cleaned) < 4:
            continue
        if line_looks_like_product(cleaned) and is_gundam_product_name(cleaned):
            return cleaned

    return ""


def pick_stock_text_from_anchor(a) -> str:
    texts = []

    direct = normalize_space(a.get_text(" ", strip=True))
    if direct:
        texts.append(direct)

    parent = find_parent_card(a)
    if parent:
        parent_text = normalize_space(parent.get_text(" ", strip=True))
        if parent_text:
            texts.append(parent_text)

    merged = " ".join(texts)
    return detect_stock_text(merged)


def pick_price_from_anchor(a) -> str:
    texts = []

    direct = normalize_space(a.get_text(" ", strip=True))
    if direct:
        texts.append(direct)

    parent = find_parent_card(a)
    if parent:
        price_selectors = [".price", ".cost", ".amount", ".money", ".won", "strong"]
        for selector in price_selectors:
            for node in parent.select(selector):
                text = normalize_space(node.get_text(" ", strip=True))
                if text:
                    texts.append(text)

        parent_text = normalize_space(parent.get_text(" ", strip=True))
        if parent_text:
            texts.append(parent_text)

    return extract_price(" ".join(texts))


def candidate_blocks_from_html(html: str):
    soup = BeautifulSoup(html, "html.parser")
    blocks = []
    seen = set()

    for a in soup.find_all("a", href=True):
        href = absolute_url(a.get("href", ""))
        if not href:
            continue

        name = pick_best_name_from_anchor(a)
        if not name:
            continue

        key = f"{href}|{normalize_name(name)}"
        if key in seen:
            continue
        seen.add(key)

        blocks.append({
            "url": href,
            "text": name,
            "stockText": pick_stock_text_from_anchor(a),
            "price": pick_price_from_anchor(a),
        })
    print(f"[DEBUG] 링크 발견: {href} / 텍스트: {a.get_text(strip=True)}")
    return blocks


def extract_products_from_listing(url: str):
    html = fetch_html(url)
    blocks = candidate_blocks_from_html(html)

    items = []
    seen = set()

    for block in blocks:
        name = cleanup_candidate_name(block["text"])
        href = block["url"]
        price = block.get("price", "")
        stock_text = block.get("stockText", "")

        lower_href = href.lower().strip()

        # 현재 목록 페이지나 theme 예약 페이지로 가는 링크는 무조건 차단
        if href.strip() == url.strip():
            continue

        if "/theme/reserve.html" in lower_href:
            continue

        if "/theme/" in lower_href:
            continue

        if "cate=" in lower_href or "sort=" in lower_href or "ordr=" in lower_href:
            continue

        if not name or len(name) < 4:
            continue

        if name.lower() in BAD_EXACT_NAMES:
            continue

        if len(name) < 3:
            continue

        if not href:
            continue

        if not stock_text:
            stock_text = "상태 확인 필요"

        key = f"{href}|{normalize_name(name)}"
        if key in seen:
            continue
        seen.add(key)

        print(f"후보: {name} -> {href} / 상태: {stock_text}")

        items.append({
            "name": name,
            "title": name,
            "price": price,
            "stockText": stock_text,
            "url": href,
            "sourcePage": url,
        })
    print(f"[DEBUG] 최종 items 개수: {len(items)}")
    return items


def determine_change(existing, new_data):
    if existing is None:
        return "notice_added"

    prev_status = existing.get("status")
    next_status = new_data.get("status")

    if prev_status != next_status:
        if prev_status == "품절" and next_status == "판매중":
            return "restocked"
        if prev_status == "판매중" and next_status == "품절":
            return "sold_out"
        return "status_changed"

    if existing.get("price") != new_data.get("price"):
        return "status_changed"

    return None


def add_event(db, item_id, name, change_type):
    db.collection("stock_events").add({
        "itemId": item_id,
        "name": name,
        "changeType": change_type,
        "createdAt": firestore.SERVER_TIMESTAMP,
    })


def update_verification_fields(doc_ref):
    latest_snap = doc_ref.get()
    if not latest_snap.exists:
        return

    latest = latest_snap.to_dict() or {}
    sources = latest.get("verificationSources", [])
    count = len(sources)

    status = "single_source"
    if count >= 2:
        status = "cross_checked"

    doc_ref.set({
        "verificationCount": count,
        "verificationStatus": status,
    }, merge=True)


def save_item(db, item):
    item_id = sha1(item["url"] + "|" + item["name"])
    canonical_name = normalize_name(item["name"])

    doc_ref = db.collection("aggregated_items").document(item_id)
    existing_snap = doc_ref.get()
    existing = existing_snap.to_dict() if existing_snap.exists else None

    data = {
        "itemId": item_id,
        "name": item["name"],
        "title": item["title"],
        "canonicalName": canonical_name,
        "price": item["price"],
        "stockText": item["stockText"],
        "status": item["stockText"],
        "source": "gundamshop",
        "sourceType": "product",
        "site": "건담샵",
        "mallName": "건담샵",
        "country": "KR",
        "region": "KR",
        "productUrl": item["url"],
        "url": item["url"],
        "sourcePage": item["sourcePage"],
        "verificationSources": ArrayUnion(["gundamshop"]),
        "updatedAt": firestore.SERVER_TIMESTAMP,
        "lastSeenAt": firestore.SERVER_TIMESTAMP,
    }

    change_type = determine_change(existing, data)
    if change_type:
        data["changeType"] = change_type
        data["lastChangedAt"] = firestore.SERVER_TIMESTAMP
        add_event(db, item_id, item["name"], change_type)
    elif existing and existing.get("changeType"):
        data["changeType"] = existing.get("changeType")

    doc_ref.set(data, merge=True)
    update_verification_fields(doc_ref)


def main():
    db = init_firestore()

    all_items = []
    for url in LIST_URLS:
        try:
            items = extract_products_from_listing(url)
            print(f"[목록] {url}")
            print(f"  후보 개수: {len(items)}")
            for item in items[:10]:
                print(
                    f"   - {item['name']} / {item['stockText']} / "
                    f"{item['price']} / {item['url']}"
                )
            all_items.extend(items)
        except Exception as e:
            print(f"[실패] {url} -> {e}")

    dedup = []
    seen = set()
    for item in all_items:
        key = item["url"] + "|" + normalize_name(item["name"])
        if key in seen:
            continue
        seen.add(key)
        dedup.append(item)

    print(f"최종 저장 대상 수: {len(dedup)}")

    saved = 0
    for item in dedup[:MAX_ITEMS]:
        try:
            save_item(db, item)
            saved += 1
            print(f"저장: {item['name']} / {item['stockText']} / {item['price']}")
        except Exception as e:
            print(f"저장 실패: {item['name']} -> {e}")

    print(f"[완료] 건담샵 완료 / 총 저장 {saved}개")


if __name__ == "__main__":
    main()
