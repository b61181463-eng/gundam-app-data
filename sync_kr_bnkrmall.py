import hashlib
import re

import requests
from bs4 import BeautifulSoup
import firebase_admin
from firebase_admin import credentials, firestore
from firebase_admin.firestore import ArrayUnion

SERVICE_ACCOUNT_PATH = "serviceAccountKey.json"

LIST_URLS = [
    "https://www.bnkrmall.co.kr/premium/p_category.do",
    "https://www.bnkrmall.co.kr/plan/p_content.do?idx=862",
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/134.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8",
}

ALLOWED_KEYWORDS = [
    "hg", "mg", "rg", "pg", "sd", "bb", "eg", "re/100",
    "full mechanics", "figure-rise", "figure rise", "30ms", "30mm",
    "건담", "자쿠", "유니콘", "프리덤", "스트라이크", "에어리얼",
    "즈고크", "사자비", "뉴건담", "시난주", "mk ii", "mk-ii",
    "rx 78", "rx-78", "건캐논", "건탱크", "돔", "겔구그", "구프",
    "바르바토스", "캘리번", "데미", "루브리스", "저스티스", "데스티니",
    "엑시아", "더블오", "아스트레이", "톨기스", "윙건담", "짐",
    "풀아머", "더블 제타", "더블제타", "알트리아", "건담데칼",
]

BLOCKED_KEYWORDS = [
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
    "www", "http", "https", ".com", ".kr",
]

GENERIC_BLOCK_EXACT = {
    "프라모델",
    "건프라",
    "건담 프라모델",
    "건담프라모델",
    "모형",
    "프라모델 키트",
    "건담 키트",
}

STOPWORDS = [
    "1", "144", "100", "60", "scale", "model", "kit",
    "건담베이스", "건담샵", "루리웹", "반다이남코코리아몰",
]

BAD_TEXT_PATTERNS = [
    "�", "Ã", "Â", "°ç", "´ã", "¿", "½",
]

MAX_ITEMS = 150


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
    if not firebase_admin._apps:
        cred = credentials.Certificate(SERVICE_ACCOUNT_PATH)
        firebase_admin.initialize_app(cred)
    return firestore.client()


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


def is_too_generic_product_name(name: str) -> bool:
    text = normalize_name(name)

    if not text:
        return True

    if text in GENERIC_BLOCK_EXACT:
        return True

    tokens = tokenize_name(name)
    if len(tokens) <= 1:
        return True

    return False


def looks_broken_text(text: str) -> bool:
    lower = (text or "").lower()
    return any(p.lower() in lower for p in BAD_TEXT_PATTERNS)


def is_gundam_product_name(name: str) -> bool:
    text = normalize_space(name).lower()
    if not text:
        return False

    if looks_broken_text(text):
        return False

    if any(k in text for k in BLOCKED_KEYWORDS):
        return False

    if is_too_generic_product_name(name):
        return False

    if len(text) < 4 or len(text) > 100:
        return False

    if text.endswith(("합니다", "있습니다", "드립니다", "바랍니다", "입니다", "됩니다")):
        return False

    if any(k in text for k in ALLOWED_KEYWORDS):
        return True

    return False


def absolute_url(href: str) -> str:
    if not href:
        return ""
    href = href.strip()
    if href.startswith("http://") or href.startswith("https://"):
        return href
    if href.startswith("//"):
        return "https:" + href
    if href.startswith("/"):
        return "https://www.bnkrmall.co.kr" + href
    return "https://www.bnkrmall.co.kr/" + href.lstrip("/")

def is_probable_product_href(href: str) -> bool:
    if not href:
        return False

    lower = href.lower().strip()

    if lower.startswith("javascript:") or lower.startswith("mailto:") or lower.startswith("#"):
        return False

    blocked_bits = [
        "p_category.do",
        "p_content.do",
        "/plan/",
        "/event",
        "/notice",
        "/board",
        "/search",
        "/login",
        "/cart",
        "/customer/",
        "/member/",
    ]
    if any(bit in lower for bit in blocked_bits):
        return False

    allowed_bits = [
        "product_no=",
        "goodsno=",
        "goods_no=",
        "item_no=",
        "itemno=",
        "/product/",
        "/goods/",
        "/item/",
        "/shop/",
    ]
    if any(bit in lower for bit in allowed_bits):
        return True

    # no=, idx= 는 너무 넓어서 반다이는 기본적으로 차단
    return False


def find_parent_card(a):
    return a.find_parent(["li", "div", "dd", "dl", "article"])


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
        if cleaned and is_gundam_product_name(cleaned):
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

    return detect_stock_text(" ".join(texts))


def pick_price_from_anchor(a) -> str:
    texts = []

    direct = normalize_space(a.get_text(" ", strip=True))
    if direct:
        texts.append(direct)

    parent = find_parent_card(a)
    if parent:
        for selector in [".price", ".cost", ".amount", ".money", ".won", "strong"]:
            for node in parent.select(selector):
                text = normalize_space(node.get_text(" ", strip=True))
                if text:
                    texts.append(text)

        parent_text = normalize_space(parent.get_text(" ", strip=True))
        if parent_text:
            texts.append(parent_text)

    return extract_price(" ".join(texts))


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

    selling_keywords = [
        "구매진행중",
        "상시구매진행중",
        "판매중",
        "구매하기",
        "바로구매",
        "장바구니",
        "주문하기",
        "buy now",
        "add to cart",
    ]
    if any(k in t for k in selling_keywords):
        return "판매중"

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

    return "상태 확인 필요"


def extract_price(text: str) -> str:
    m = re.search(r"([\d,]+)\s*원", text)
    return f"{m.group(1)}원" if m else ""


def cleanup_candidate_name(text: str) -> str:
    name = normalize_space(text)

    name = re.sub(r"[\d,]+\s*원.*$", "", name).strip()
    name = re.sub(r"\[프리미엄 반다이\]", "", name).strip()
    name = re.sub(r"\[타마시이 한정\]", "", name).strip()
    name = re.sub(r"^[✔✓•●·\-\*\[\]▶▷☞]+", "", name).strip()
    name = re.sub(r"\(.*?(한정|제한|별매|안내|예약).*?\)", "", name)
    name = re.sub(r"\[.*?(한정|제한|별매|안내|예약).*?\]", "", name)

    return normalize_space(name)


def extract_background_image(style_text: str) -> str:
    if not style_text:
        return ""

    m = re.search(r'background-image\s*:\s*url\((["\']?)(.*?)\1\)', style_text, re.IGNORECASE)
    if m:
        return absolute_url(m.group(2).strip())

    m = re.search(r'url\((["\']?)(.*?)\1\)', style_text, re.IGNORECASE)
    if m:
        return absolute_url(m.group(2).strip())

    return ""


def find_parent_card(a):
    return a.find_parent(["li", "div", "dd", "dl", "article"])


def pick_image_from_anchor(a) -> str:
    candidates = []

    # 1순위: a 내부 이미지
    for img in a.find_all("img"):
        for attr in ["data-src", "data-original", "data-lazy", "data-lazy-src", "src"]:
            src = normalize_space(img.get(attr))
            if src:
                candidates.append(src)

    parent = find_parent_card(a)

    # 2순위: 카드 내부 이미지
    if parent:
        for img in parent.find_all("img"):
            for attr in ["data-src", "data-original", "data-lazy", "data-lazy-src", "src"]:
                src = normalize_space(img.get(attr))
                if src:
                    candidates.append(src)

        # 3순위: background-image
        for tag in parent.find_all(True):
            style = normalize_space(tag.get("style"))
            if not style:
                continue
            bg = extract_background_image(style)
            if bg:
                candidates.append(bg)

    for src in candidates:
        lower = src.lower()
        if not src:
            continue
        if "blank" in lower or "noimage" in lower or "no_image" in lower:
            continue
        if lower.startswith("data:image"):
            continue
        return absolute_url(src)

    return ""


def candidate_blocks_from_html(html: str):
    soup = BeautifulSoup(html, "html.parser")
    blocks = []
    seen = set()

    for a in soup.find_all("a", href=True):
        href = absolute_url(a.get("href", ""))
        if not is_probable_product_href(href):
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
            "price": pick_price_from_anchor(a),
            "stockText": pick_stock_text_from_anchor(a),
        })

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

        # 목록 / 기획전 / 카테고리 링크 강제 차단
        if any(x in lower_href for x in ["p_category.do", "p_content.do", "/plan/"]):
            continue

        if not name or len(name) < 4:
            continue

        if not is_gundam_product_name(name):
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
            "mallName": "반다이남코코리아몰",
            "site": "반다이남코코리아몰",
        })

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
        "source": "bnkrmall",
        "sourceType": "product",
        "site": item["site"],
        "mallName": item["mallName"],
        "country": "KR",
        "region": "KR",
        "productUrl": item["url"],
        "url": item["url"],
        "imageUrl": item.get("imageUrl", ""),
        "sourcePage": item["sourcePage"],
        "verificationSources": ArrayUnion(["bnkrmall"]),
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
                    f"{item['price']} / {item['url']} / {item.get('imageUrl', '')}"
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
    print(f"이미지 있는 대상 수: {sum(1 for item in dedup if item.get('imageUrl'))}")

    saved = 0
    for item in dedup[:MAX_ITEMS]:
        try:
            save_item(db, item)
            saved += 1
            print(f"저장: {item['name']} / {item['stockText']} / {item['price']}")
        except Exception as e:
            print(f"저장 실패: {item['name']} -> {e}")

    print(f"[완료] 반다이남코코리아몰 완료 / 총 저장 {saved}개")


if __name__ == "__main__":
    main()