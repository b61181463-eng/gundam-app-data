import hashlib
import json
import re
from pathlib import Path
from urllib.parse import urljoin, urlparse, parse_qs

import requests
from bs4 import BeautifulSoup

try:
    import firebase_admin
    from firebase_admin import credentials, firestore
    from firebase_admin.firestore import ArrayUnion
except Exception:
    firebase_admin = None
    credentials = None
    firestore = None

    def ArrayUnion(values):
        return values


SERVICE_ACCOUNT_PATH = "serviceAccountKey.json"
OUTPUT_JSON_PATH = Path("data/gundambase_items.json")

LIST_URLS = [
    "https://www.thegundambase.com/",
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
    "copyright",
]

BAD_EXACT_NAMES = {
    "sale price now",
    "sale price",
    "price now",
    "regular",
}

BROKEN_PATTERNS = [
    "�", "Ã", "Â", "°ç", "´ã", "¿", "½", "À", "Ã¬", "Ã¥",
    "¼", "¹", "º", "³", "²", "¾", "Ð", "Ñ", "Õ", "Ö",
]


def sha1(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


def normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()


def normalize_line_breaks(text: str) -> str:
    return (
        str(text or "")
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
            if korean_chars >= 10 and broken_chars < 10:
                return text
        except Exception:
            continue

    return fix_broken_korean(res.text)


def init_firestore():
    if firebase_admin is None or credentials is None or firestore is None:
        print("[경고] firebase_admin 미설치 - Firestore 저장 생략")
        return None

    try:
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
    text = normalize_space(name).lower()
    text = text.replace("ver.", "ver")
    text = text.replace("version", "ver")
    text = re.sub(r"\b1/\d+\b", "", text)
    text = re.sub(r"\b\d+/\d+\b", "", text)
    text = re.sub(r"[\[\]\(\)\-_/.,:+]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def absolute_url(href: str) -> str:
    return urljoin("https://www.thegundambase.com", href or "")


def looks_broken_text(text: str) -> bool:
    if not text:
        return False
    lower = text.lower()
    if any(p.lower() in lower for p in BROKEN_PATTERNS):
        return True
    if re.search(r"[¼¹º³²¾ÐÑÕÖ]", text):
        return True
    return False


def cleanup_candidate_name(text: str) -> str:
    name = normalize_space(text)

    name = re.sub(r"[\d,]+\s*원.*$", "", name).strip()
    name = re.sub(r"^No\.\d+\s*", "", name).strip()
    name = re.sub(r"^[✔✓•●·\-\*\[\]▶▷☞]+", "", name).strip()
    name = re.sub(r"\[\d+\]\s*$", "", name).strip()

    name = re.sub(r"\(.*?(한정|제한|별매|안내|예약).*?\)", "", name)
    name = re.sub(r"\[.*?(한정|제한|별매|안내|예약).*?\]", "", name)

    name = re.sub(r"\s{2,}", " ", name).strip()
    return normalize_space(name)


def is_probable_product_href(href: str) -> bool:
    if not href:
        return False

    lower = href.lower().strip()

    if lower.startswith("javascript:") or lower.startswith("mailto:") or lower.startswith("#"):
        return False

    blocked_bits = [
        "/board/",
        "/bbs/",
        "/search",
        "/member/",
        "/mypage/",
        "/cart",
        "/login",
        "/join",
    ]
    if any(bit in lower for bit in blocked_bits):
        return False

    if lower in ["https://www.thegundambase.com/", "https://www.thegundambase.com"]:
        return False

    parsed = urlparse(lower)
    query = parse_qs(parsed.query)
    detail_keys = {"it_id", "goodsno", "goods_no", "product_no", "item_no", "id", "no", "idx"}

    if any(k in query for k in detail_keys):
        return True

    detail_path_bits = [
        "/shop/item.php",
        "/shop/item.view.php",
        "/shop/goods_view.php",
        "/goods/view",
        "/product/view",
        "/item/view",
        "/shop/detail",
        "/product/",
        "/item/",
        "/productdetail",
    ]
    if any(bit in lower for bit in detail_path_bits):
        return True

    # 완화: thegundambase 내부 링크면 일단 후보 허용
    if "thegundambase.com" in lower:
        return True

    return False


def detect_stock_text(text: str) -> str:
    t = normalize_space(text).lower()

    if any(k in t for k in ["품절", "일시품절", "sold out", "out of stock", "재고없음", "재고 없음"]):
        return "품절"

    if any(k in t for k in ["예약", "예약중", "예약 판매", "입고예정", "입고 예정", "pre-order", "preorder"]):
        return "예약/입고예정"

    if any(k in t for k in ["판매중", "장바구니", "구매", "구매하기", "바로구매", "buy now", "add to cart"]):
        return "판매중"

    return "상태 확인 필요"


def extract_price(text: str) -> str:
    m = re.search(r"([\d,]+)\s*원", text)
    return f"{m.group(1)}원" if m else ""


def is_gundam_product_name(name: str) -> bool:
    text = cleanup_candidate_name(name).lower()
    if not text:
        return False

    if text in BAD_EXACT_NAMES:
        return False

    if len(text) < 3 or len(text) > 120:
        return False

    if looks_broken_text(text):
        return False

    if any(b in text for b in BLOCKED_KEYWORDS):
        return False

    if "프라모델" in text:
        return False

    if any(k in text for k in ALLOWED_KEYWORDS):
        return True

    if re.search(r"\b(mgsd|mgex|pg|mg|rg|hg|sd|bb|eg)\b", text):
        return True

    return False


def find_parent_card(a):
    return a.find_parent(["li", "div", "dd", "dl", "section", "article"])


def pick_best_name_from_anchor(a) -> str:
    candidates = []

    selectors = [
        ".item_name", ".prd_name", ".goods_name", ".product_name",
        ".name", ".tit", ".title", "strong", "b", "h3", "h4", "dt",
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

        parent_text = normalize_space(parent.get_text(" ", strip=True))
        if parent_text:
            candidates.append(parent_text)

    for text in candidates:
        cleaned = cleanup_candidate_name(text)
        if not cleaned:
            continue
        if is_gundam_product_name(cleaned):
            return cleaned

    for text in candidates:
        cleaned = cleanup_candidate_name(text)
        if len(cleaned) >= 3 and not looks_broken_text(cleaned):
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
        price_selectors = [".price", ".cost", ".amount", ".money", ".won", "strong", "dd"]
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

    anchor_count = 0
    href_pass_count = 0
    name_pass_count = 0

    for a in soup.find_all("a", href=True):
        anchor_count += 1

        href = absolute_url(a.get("href", ""))
        raw_text = normalize_space(a.get_text(" ", strip=True))

        if not is_probable_product_href(href):
            continue
        href_pass_count += 1

        name = pick_best_name_from_anchor(a)
        if not name:
            continue
        name_pass_count += 1

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

        if len(blocks) <= 20:
            print(f"[DEBUG] 후보 링크: {href} / 이름: {name} / 원문: {raw_text}")

    print(f"[DEBUG] 전체 a[href] 수: {anchor_count}")
    print(f"[DEBUG] href 통과 수: {href_pass_count}")
    print(f"[DEBUG] 이름 통과 수: {name_pass_count}")
    print(f"[DEBUG] blocks 수: {len(blocks)}")

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

        if href.strip() == url.strip():
            continue

        if not href:
            continue

        if lower_href.endswith("/search") or "/search" in lower_href:
            continue

        if len(name) < 3:
            continue

        if name.lower() in BAD_EXACT_NAMES:
            continue

        if looks_broken_text(name):
            continue

        if any(b in name.lower() for b in BLOCKED_KEYWORDS):
            continue

        if not is_gundam_product_name(name):
            continue

        if not stock_text:
            stock_text = "상태 확인 필요"

        key = f"{href}|{normalize_name(name)}"
        if key in seen:
            continue
        seen.add(key)

        print(f"[DEBUG] 최종 후보: {name} -> {href} / 상태: {stock_text} / 가격: {price}")

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
    if db is None or firestore is None:
        return

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
    if db is None or firestore is None:
        return False

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
        "source": "gundambase",
        "sourceType": "product",
        "site": "건담베이스",
        "mallName": "건담베이스",
        "country": "KR",
        "region": "KR",
        "productUrl": item["url"],
        "url": item["url"],
        "sourcePage": item["sourcePage"],
        "verificationSources": ArrayUnion(["gundambase"]),
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
    return True


def save_items_json(items, output_path=OUTPUT_JSON_PATH):
    output_path.parent.mkdir(parents=True, exist_ok=True)

    payload = []
    for item in items:
        item_id = sha1(item["url"] + "|" + item["name"])
        payload.append({
            "itemId": item_id,
            "name": item["name"],
            "title": item["title"],
            "canonicalName": normalize_name(item["name"]),
            "price": item["price"],
            "stockText": item["stockText"],
            "status": item["stockText"],
            "source": "gundambase",
            "sourceType": "product",
            "site": "건담베이스",
            "mallName": "건담베이스",
            "country": "KR",
            "region": "KR",
            "productUrl": item["url"],
            "url": item["url"],
            "sourcePage": item["sourcePage"],
            "verificationSources": ["gundambase"],
        })

    output_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"[저장] JSON 저장 완료: {output_path} / {len(payload)}개")


def main():
    db = init_firestore()

    all_items = []
    for url in LIST_URLS:
        try:
            print(f"[시작] 목록 요청: {url}")
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

    dedup = dedup[:MAX_ITEMS]

    print(f"[DEBUG] 최종 저장 대상 수: {len(dedup)}")

    save_items_json(dedup)

    saved = 0
    if db is not None:
        for item in dedup:
            try:
                if save_item(db, item):
                    saved += 1
                    print(f"저장: {item['name']} / {item['stockText']} / {item['price']}")
            except Exception as e:
                print(f"저장 실패: {item['name']} -> {e}")
    else:
        print("[경고] Firestore 저장은 생략하고 JSON만 생성함")

    print(f"[완료] GundamBase 완료 / 총 추출 {len(dedup)}개 / Firestore 저장 {saved}개")


if __name__ == "__main__":
    main()
