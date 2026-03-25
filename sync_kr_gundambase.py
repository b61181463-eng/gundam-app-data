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

BASE_URL = "https://www.thegundambase.com"
LIST_URLS = [
    "https://www.thegundambase.com",
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

ALLOWED_HINTS = [
    "건담", "자쿠", "유니콘", "프리덤", "스트라이크", "에어리얼",
    "즈고크", "사자비", "뉴건담", "시난주", "엑시아", "바르바토스",
    "캘리번", "루브리스", "데스티니", "저스티스", "아스트레이",
    "더블오", "톨기스", "윙건담", "짐", "건캐논", "건탱크",
    "mgsd", "mgex", "pg", "mg", "rg", "hg", "sd", "bb", "eg",
    "re/100", "full mechanics", "figure-rise", "30ms", "30mm",
]

BLOCKED_HINTS = [
    "티셔츠", "머그컵", "포스터", "아크릴", "키링", "스티커", "가방",
    "타월", "쿠션", "노트", "볼펜", "문구", "인형", "봉제", "도서",
    "프라모델", "건프라", "모형", "공지", "안내", "이벤트", "문의",
    "게시판", "로그인", "장바구니 보기",
]

DETAIL_PARAM_HINTS = {"product_no", "goodsno", "goods_no", "it_id", "item_no", "id", "no", "uid"}
DETAIL_PATH_HINTS = [
    "/product/",
    "/shopdetail",
    "/goods_view",
    "/item/",
    "/view",
]


def sha1(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


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


def fetch_html(url: str) -> str:
    res = requests.get(url, headers=HEADERS, timeout=20)
    res.raise_for_status()
    if not res.encoding or res.encoding.lower() == "iso-8859-1":
        res.encoding = res.apparent_encoding or "utf-8"
    return res.text


def absolute_url(href: str) -> str:
    return urljoin(BASE_URL, href or "")


def looks_like_product_url(url: str) -> bool:
    if not url:
        return False

    lower = url.lower()
    if lower.startswith("javascript:") or lower.startswith("mailto:") or lower.startswith("#"):
        return False

    parsed = urlparse(lower)
    qs = parse_qs(parsed.query)

    if any(hint in lower for hint in DETAIL_PATH_HINTS):
        return True

    if any(k in qs for k in DETAIL_PARAM_HINTS):
        return True

    if re.search(r"/product/\d+", lower):
        return True

    return False


def is_gundam_product_name(name: str) -> bool:
    text = normalize_space(name).lower()
    if not text:
        return False

    if len(text) < 2 or len(text) > 120:
        return False

    if any(b in text for b in BLOCKED_HINTS):
        return False

    if any(a in text for a in ALLOWED_HINTS):
        return True

    if re.search(r"\b(mg|rg|hg|pg|sd|eg|bb|mgsd)\b", text):
        return True

    return False


def detect_stock_text(text: str) -> str:
    t = normalize_space(text).lower()

    if "품절" in t or "sold out" in t or "out of stock" in t:
        return "품절"
    if "예약" in t or "입고예정" in t or "입고 예정" in t:
        return "예약/입고예정"
    if "판매중" in t or "구매" in t or "장바구니" in t or "buy" in t:
        return "판매중"
    return "판매중"


def extract_price(text: str) -> str:
    m = re.search(r"([\d,]+)\s*원", text)
    if m:
        return f"{m.group(1)}원"

    m = re.search(r"([\d,]+)", text)
    if m:
        return f"{m.group(1)}원"

    return ""


def pick_best_name(block) -> str:
    selectors = [
        ".item_name", ".prd_name", ".goods_name", ".product_name",
        ".name", ".tit", ".title", "strong", "b", "h3", "h4", "dt",
    ]

    for sel in selectors:
        node = block.select_one(sel)
        if node:
            text = normalize_space(node.get_text(" ", strip=True))
            if is_gundam_product_name(text):
                return text

    all_text = normalize_space(block.get_text(" ", strip=True))
    if is_gundam_product_name(all_text):
        return all_text[:120]

    return ""


def pick_best_link(block) -> str:
    candidates = []

    for a in block.select("a[href]"):
        href = absolute_url(a.get("href", ""))
        if not href:
            continue

        score = 0
        if looks_like_product_url(href):
            score += 100
        if a.select_one("img"):
            score += 10

        txt = normalize_space(a.get_text(" ", strip=True)).lower()
        for kw in ["view", "detail", "보기", "상품", "구매"]:
            if kw in txt:
                score += 5

        candidates.append((score, href))

    if not candidates:
        return ""

    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0][1]


def pick_price(block) -> str:
    selectors = [".price", ".cost", ".amount", ".money", ".won", "dd"]
    for sel in selectors:
        node = block.select_one(sel)
        if node:
            price = extract_price(node.get_text(" ", strip=True))
            if price:
                return price

    return extract_price(block.get_text(" ", strip=True))


def pick_stock_text(block) -> str:
    return detect_stock_text(block.get_text(" ", strip=True))


def extract_products_from_listing(url: str):
    html = fetch_html(url)
    soup = BeautifulSoup(html, "html.parser")

    selectors = [
        ".prdList li",
        ".product_list li",
        ".goods_list li",
        ".item_list li",
        ".itemList li",
        ".productList li",
        ".goodsList li",
        ".goods_box",
        ".item_box",
        ".product_box",
        "li:has(a[href])",
        "div:has(a[href])",
    ]

    cards = []
    for sel in selectors:
        found = soup.select(sel)
        if len(found) >= 3:
            cards = found
            break

    if not cards:
        cards = soup.select("a[href]")

    items = []
    seen = set()

    for card in cards:
        name = pick_best_name(card)
        if not name:
            continue

        href = pick_best_link(card)
        if not href:
            continue

        if not looks_like_product_url(href):
            continue

        stock_text = pick_stock_text(card)
        price = pick_price(card)

        key = f"{href}|{normalize_name(name)}"
        if key in seen:
            continue
        seen.add(key)

        items.append({
            "name": name,
            "title": name,
            "price": price,
            "stockText": stock_text,
            "url": href,
            "sourcePage": url,
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
            items = extract_products_from_listing(url)
            print(f"[목록] {url}")
            print(f"  후보 개수: {len(items)}")
            for item in items[:5]:
                print(f"   - {item['name']} / {item['stockText']} / {item['price']} / {item['url']}")
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
