import hashlib
import re
from urllib.parse import quote

import requests
from bs4 import BeautifulSoup
import firebase_admin
from firebase_admin import credentials, firestore
from firebase_admin.firestore import ArrayUnion

SERVICE_ACCOUNT_PATH = "serviceAccountKey.json"

SEARCH_KEYWORDS = [
    "HG 건담",
    "MG 건담",
    "RG 건담",
    "PG 건담",
    "건프라",
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
    "엑시아", "더블오", "아스트레이", "톨기스", "윙건담", "짐 스나이퍼",
    "풀아머", "더블 제타", "더블제타", "알트리아",
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
    "프라모델", "건프라", "모형",
    "www", "http", "https", ".com", ".kr",
]

BAD_TEXT_PATTERNS = [
    "�", "Ã", "Â", "°ç", "´ã", "¿", "½",
    "www", "http", "https", ".com", ".kr",
]


def sha1(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


def normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def normalize_name(name: str) -> str:
    text = (name or "").lower().strip()
    text = text.replace("ver.", "ver")
    text = text.replace("version", "ver")
    text = re.sub(r"\b1/\d+\b", "", text)
    text = re.sub(r"\b\d+/\d+\b", "", text)
    text = re.sub(r"[\[\]\(\)\-_/.,:+]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


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

    if len(text) < 4 or len(text) > 100:
        return False

    if text.endswith(("합니다", "있습니다", "드립니다", "바랍니다", "입니다", "됩니다")):
        return False

    if any(k in text for k in ALLOWED_KEYWORDS):
        return True

    return False


def fetch_html(url: str) -> str:
    res = requests.get(url, headers=HEADERS, timeout=20)
    res.raise_for_status()
    res.encoding = res.apparent_encoding or "utf-8"
    return res.text


def init_firestore():
    if not firebase_admin._apps:
        cred = credentials.Certificate(SERVICE_ACCOUNT_PATH)
        firebase_admin.initialize_app(cred)
    return firestore.client()


def detect_stock_text(text: str) -> str:
    t = normalize_space(text).lower()

    if "품절" in text or "sold out" in t or "out of stock" in t:
        return "품절"

    if "예약" in text or "입고예정" in text or "입고 예정" in text:
        return "예약/입고예정"

    if "판매중" in text or "장바구니" in text or "구매" in text:
        return "판매중"

    return "판매중"


def extract_price(text: str) -> str:
    m = re.search(r"([\d,]+)\s*원", text)
    return f"{m.group(1)}원" if m else ""


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


def search_smartstore_items(keyword: str):
    # 네이버 쇼핑 검색 결과 페이지 기반
    query = quote(keyword)
    url = f"https://search.shopping.naver.com/search/all?query={query}"
    html = fetch_html(url)
    soup = BeautifulSoup(html, "html.parser")

    items = []
    seen = set()

    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        text = normalize_space(a.get_text(" ", strip=True))

        if not href:
            continue

        if "smartstore.naver.com" not in href and "/products/" not in href:
            continue

        if not is_gundam_product_name(text):
            continue

        if href in seen:
            continue
        seen.add(href)

        parent_text = ""
        if a.parent:
            parent_text = normalize_space(a.parent.get_text(" ", strip=True))

        merged_text = normalize_space(f"{text} {parent_text}")
        price = extract_price(merged_text)
        stock_text = detect_stock_text(merged_text)

        items.append({
            "name": text,
            "title": text,
            "price": price,
            "stockText": stock_text,
            "url": href,
            "sourcePage": url,
            "mallName": "스마트스토어",
            "site": "네이버 스마트스토어",
        })

    return items


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
        "source": "smartstore",
        "sourceType": "product",
        "site": item["site"],
        "mallName": item["mallName"],
        "country": "KR",
        "region": "KR",
        "productUrl": item["url"],
        "url": item["url"],
        "sourcePage": item["sourcePage"],
        "verificationSources": ArrayUnion(["smartstore"]),
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
    for keyword in SEARCH_KEYWORDS:
        try:
            items = search_smartstore_items(keyword)
            print(f"[검색] {keyword}")
            print(f"  후보 개수: {len(items)}")
            for item in items[:5]:
                print(f"   - {item['name']} / {item['stockText']} / {item['price']}")
            all_items.extend(items)
        except Exception as e:
            print(f"[실패] {keyword} -> {e}")

    dedup = []
    seen = set()
    for item in all_items:
        key = item["url"] + "|" + normalize_name(item["name"])
        if key in seen:
            continue
        seen.add(key)
        dedup.append(item)

    saved = 0
    for item in dedup[:120]:
        try:
            save_item(db, item)
            saved += 1
            print(f"저장: {item['name']} / {item['stockText']} / {item['price']}")
        except Exception as e:
            print(f"저장 실패: {item['name']} -> {e}")

    print(f"✅ 스마트스토어 완료 / 총 저장 {saved}개")


if __name__ == "__main__":
    main()