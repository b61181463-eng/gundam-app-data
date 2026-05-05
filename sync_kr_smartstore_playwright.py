import hashlib
import re
from datetime import datetime

import firebase_admin
from firebase_admin import credentials, firestore
from playwright.sync_api import sync_playwright

SERVICE_ACCOUNT_PATH = "serviceAccountKey.json"

# 나중에 실제 스토어로 바꾸면 됨
STORE_URL = "https://smartstore.naver.com/zeonshop"

ALLOWED_KEYWORDS = [
    "hg", "mg", "rg", "pg", "sd", "bb", "eg", "re/100",
    "full mechanics", "figure-rise standard",
    "건담", "건프라", "프라모델",
    "자쿠", "유니콘", "프리덤", "스트라이크", "에어리얼",
    "즈고크", "사자비", "뉴건담", "rx-78", "mk-ii", "시난주",
    "건캐논", "건탱크", "돔", "겔구그", "구프", "바르바토스",
]

BLOCKED_KEYWORDS = [
    "티셔츠", "후드", "의류", "의상",
    "머그컵", "컵", "텀블러", "보틀",
    "키링", "열쇠고리", "아크릴", "스탠드", "스티커",
    "포스터", "브로마이드", "엽서",
    "노트", "문구", "필통", "펜", "볼펜",
    "쿠션", "담요", "타월", "수건",
    "케이스", "파우치", "가방",
    "과자", "식품", "음료",
    "잡지", "도서", "책",
    "카드", "트레이딩 카드",
    "넨도로이드", "봉제", "인형",
]


def sha1(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


def normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def is_gundam_product_name(name: str) -> bool:
    text = normalize_space(name).lower()
    if not text:
        return False

    if any(keyword in text for keyword in BLOCKED_KEYWORDS):
        return False

    if any(keyword in text for keyword in ALLOWED_KEYWORDS):
        return True

    return False


def init_firestore():
    if not firebase_admin._apps:
        cred = credentials.Certificate(SERVICE_ACCOUNT_PATH)
        firebase_admin.initialize_app(cred)
    return firestore.client()


def detect_stock_text(text: str) -> str:
    lowered = (text or "").lower()

    if "품절" in text or "sold out" in lowered or "out of stock" in lowered:
        return "품절"

    if (
        "장바구니" in text
        or "구매하기" in text
        or "바로구매" in text
        or "판매중" in text
        or "구매 가능" in text
        or "available" in lowered
        or "in stock" in lowered
    ):
        return "판매중"

    return "상태 확인 필요"


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

    prev_price = existing.get("price")
    next_price = new_data.get("price")
    if prev_price != next_price:
        return "status_changed"

    return None


def add_event(db, item_id, name, change_type):
    db.collection("stock_events").add({
        "itemId": item_id,
        "name": name,
        "changeType": change_type,
        "createdAt": firestore.SERVER_TIMESTAMP,
    })


def extract_products_with_playwright(store_url: str):
    items = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1440, "height": 2200})

        page.goto(store_url, wait_until="networkidle", timeout=60000)

        # 페이지를 몇 번 내려서 상품 렌더링 유도
        for _ in range(4):
            page.mouse.wheel(0, 2500)
            page.wait_for_timeout(1200)

        anchors = page.locator("a[href*='/products/']")
        count = anchors.count()

        seen = set()

        for i in range(count):
            try:
                a = anchors.nth(i)
                href = a.get_attribute("href") or ""
                text = normalize_space(a.inner_text())

                if "/products/" not in href:
                    continue

                if not href.startswith("http"):
                    if href.startswith("/"):
                        href = "https://smartstore.naver.com" + href
                    else:
                        href = store_url.rstrip("/") + "/" + href.lstrip("/")

                # 너무 짧은 텍스트는 제외
                if len(text) < 2:
                    continue

                key = f"{href}|{text}"
                if key in seen:
                    continue
                seen.add(key)

                items.append({
                    "name": text,
                    "url": href,
                })
            except Exception:
                continue

        browser.close()

    return items


def parse_product_with_playwright(product_url: str):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1440, "height": 2200})

        page.goto(product_url, wait_until="networkidle", timeout=60000)
        page.wait_for_timeout(1500)

        full_text = normalize_space(page.locator("body").inner_text())

        title = ""
        try:
            title = normalize_space(page.locator("h1").first.inner_text())
        except Exception:
            pass

        if not title:
            try:
                title = normalize_space(page.title())
            except Exception:
                title = ""

        price = ""
        price_match = re.search(r"([\d,]+)\s*원", full_text)
        if price_match:
            price = f"{price_match.group(1)}원"

        stock_text = detect_stock_text(full_text)

        image_url = ""
        try:
            img = page.locator("img").first
            image_url = img.get_attribute("src") or ""
        except Exception:
            pass

        browser.close()

    return {
        "title": title,
        "price": price,
        "stockText": stock_text,
        "imageUrl": image_url,
    }


def save_item(db, parsed_item):
    item_id = sha1(parsed_item["url"])

    existing = db.collection("aggregated_items").document(item_id).get()
    existing_data = existing.to_dict() if existing.exists else None

    new_data = {
        "itemId": item_id,
        "name": parsed_item["title"],
        "title": parsed_item["title"],
        "price": parsed_item["price"],
        "stockText": parsed_item["stockText"],
        "status": parsed_item["stockText"],
        "source": "smartstore",
        "sourceType": "product",
        "site": "네이버 스마트스토어",
        "mallName": "스마트스토어",
        "country": "KR",
        "region": "KR",
        "productUrl": parsed_item["url"],
        "url": parsed_item["url"],
        "imageUrl": parsed_item["imageUrl"],
        "updatedAt": firestore.SERVER_TIMESTAMP,
        "lastSeenAt": firestore.SERVER_TIMESTAMP,
    }

    change_type = determine_change(existing_data, new_data)
    if change_type:
        new_data["changeType"] = change_type
        new_data["lastChangedAt"] = firestore.SERVER_TIMESTAMP
        add_event(db, item_id, parsed_item["title"], change_type)
    elif existing_data and existing_data.get("changeType"):
        new_data["changeType"] = existing_data.get("changeType")

    db.collection("aggregated_items").document(item_id).set(new_data, merge=True)


def main():
    db = init_firestore()

    raw_items = extract_products_with_playwright(STORE_URL)
    print(f"원본 후보 {len(raw_items)}개 발견")

    filtered = []
    seen_urls = set()

    for item in raw_items:
        if item["url"] in seen_urls:
            continue
        seen_urls.add(item["url"])

        if not is_gundam_product_name(item["name"]):
            continue

        filtered.append(item)

    print(f"건담 상품 필터 후 {len(filtered)}개")

    saved_count = 0

    for item in filtered[:20]:
        try:
            parsed = parse_product_with_playwright(item["url"])

            title = parsed["title"] or item["name"]
            if not is_gundam_product_name(title):
                continue

            payload = {
                "title": title,
                "price": parsed["price"],
                "stockText": parsed["stockText"],
                "imageUrl": parsed["imageUrl"],
                "url": item["url"],
            }

            save_item(db, payload)
            saved_count += 1
            print(f"저장: {title} / {parsed['stockText']} / {parsed['price']}")
        except Exception as e:
            print(f"에러: {item['url']} -> {e}")

    print(f"✅ 스마트스토어 완료 / 저장 {saved_count}개")


if __name__ == "__main__":
    main()