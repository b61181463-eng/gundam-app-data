import re
import requests
from bs4 import BeautifulSoup
from collections import defaultdict

import firebase_admin
from firebase_admin import credentials, firestore

SERVICE_ACCOUNT_PATH = "serviceAccountKey.json"

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}


def clean_text(text):
    return re.sub(r"\s+", " ", text).strip()


def init_firestore():
    if not firebase_admin._apps:
        cred = credentials.Certificate(SERVICE_ACCOUNT_PATH)
        firebase_admin.initialize_app(cred)
    return firestore.client()


def make_doc_id(text: str):
    return re.sub(r"[^a-zA-Z0-9가-힣]+", "_", text).strip("_").lower()


def normalize_product_name(name: str):
    text = name.lower().strip()

    remove_words = [
        "bandai spirits",
        "bandai",
        "model kit",
        "plastic model",
        "plamo",
        "pre-order",
        "preorder",
        "gundam planet",
        "usa gundam store",
        "newtype",
    ]

    for word in remove_words:
        text = text.replace(word, " ")

    text = re.sub(r"\(.*?\)", " ", text)

    replacements = {
        "mobile suit gundam": "gundam",
        "rx 78 2": "rx-78-2",
        "rx78 2": "rx-78-2",
        "rx 78-2": "rx-78-2",
        "rx78-2": "rx-78-2",
        "msn 04": "msn-04",
        "msn04": "msn-04",
        "hguc": "hg",
    }

    for old, new in replacements.items():
        text = text.replace(old, new)

    text = re.sub(r"[^a-z0-9가-힣/\-\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def stock_to_fields(stock_text: str):
    text = clean_text(stock_text)
    lower = text.lower()

    if "out of stock" in lower or "sold out" in lower:
        return {
            "quantity": 0,
            "isSoldOut": True,
            "stockStatus": "Out of stock",
            "sourceText": text,
        }

    if "only" in lower and "left" in lower:
        match = re.search(r"only\s+(\d+)\s+left", lower)
        if match:
            qty = int(match.group(1))
            return {
                "quantity": qty,
                "isSoldOut": qty == 0,
                "stockStatus": f"Only {qty} left",
                "sourceText": text,
            }

    if "<" in lower and "left" in lower:
        match = re.search(r"<\s*(\d+)\s+left", lower)
        if match:
            qty = int(match.group(1))
            return {
                "quantity": qty,
                "isSoldOut": False,
                "stockStatus": f"< {qty} left",
                "sourceText": text,
            }

    return {
        "quantity": 999,
        "isSoldOut": False,
        "stockStatus": "In stock",
        "sourceText": text if text else "In stock",
    }


def save_store_items(db, store_id, store_name, country, source_url, items):
    store_ref = db.collection("stores").document(store_id)

    store_ref.set({
        "name": store_name,
        "country": country,
        "sourceType": "web_scrape",
        "sourceUrl": source_url,
        "lastSyncedAt": firestore.SERVER_TIMESTAMP,
        "updatedAt": firestore.SERVER_TIMESTAMP,
    }, merge=True)

    saved_count = 0

    for item in items:
        doc_id = make_doc_id(item["name"])
        if not doc_id:
            continue

        store_ref.collection("items").document(doc_id).set({
            "name": item["name"],
            "normalizedName": normalize_product_name(item["name"]),
            "quantity": item["quantity"],
            "isSoldOut": item["isSoldOut"],
            "restocked": False,
            "stockStatus": item["stockStatus"],
            "sourceText": item["sourceText"],
            "lastSyncedAt": firestore.SERVER_TIMESTAMP,
            "updatedAt": firestore.SERVER_TIMESTAMP,
        }, merge=True)

        saved_count += 1

    print(f"{store_name} 저장 완료: {saved_count}개")


# =========================
# Newtype
# =========================
def fetch_newtype():
    url = "https://newtype.us/search?q=gundam"
    response = requests.get(url, headers=HEADERS, timeout=20)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")

    stock_nodes = soup.find_all(
        string=re.compile(
            r"(In stock|Out of stock|Only\s+\d+\s+left|<\s*\d+\s+left)",
            re.I,
        )
    )

    items = []
    seen = set()

    for stock_node in stock_nodes:
        stock_text = clean_text(stock_node)
        parent = stock_node.parent
        product_block = None

        for _ in range(8):
            if parent is None:
                break
            if parent.name in ["article", "div", "li", "section"]:
                links = parent.find_all("a")
                if links:
                    product_block = parent
            parent = parent.parent

        if product_block is None:
            continue

        candidates = []
        for a in product_block.find_all("a"):
            text = clean_text(a.get_text(" ", strip=True))
            if not text:
                continue
            if len(text) < 4:
                continue
            if re.fullmatch(
                r"(In stock|Out of stock|Only\s+\d+\s+left|<\s*\d+\s+left)",
                text,
                re.I,
            ):
                continue
            if text.lower() in {"add to bag", "yes", "no"}:
                continue
            candidates.append(text)

        if not candidates:
            continue

        name = sorted(candidates, key=len, reverse=True)[0]

        if name in seen:
            continue
        seen.add(name)

        stock_info = stock_to_fields(stock_text)

        items.append({
            "name": name,
            "quantity": stock_info["quantity"],
            "isSoldOut": stock_info["isSoldOut"],
            "stockStatus": stock_info["stockStatus"],
            "sourceText": stock_info["sourceText"],
        })

    return url, items


# =========================
# USA Gundam Store
# =========================
def fetch_usa_gundam_store():
    url = "https://www.usagundamstore.com/pages/search-results-page?q=hg+nobel+gundam"
    response = requests.get(url, headers=HEADERS, timeout=20)
    response.raise_for_status()
    text = response.text

    stock_map = {}

    price_entries = re.finditer(
        r'productImageAndPrice\[(\d+)\]\s*=\s*\[(.*?)\];',
        text,
        re.S,
    )

    for match in price_entries:
        product_id = match.group(1)
        raw_block = match.group(2)

        if "mega-menu-sold_out" in raw_block.lower() or "sold out" in raw_block.lower():
            stock_map[product_id] = stock_to_fields("Sold out")
        else:
            stock_map[product_id] = stock_to_fields("In stock")

    wireframe_match = re.search(
        r'var\s+mmWireframe\s*=\s*\{"html"\s*:\s*"(.*)"\s*\};',
        text,
        re.S,
    )

    if not wireframe_match:
        print("USA mmWireframe 추출 실패")
        return url, []

    raw_html = wireframe_match.group(1)
    raw_html = bytes(raw_html, "utf-8").decode("unicode_escape")

    soup = BeautifulSoup(raw_html, "html.parser")

    items = []
    seen_names = set()

    for li in soup.select("li[role='none']"):
        img = li.select_one("img.get-product-image[data-id]")
        name_el = li.select_one("a.mm-product-name .mm-title")

        if not img or not name_el:
            continue

        product_id = img.get("data-id", "").strip()
        name = clean_text(name_el.get_text(" ", strip=True))

        if not product_id or not name:
            continue
        if name in seen_names:
            continue

        seen_names.add(name)
        stock_info = stock_map.get(product_id, stock_to_fields("In stock"))

        items.append({
            "name": name,
            "quantity": stock_info["quantity"],
            "isSoldOut": stock_info["isSoldOut"],
            "stockStatus": stock_info["stockStatus"],
            "sourceText": stock_info["sourceText"],
        })

    print("USA Gundam Store 파싱 결과 개수:", len(items))
    if items:
        print("USA 첫 상품:", items[0])

    return url, items


# =========================
# Gundam Planet
# public gunpla collection page
# =========================
def fetch_gundam_planet():
    url = "https://www.gundamplanet.com/collections/gunpla"
    response = requests.get(url, headers=HEADERS, timeout=20)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    page_text = clean_text(soup.get_text(" ", strip=True))

    # 공개 페이지에 product block이 텍스트로 노출됨:
    # <상품명> ... Added Go to cart Add to cart Read more ...
    # 또는 Sold Out Read more ...
    pattern = re.finditer(
        r'([A-Za-z0-9가-힣#/\-\(\)\[\]\.\'":,&\+\s]{8,140}?)\s+'
        r'(?:NULL|.{0,180}?)\s+read more\s+Added\s+Go to cart\s+'
        r'(Add to cart|Sold Out)\s+Read more',
        page_text,
        re.I,
    )

    items = []
    seen_names = set()

    for match in pattern:
        name = clean_text(match.group(1))
        stock_label = clean_text(match.group(2))

        bad_fragments = [
            "top selling items",
            "shopping options",
            "filter and sort",
            "gundam planet",
            "special offers",
            "account help",
            "sort by",
            "clear all",
        ]

        lower = name.lower()
        if len(name) < 8:
            continue
        if any(bad in lower for bad in bad_fragments):
            continue
        if name in seen_names:
            continue

        seen_names.add(name)
        stock_info = stock_to_fields(stock_label)

        items.append({
            "name": name,
            "quantity": stock_info["quantity"],
            "isSoldOut": stock_info["isSoldOut"],
            "stockStatus": stock_info["stockStatus"],
            "sourceText": stock_info["sourceText"],
        })

    print("Gundam Planet 파싱 결과 개수:", len(items))
    if items:
        print("Gundam Planet 첫 상품:", items[0])

    return url, items


# =========================
# aggregated_items 생성
# =========================
def infer_consensus_status(source_entries):
    in_stock_count = 0
    out_stock_count = 0
    low_stock_count = 0

    for entry in source_entries:
        status = entry["stockStatus"].lower()

        if "out of stock" in status or "sold out" in status:
            out_stock_count += 1
        elif "only" in status or "<" in status:
            low_stock_count += 1
            in_stock_count += 1
        elif "in stock" in status:
            in_stock_count += 1

    if in_stock_count >= 2 and out_stock_count == 0:
        if low_stock_count >= 1:
            return "low_stock"
        return "in_stock"

    if out_stock_count >= 2 and in_stock_count == 0:
        return "out_of_stock"

    if in_stock_count >= 1 and out_stock_count >= 1:
        return "check_required"

    if in_stock_count == 1 and out_stock_count == 0:
        if low_stock_count >= 1:
            return "low_stock"
        return "in_stock"

    if out_stock_count == 1 and in_stock_count == 0:
        return "out_of_stock"

    return "unknown"


def build_aggregated_items(db, all_store_results):
    grouped = defaultdict(list)

    for store_id, store_name, items in all_store_results:
        for item in items:
            normalized = normalize_product_name(item["name"])
            if not normalized:
                continue

            grouped[normalized].append({
                "storeId": store_id,
                "storeName": store_name,
                "name": item["name"],
                "stockStatus": item["stockStatus"],
                "isSoldOut": item["isSoldOut"],
                "quantity": item["quantity"],
            })

    agg_ref = db.collection("aggregated_items")

    saved = 0

    for normalized_name, sources in grouped.items():
        if not sources:
            continue

        consensus = infer_consensus_status(sources)

        display_name = max(sources, key=lambda x: len(x["name"]))["name"]
        doc_id = make_doc_id(normalized_name)

        agg_ref.document(doc_id).set({
            "name": display_name,
            "normalizedName": normalized_name,
            "sourceCount": len(sources),
            "sources": sources,
            "consensusStatus": consensus,
            "lastCheckedAt": firestore.SERVER_TIMESTAMP,
            "updatedAt": firestore.SERVER_TIMESTAMP,
        }, merge=True)

        saved += 1

    print(f"aggregated_items 저장 완료: {saved}개")


def main():
    db = init_firestore()
    all_store_results = []

    print("Newtype 수집중...")
    newtype_url, newtype_items = fetch_newtype()
    print("Newtype 파싱 결과 개수:", len(newtype_items))
    if newtype_items:
        save_store_items(
            db,
            "newtype_us",
            "Newtype",
            "USA",
            newtype_url,
            newtype_items,
        )
        all_store_results.append(("newtype_us", "Newtype", newtype_items))

    print("USA Gundam Store 수집중...")
    usa_url, usa_items = fetch_usa_gundam_store()
    if usa_items:
        save_store_items(
            db,
            "usa_gundam_store",
            "USA Gundam Store",
            "USA",
            usa_url,
            usa_items,
        )
        all_store_results.append(("usa_gundam_store", "USA Gundam Store", usa_items))
    else:
        print("USA Gundam Store 저장할 상품 없음")

    print("Gundam Planet 수집중...")
    gp_url, gp_items = fetch_gundam_planet()
    if gp_items:
        save_store_items(
            db,
            "gundam_planet",
            "Gundam Planet",
            "USA",
            gp_url,
            gp_items,
        )
        all_store_results.append(("gundam_planet", "Gundam Planet", gp_items))
    else:
        print("Gundam Planet 저장할 상품 없음")

    print("aggregated_items 생성중...")
    build_aggregated_items(db, all_store_results)

    print("전체 동기화 완료")


if __name__ == "__main__":
    main()