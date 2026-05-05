from collections import defaultdict

import firebase_admin
from firebase_admin import credentials, firestore

from name_utils import normalize_name, tokenize_name, is_probably_same_product

SERVICE_ACCOUNT_PATH = "serviceAccountKey.json"


def init_firestore():
    if not firebase_admin._apps:
        cred = credentials.Certificate(SERVICE_ACCOUNT_PATH)
        firebase_admin.initialize_app(cred)
    return firestore.client()


def safe_str(value, fallback=""):
    if value is None:
        return fallback
    text = str(value).strip()
    return text if text else fallback


def main():
    db = init_firestore()

    docs = list(db.collection("aggregated_items").stream())

    by_source = defaultdict(list)

    for doc in docs:
        data = doc.to_dict() or {}

        if safe_str(data.get("country")).upper() != "KR":
            continue

        source = safe_str(data.get("source"))
        name = safe_str(data.get("name") or data.get("title"))
        canonical = safe_str(data.get("canonicalName")) or normalize_name(name)

        if not source or not name:
            continue

        # 병합 문서는 제외
        if safe_str(data.get("site")) == "KR 통합":
            continue

        by_source[source].append({
            "name": name,
            "canonical": canonical,
            "tokens": tokenize_name(name),
        })

    gundamshop_items = by_source.get("gundamshop", [])
    notice_items = by_source.get("gundambase_notice", [])

    print(f"건담샵 개수: {len(gundamshop_items)}")
    print(f"루리웹 공지 개수: {len(notice_items)}")
    print()

    exact_matches = []
    fuzzy_matches = []

    notice_canonical_map = defaultdict(list)
    for item in notice_items:
        notice_canonical_map[item["canonical"]].append(item)

    # 1) exact canonical match
    for shop in gundamshop_items:
        if shop["canonical"] in notice_canonical_map:
            for notice in notice_canonical_map[shop["canonical"]]:
                exact_matches.append((shop, notice))

    print(f"정확 일치 개수: {len(exact_matches)}")
    for idx, (shop, notice) in enumerate(exact_matches[:20], start=1):
        print(f"[정확 {idx}]")
        print("  SHOP  :", shop["name"])
        print("  NOTICE:", notice["name"])
        print("  CANON :", shop["canonical"])
        print()

    # 2) fuzzy match
    for shop in gundamshop_items:
        for notice in notice_items:
            if shop["canonical"] == notice["canonical"]:
                continue

            if is_probably_same_product(shop["name"], notice["name"]):
                fuzzy_matches.append((shop, notice))

    print(f"유사 일치 개수: {len(fuzzy_matches)}")
    for idx, (shop, notice) in enumerate(fuzzy_matches[:30], start=1):
        print(f"[유사 {idx}]")
        print("  SHOP  :", shop["name"])
        print("  NOTICE:", notice["name"])
        print("  SHOP C:", shop["canonical"])
        print("  NOTE C:", notice["canonical"])
        print("  SHOP T:", shop["tokens"])
        print("  NOTE T:", notice["tokens"])
        print()


if __name__ == "__main__":
    main()