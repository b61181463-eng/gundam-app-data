import json
import re
import os
from datetime import datetime
from pathlib import Path
from collections import defaultdict

import firebase_admin
from firebase_admin import credentials, firestore

# =====================
# 설정
# =====================

INPUT_FILES = [
    "data/gundamshop_items.json",
    "data/gundambase_items.json",
    "data/bnkrmall_items.json",
    "data/hobbyfactory_items.json",
    "data/gundamcity_items.json",
    "data/modelsale_items.json",
]

COLLECTION_NAME = "aggregated_items"
NOTICE_COLLECTION = "notices"

# =====================
# Firebase 초기화
# =====================

def init_firestore():
    if not firebase_admin._apps:
        cred_json = os.environ.get("FIREBASE_SERVICE_ACCOUNT_JSON")
        cred_dict = json.loads(cred_json)
        cred = credentials.Certificate(cred_dict)
        firebase_admin.initialize_app(cred)
    return firestore.client()

# =====================
# 유틸
# =====================

def clean_text(text):
    return re.sub(r"\s+", " ", (text or "")).strip()

def clean_name(name):
    name = clean_text(name)
    name = re.sub(r'^\[[^\]]+\]\s*', '', name)
    name = re.sub(r'^\([^)]+\)\s*', '', name)
    return name

def extract_price(text):
    text = text or ""
    m = re.search(r"[\d,]+원", text)
    return m.group(0) if m else ""

def normalize_status(text):
    t = (text or "").lower()

    if any(x in t for x in ["품절", "sold out", "out of stock"]):
        return "품절"
    if any(x in t for x in ["예약", "preorder"]):
        return "예약중"
    if any(x in t for x in ["판매", "구매", "장바구니", "in stock"]):
        return "판매중"

    return "상태 확인중"

def normalize_key(name):
    name = name.upper()
    name = re.sub(r"[^A-Z0-9가-힣]", "", name)
    return name

def is_valid_gundam_item(item):
    text = (
        f"{item.get('name','')} "
        f"{item.get('title','')} "
        f"{item.get('stockText','')}"
    ).upper()

    exclude = [
        "공지", "NOTICE", "EVENT", "이벤트",
        "쿠폰", "사은품", "증정",
        "배송", "안내", "문의", "교환",
        "반품", "결제", "공지사항",
    ]

    for e in exclude:
        if e in text:
            return False

    include = ["PG", "MG", "RG", "HG", "SD", "MGEX", "MGSD"]

    return any(i in text for i in include)

# =====================
# 데이터 로드
# =====================

def load_all_items():
    items = []

    for file in INPUT_FILES:
        path = Path(file)
        if not path.exists():
            continue

        data = json.loads(path.read_text(encoding="utf-8"))
       
