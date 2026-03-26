import hashlib
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

def is_allowed_final_url(item: dict) -> bool:
    url = (item.get("productUrl") or item.get("url") or "").strip()
    mall = (item.get("mallName") or item.get("site") or "").lower()

    if not url:
        return False

    try:
        host = (urlparse(url).netloc or "").lower()
    except Exception:
        return False

    # mall/source 기준으로 허용 도메인 강제
    if "건담샵" in mall or "gundamshop" in mall:
        return host == "gundamshop.co.kr" or host.endswith(".gundamshop.co.kr")

    if "건담베이스" in mall or "gundambase" in mall:
        return host == "thegundambase.com" or host.endswith(".thegundambase.com")

    if "비엔케이알몰" in mall or "bnkrmall" in mall:
        return host == "bnkrmall.co.kr" or host.endswith(".bnkrmall.co.kr")

    # 알 수 없는 mall은 일단 막기
    return False
    
try:
    import firebase_admin
    from firebase_admin import credentials, firestore
except Exception:
    firebase_admin = None
    credentials = None
    firestore = None


SERVICE_ACCOUNT_PATH = "serviceAccountKey.json"

INPUT_JSON_FILES = [
    "data/gundambase_items.json",
    "data/gundamshop_items.json",
    "data/bnkrmall_items.json",
]

OUTPUT_JSON_PATH = "data/aggregated_item_kr.json"
TARGET_COLLECTION = "aggregated_items"


def sha1(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


def normalize_space(text: str) -> str:
    text = str(text or "").replace("\xa0", " ")
    return re.sub(r"\s+", " ", text).strip()


def normalize_name(name: str) -> str:
    text = normalize_space(name).lower()

    text = text.replace("ver.", "ver")
    text = text.replace("version", "ver")

    # 스케일 표기 제거
    text = re.sub(r"\b1/\d+\b", "", text)
    text = re.sub(r"\b\d+/\d+\b", "", text)

    # 특수문자 정리
    text = re.sub(r"[\[\]\(\)\-_/.,:+]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def normalize_price(price: Any) -> str:
    text = normalize_space(str(price or ""))
    if not text:
        return ""

    m = re.search(r"([\d,]+)\s*원", text)
    if m:
        return f"{m.group(1)}원"

    m = re.search(r"([\d,]+)", text)
    if m:
        return f"{m.group(1)}원"

    return text


def normalize_status(status: Any) -> str:
    text = normalize_space(str(status or ""))

    if not text:
        return "확인필요"

    lower = text.lower()

    if "품절" in text or "sold out" in lower or "out of stock" in lower:
        return "품절"

    if "예약" in text or "입고예정" in text or "입고 예정" in text:
        return "예약/입고예정"

    if "판매중" in text or "구매" in text or "장바구니" in text or "buy" in lower:
        return "판매중"

    return text


def load_json_file(path: str) -> List[Dict[str, Any]]:
    p = Path(path)
    if not p.exists():
        print(f"[경고] 입력 JSON 없음: {path}")
        return []

    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        if isinstance(data, list):
            print(f"[로드] {path} / {len(data)}개")
            return data
        print(f"[경고] 리스트 형식 아님: {path}")
        return []
    except Exception as e:
        print(f"[경고] JSON 읽기 실패: {path} -> {e}")
        return []


def init_firestore():
    if firebase_admin is None or credentials is None or firestore is None:
        print("[경고] firebase_admin 미설치 - Firestore 저장 생략")
        return None

    try:
        key_path = Path(SERVICE_ACCOUNT_PATH)

        if not key_path.exists():
            print(f"[경고] {SERVICE_ACCOUNT_PATH} 없음 - Firestore 저장 생략")
            return None

        if not firebase_admin._apps:
            cred = credentials.Certificate(str(key_path))
            firebase_admin.initialize_app(cred)

        return firestore.client()
    except Exception as e:
        print(f"[경고] Firestore 초기화 건너뜀: {e}")
        return None


def build_source_list(item: Dict[str, Any]) -> List[str]:
    sources = item.get("verificationSources") or item.get("sources") or []

    if isinstance(sources, str):
        sources = [sources]

    if not sources:
        one = normalize_space(item.get("source", ""))
        if one:
            sources = [one]

    cleaned = []
    seen = set()
    for s in sources:
        s = normalize_space(s)
        if not s:
            continue
        if s in seen:
            continue
        seen.add(s)
        cleaned.append(s)
    return cleaned


def make_group_key(item):
    canonical_name = normalize_name(
        item.get("canonicalName") or item.get("name") or item.get("title") or ""
    )
    mall_name = normalize_space(item.get("mallName") or item.get("site") or "")
    return f"{canonical_name}|{mall_name}"


def choose_better_item(a: Dict[str, Any], b: Dict[str, Any]) -> Dict[str, Any]:
    """
    같은 상품군 안에서 대표 레코드 선택.
    우선순위:
    1) 판매중
    2) 가격 정보 있음
    3) 이름 길이 적당히 긴 것
    """
    def score(x: Dict[str, Any]) -> int:
        s = 0
        status = normalize_status(x.get("status") or x.get("stockText"))
        price = normalize_price(x.get("price"))
        name = normalize_space(x.get("name") or x.get("title"))

        if status == "판매중":
            s += 100
        elif status == "예약/입고예정":
            s += 60
        elif status == "품절":
            s += 30

        if price:
            s += 20

        s += min(len(name), 40)
        return s

    return a if score(a) >= score(b) else b


def merge_items(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[str, Dict[str, Any]] = {}

    for raw in items:
        name = normalize_space(raw.get("name") or raw.get("title"))
        if not name:
            continue

        key = make_group_key(raw)
        if not key:
            continue

        current = {
            "name": name,
            "title": normalize_space(raw.get("title") or name),
            "canonicalName": normalize_name(raw.get("canonicalName") or name),
            "price": normalize_price(raw.get("price")),
            "stockText": normalize_status(raw.get("stockText") or raw.get("status")),
            "status": normalize_status(raw.get("status") or raw.get("stockText")),
            "source": normalize_space(raw.get("source") or ""),
            "sourceType": normalize_space(raw.get("sourceType") or "product"),
            "site": normalize_space(raw.get("site") or raw.get("mallName") or ""),
            "mallName": normalize_space(raw.get("mallName") or raw.get("site") or ""),
            "country": normalize_space(raw.get("country") or "KR"),
            "region": normalize_space(raw.get("region") or "KR"),
            "productUrl": normalize_space(raw.get("productUrl") or raw.get("url")),
            "url": normalize_space(raw.get("url") or raw.get("productUrl")),
            "sourcePage": normalize_space(raw.get("sourcePage") or ""),
            "verificationSources": build_source_list(raw),
        }

        if key not in grouped:
            grouped[key] = current
            continue

        prev = grouped[key]
        best = choose_better_item(prev, current)

        merged_sources = []
        seen_sources = set()
        for s in prev.get("verificationSources", []) + current.get("verificationSources", []):
            s = normalize_space(s)
            if not s or s in seen_sources:
                continue
            seen_sources.add(s)
            merged_sources.append(s)

        best["verificationSources"] = merged_sources

        # 비어있던 필드는 채우기
        for field in [
            "price",
            "productUrl",
            "url",
            "sourcePage",
            "site",
            "mallName",
            "source",
            "title",
        ]:
            if not normalize_space(best.get(field)):
                alt = prev.get(field) or current.get(field) or ""
                best[field] = normalize_space(alt)

        grouped[key] = best

    merged_list = []
    for canonical_name, item in grouped.items():
        sources = item.get("verificationSources", [])
        verification_count = len(sources)
        verification_status = "cross_checked" if verification_count >= 2 else "single_source"

        item_id_base = (item.get("url") or item.get("name") or canonical_name)
        item_id = sha1(item_id_base + "|" + canonical_name)

        merged_item = {
            "itemId": item_id,
            "name": item.get("name", ""),
            "title": item.get("title", ""),
            "canonicalName": canonical_name,
            "price": item.get("price", ""),
            "stockText": item.get("stockText", ""),
            "status": item.get("status", ""),
            "source": item.get("source", ""),
            "sourceType": item.get("sourceType", "product"),
            "site": item.get("site", ""),
            "mallName": item.get("mallName", ""),
            "country": item.get("country", "KR"),
            "region": item.get("region", "KR"),
            "productUrl": item.get("productUrl", ""),
            "url": item.get("url", ""),
            "sourcePage": item.get("sourcePage", ""),
            "verificationSources": sources,
            "verificationCount": verification_count,
            "verificationStatus": verification_status,
        }
        merged_list.append(merged_item)

    merged_list.sort(key=lambda x: (x.get("name", ""), x.get("mallName", "")))
    return merged_list


def save_merged_json(items: List[Dict[str, Any]], output_path: str = OUTPUT_JSON_PATH):
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(items, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"[저장] 병합 JSON 저장 완료: {output_path} / {len(items)}개")


def save_to_firestore(db, items: List[Dict[str, Any]]) -> int:
    if db is None or firestore is None:
        print("[경고] Firestore 저장 생략")
        return 0

    saved = 0
    for item in items:
        try:
            doc_id = item["itemId"]
            payload = dict(item)
            payload["updatedAt"] = firestore.SERVER_TIMESTAMP
            db.collection(TARGET_COLLECTION).document(doc_id).set(payload, merge=True)
            saved += 1
        except Exception as e:
            print(f"[경고] Firestore 저장 실패: {item.get('name', '')} -> {e}")

    print(f"[저장] Firestore 저장 완료 / {saved}개")
    return saved


def main():
    db = init_firestore()

    source_items: List[Dict[str, Any]] = []

    for path in INPUT_JSON_FILES:
        source_items.extend(load_json_file(path))

    print(f"[정보] 전체 입력 개수: {len(source_items)}개")

    merged_items = merge_items(source_items)
    before_count = len(merged_items)

    merged_items = [item for item in merged_items if is_allowed_final_url(item)]

    print(f"[정리] 외부 URL 제거: {before_count} -> {len(merged_items)}")
    print(f"[정보] 병합 결과 개수: {len(merged_items)}개")

    save_merged_json(merged_items, OUTPUT_JSON_PATH)
    save_to_firestore(db, merged_items)

    print("[완료] KR 병합 완료")


if __name__ == "__main__":
    main()
