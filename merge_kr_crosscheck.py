import hashlib
import re
from collections import defaultdict

import firebase_admin
from firebase_admin import credentials, firestore

SERVICE_ACCOUNT_PATH = "serviceAccountKey.json"

SOURCE_COLLECTION = "aggregated_items"
TARGET_COLLECTION = "aggregated_items_merged"


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

def sha1(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


def normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()
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

BAD_SUBSTRINGS = [
    "sale price",
    "price now",
    "regular",
    "zagtoon",
    "method",
    "samg",
    "toei animation",
    "level-5",
    "hisago amazake-no",
    "예약중인",
    "예약상품",
    "건담샵에서 예약중인",
    "ⓒ",
    "©",
    "원피스",
    "포켓몬",
    "짱구",
    "명탐정 코난",
    "코난",
    "드래곤볼",
    "나루토",
    "귀멸",
    "에반게리온",
    "디지몬",
    "유희왕",
    "산리오",
    "마블",
    "디즈니",
    "토이스토리",
    "토토로",
    "세일러문",
    "프리큐어",
    "소닉",
    "도라에몽",
    "스파이더맨",
    "배트맨",
    "슈퍼맨",
    "가면라이더",
    "울트라맨",
    "업계",
    "작가",
    "부활절",
    "¼",
    "¹",
    "º",
    "³",
    "²",
    "¾",
]

ALLOWED_HINTS = [
    "건담", "자쿠", "유니콘", "프리덤", "스트라이크", "에어리얼",
    "즈고크", "사자비", "뉴건담", "시난주", "엑시아",
    "바르바토스", "캘리번", "루브리스", "데스티니",
    "저스티스", "아스트레이", "더블오", "톨기스",
    "윙건담", "짐", "건캐논", "건탱크",
    "mgsd", "mgex", "pg", "mg", "rg", "hg", "sd", "bb", "eg",
    "bb전사", "삼국창걸전", "30ms", "30mm",
]


def should_block_name(name: str, data: dict) -> bool:
    text = normalize_space(name).lower()

    source = normalize_space(data.get("source")).lower()
    site = normalize_space(data.get("site")).lower()
    mall = normalize_space(data.get("mallName")).lower()

    if not text:
        return True

    if text in BAD_EXACT_NAMES:
        return True

    if "ⓒ" in text or "©" in text:
        return True

    if any(bad in text for bad in BAD_SUBSTRINGS):
        return True

    if re.search(r"[¼¹º³²¾ÐÑÕÖ]", text):
        return True

    if re.fullmatch(r"[\d.,]+\s*(regular|sale price now|sale price|price now)", text):
        return True

    if re.search(r"[\d.,]+\s*(regular|sale price|price now)", text):
        return True

    if re.fullmatch(r"[a-z0-9\s.,/\-]+", text):
        if not any(ok in text for ok in ["gundam", "hg", "mg", "rg", "pg", "sd", "mgsd", "mgex"]):
            return True

    if len(text) < 4:
        return True

    if not any(hint.lower() in text for hint in ALLOWED_HINTS):
        if (
            source in {"gundamshop", "bnkrmall", "gundambase_notice"}
            or "건담샵" in site
            or "반다이" in site
            or "건담베이스" in site
            or "건담샵" in mall
            or "반다이" in mall
            or "건담베이스" in mall
        ):
            return True

    return False


def normalize_name(name: str) -> str:
    text = normalize_space(name).lower()
    text = text.replace("ver.", "ver")
    text = text.replace("version", "ver")
    text = re.sub(r"\b1/\d+\b", "", text)
    text = re.sub(r"\b\d+/\d+\b", "", text)
    text = re.sub(r"[\[\]\(\)\-_/.,:+]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def choose_best_link(data: dict) -> str:
    candidates = [
        data.get("productUrl"),
        data.get("url"),
        data.get("link"),
    ]
    for value in candidates:
        text = normalize_space(value)
        if text:
            return text
    return ""


def choose_best_name(data: dict) -> str:
    for key in ["name", "title", "canonicalName"]:
        value = normalize_space(data.get(key))
        if value:
            return value
    return ""


def choose_best_price(group: list[dict]) -> str:
    for item in group:
        price = normalize_space(item.get("price"))
        if price:
            return price
    return ""


def choose_best_image(group: list[dict]) -> str:
    for item in group:
        image = normalize_space(item.get("imageUrl"))
        if image:
            return image
    return ""


def choose_best_notice_date(group: list[dict]) -> str:
    for item in group:
        date = normalize_space(item.get("noticeDate"))
        if date:
            return date
    return ""


def choose_best_source_item(group: list[dict]) -> dict:
    # 1순위: 실제 상품 링크가 있는 일반 상품
    for item in group:
        source_type = normalize_space(item.get("sourceType")).lower()
        link = choose_best_link(item)
        if source_type == "product" and link:
            return item

    # 2순위: 링크가 있는 아무 항목
    for item in group:
        link = choose_best_link(item)
        if link:
            return item

    # 3순위: 첫 항목
    return group[0]


def collect_verification_sources(group: list[dict]) -> list[str]:
    result = []
    seen = set()

    for item in group:
        raw = item.get("verificationSources")
        if isinstance(raw, list):
            for source in raw:
                text = normalize_space(source)
                if text and text not in seen:
                    seen.add(text)
                    result.append(text)

        source = normalize_space(item.get("source"))
        if source and source not in seen:
            seen.add(source)
            result.append(source)

    return result


def is_notice_group(group: list[dict]) -> bool:
    for item in group:
        source_type = normalize_space(item.get("sourceType")).lower()
        if source_type == "notice_item":
            return True
    return False


def choose_status(group: list[dict]) -> str:
    # 일반 상품이 하나라도 있으면 일반 상품 상태 우선
    for item in group:
        source_type = normalize_space(item.get("sourceType")).lower()
        status = normalize_space(item.get("status") or item.get("stockText"))
        if source_type == "product" and status:
            return status

    # 없으면 아무 상태나
    for item in group:
        status = normalize_space(item.get("status") or item.get("stockText"))
        if status:
            return status

    return "상태 확인 필요"


def choose_source_type(group: list[dict]) -> str:
    if is_notice_group(group):
        has_product = any(
            normalize_space(item.get("sourceType")).lower() == "product"
            for item in group
        )
        if has_product:
            return "product"
        return "notice_item"
    return "product"


def choose_display_site(best: dict, group: list[dict]) -> tuple[str, str]:
    sources = collect_verification_sources(group)

    site_map = {
        "gundambase_notice": "건담베이스",
        "gundamshop": "건담샵",
        "bnkrmall": "반다이남코코리아몰",
        "smartstore": "스마트스토어",
    }

    labels = []
    for source in sources:
        label = site_map.get(source, source)
        if label and label not in labels:
            labels.append(label)

    if not labels:
        mall = normalize_space(best.get("mallName"))
        site = normalize_space(best.get("site"))
        source = normalize_space(best.get("source"))
        fallback = mall or site or source or "알 수 없음"
        return fallback, fallback

    if len(labels) == 1:
        return labels[0], labels[0]

    merged_label = f"{labels[0]} 외 {len(labels) - 1}곳"
    return merged_label, merged_label


def build_merged_doc(group: list[dict]) -> tuple[str, dict]:
    best = choose_best_source_item(group)

    name = choose_best_name(best)
    canonical = normalize_name(name)
    best_link = choose_best_link(best)
    status = choose_status(group)
    verification_sources = collect_verification_sources(group)
    source_type = choose_source_type(group)
    mall_name, site = choose_display_site(best, group)

    merged_id_base = canonical or name or best_link or sha1(str(group))
    merged_id = sha1(merged_id_base)

    # notice 전용 링크도 따로 모아두기
    notice_links = []
    for item in group:
        if normalize_space(item.get("sourceType")).lower() == "notice_item":
            link = choose_best_link(item)
            if link and link not in notice_links:
                notice_links.append(link)

    data = {
        "itemId": merged_id,
        "name": name,
        "title": normalize_space(best.get("title")) or name,
        "canonicalName": canonical,
        "price": choose_best_price(group),
        "stockText": status,
        "status": status,
        "source": normalize_space(best.get("source")),
        "sourceType": source_type,
        "site": site,
        "mallName": mall_name,
        "country": normalize_space(best.get("country")) or "KR",
        "region": normalize_space(best.get("region")) or "KR",
        "productUrl": best_link,
        "url": best_link,
        "link": best_link,
        "imageUrl": choose_best_image(group),
        "noticeDate": choose_best_notice_date(group),
        "noticeLinks": notice_links,
        "verificationSources": verification_sources,
        "verificationCount": len(verification_sources),
        "verificationStatus": (
            "cross_checked" if len(verification_sources) >= 2 else "single_source"
        ),
        "isNotice": is_notice_group(group),
        "mergedFromIds": [normalize_space(item.get("itemId")) for item in group if normalize_space(item.get("itemId"))],
        "updatedAt": firestore.SERVER_TIMESTAMP,
        "lastSeenAt": firestore.SERVER_TIMESTAMP,
    }

    # 루리웹 공지만 있는 경우엔 productUrl/url/link는 공지 링크가 들어감
    if not data["productUrl"] and notice_links:
        data["productUrl"] = notice_links[0]
        data["url"] = notice_links[0]
        data["link"] = notice_links[0]

    return merged_id, data


def main():
    db = init_firestore()

    source_docs = list(db.collection(SOURCE_COLLECTION).stream())
    print(f"원본 문서 수: {len(source_docs)}")

    groups = defaultdict(list)

    for doc in source_docs:
        try:
            data = doc.to_dict() or {}
            name = choose_best_name(data)
            canonical = normalize_name(name)

            if not canonical:
                continue

            if should_block_name(name, data):
                print(f"차단됨(그룹단계): {name}")
                continue

            groups[canonical].append(data)

        except Exception as e:
            print(f"문서 처리 실패: {e}")
            continue

    print(f"병합 그룹 수: {len(groups)}")

    # 기존 병합본 삭제
    existing_merged = list(db.collection(TARGET_COLLECTION).stream())
    print(f"기존 병합 문서 수: {len(existing_merged)}")

    batch = db.batch()
    op_count = 0

    for doc in existing_merged:
        batch.delete(doc.reference)
        op_count += 1

        if op_count >= 400:
            batch.commit()
            batch = db.batch()
            op_count = 0

    if op_count > 0:
        batch.commit()
        batch = db.batch()
        op_count = 0

    # 새 병합본 저장
    saved_count = 0

    for _, group in groups.items():
        merged_id, merged_data = build_merged_doc(group)

        if should_block_name(merged_data.get("name", ""), merged_data):
            print(f"차단됨(병합단계): {merged_data.get('name')}")
            continue
        ref = db.collection(TARGET_COLLECTION).document(merged_id)
        batch.set(ref, merged_data)
        op_count += 1
        saved_count += 1

        print(
            f"병합 저장: {merged_data['name']} / "
            f"{merged_data['status']} / "
            f"{merged_data['productUrl']}"
        )

        if op_count >= 400:
            batch.commit()
            batch = db.batch()
            op_count = 0

    if op_count > 0:
        batch.commit()

    print(f"[완료] 병합 완료 / 총 저장 {saved_count}개")


if __name__ == "__main__":
    main()
