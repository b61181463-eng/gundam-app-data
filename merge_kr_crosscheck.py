import os
import json
import firebase_admin
from firebase_admin import credentials, firestore

def init_firestore():
    if not firebase_admin._apps:
        firebase_json = os.environ.get("FIREBASE_SERVICE_ACCOUNT_JSON")

        if firebase_json:
            # GitHub Actions / 환경변수용
            cred_dict = json.loads(firebase_json)
            cred = credentials.Certificate(cred_dict)
        else:
            # 로컬 실행용
            cred = credentials.Certificate("serviceAccountKey.json")

        firebase_admin.initialize_app(cred)

    return firestore.client()

INPUT_JSON_FILES = [
    "data/gundamshop_items.json",
    "data/gundambase_items.json",
    "data/bnkrmall_items.json",
]

OUTPUT_JSON_PATH = Path("data/aggregated_item_kr.json")

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

ALLOWED_HOSTS = {
    "gundamshop.co.kr",
    "www.gundamshop.co.kr",
    "thegundambase.com",
    "www.thegundambase.com",
    "bnkrmall.co.kr",
    "www.bnkrmall.co.kr",
    "ruliweb.com",
    "www.ruliweb.com",
    "bandai.co.kr",
    "www.bandai.co.kr",
    "bandaimall.co.kr",
    "www.bandaimall.co.kr",
}


def sha1(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


def normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()


def normalize_name(name: str) -> str:
    text = normalize_space(name).lower()
    text = text.replace("ver.", "ver")
    text = text.replace("version", "ver")
    text = re.sub(r"\b1/\d+\b", "", text)
    text = re.sub(r"\b\d+/\d+\b", "", text)
    text = re.sub(r"[\[\]\(\)\-_/.,:+]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def load_json_file(path: str) -> list[dict]:
    p = Path(path)
    if not p.exists():
        print(f"[경고] 입력 JSON 없음: {path}")
        return []

    try:
        with open(p, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            print(f"[로드] {path} / {len(data)}개")
            return data
        print(f"[경고] 리스트 형식 아님: {path}")
        return []
    except Exception as e:
        print(f"[경고] JSON 읽기 실패: {path} -> {e}")
        return []


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
            source in {"gundamshop", "bnkrmall", "gundambase_notice", "gundambase"}
            or "건담샵" in site
            or "반다이" in site
            or "건담베이스" in site
            or "루리웹" in site
            or "건담샵" in mall
            or "반다이" in mall
            or "건담베이스" in mall
            or "루리웹" in mall
        ):
            return True

    return False


def choose_best_link(data: dict) -> str:
    for key in ["productUrl", "url", "link"]:
        value = normalize_space(data.get(key))
        if value:
            return value
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
    for item in group:
        source_type = normalize_space(item.get("sourceType")).lower()
        link = choose_best_link(item)
        if source_type == "product" and link:
            return item

    for item in group:
        link = choose_best_link(item)
        if link:
            return item

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
    priority = ["판매중", "품절", "예약/입고예정"]

    product_statuses = []
    for item in group:
        source_type = normalize_space(item.get("sourceType")).lower()
        status = normalize_space(item.get("status") or item.get("stockText"))
        if source_type == "product" and status:
            product_statuses.append(status)

    for p in priority:
        for status in product_statuses:
            if status == p:
                return status

    statuses = []
    for item in group:
        status = normalize_space(item.get("status") or item.get("stockText"))
        if status:
            statuses.append(status)

    for p in priority:
        for status in statuses:
            if status == p:
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


def is_allowed_final_url(item: dict) -> bool:
    url = normalize_space(item.get("productUrl") or item.get("url") or item.get("link"))
    if not url:
        return False

    try:
        host = (urlparse(url).netloc or "").lower()
    except Exception:
        return False

    return host in ALLOWED_HOSTS


def build_merged_doc(group: list[dict]) -> tuple[str, dict]:
    best = choose_best_source_item(group)

    name = choose_best_name(best)
    canonical = normalize_name(name)
    best_link = choose_best_link(best)
    status = choose_status(group)
    verification_sources = collect_verification_sources(group)
    source_type = choose_source_type(group)

    mall_name = normalize_space(best.get("mallName")) or normalize_space(best.get("site")) or "알 수 없음"
    site = normalize_space(best.get("site")) or mall_name

    merged_id_base = f"{canonical}|{mall_name}" or name or best_link or sha1(str(group))
    merged_id = sha1(merged_id_base)

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
        "mergedFromIds": [
            normalize_space(item.get("itemId"))
            for item in group
            if normalize_space(item.get("itemId"))
        ],
    }

    if not data["productUrl"] and notice_links:
        data["productUrl"] = notice_links[0]
        data["url"] = notice_links[0]
        data["link"] = notice_links[0]

    return merged_id, data


def main():
    source_items = []
    for path in INPUT_JSON_FILES:
        source_items.extend(load_json_file(path))

    print(f"원본 항목 수: {len(source_items)}")

    groups = defaultdict(list)

    for data in source_items:
        try:
            name = choose_best_name(data)
            canonical = normalize_name(name)
            mall = normalize_space(data.get("mallName") or data.get("site") or "")

            if not canonical or not mall:
                continue

            if should_block_name(name, data):
                print(f"차단됨(그룹단계): {name}")
                continue

            group_key = f"{canonical}|{mall}"
            groups[group_key].append(data)

        except Exception as e:
            print(f"항목 처리 실패: {e}")
            continue

    print(f"병합 그룹 수: {len(groups)}")

    results = []
    blocked_count = 0

    for _, group in groups.items():
        merged_id, merged_data = build_merged_doc(group)

        if should_block_name(merged_data.get("name", ""), merged_data):
            print(f"차단됨(병합단계): {merged_data.get('name')}")
            blocked_count += 1
            continue

        if not is_allowed_final_url(merged_data):
            print(f"외부 URL 차단: {merged_data.get('name')} / {merged_data.get('productUrl')}")
            blocked_count += 1
            continue

        results.append(merged_data)

        print(
            f"병합 저장: {merged_data['name']} / "
            f"{merged_data['mallName']} / "
            f"{merged_data['status']} / "
            f"{merged_data['productUrl']}"
        )

    OUTPUT_JSON_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    print(f"[완료] 병합 완료 / 총 저장 {len(results)}개 / 차단 {blocked_count}개")
    print(f"[저장] JSON 저장 완료: {OUTPUT_JSON_PATH}")


if __name__ == "__main__":
    main()
