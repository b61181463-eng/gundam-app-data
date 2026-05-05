import re
import requests
from bs4 import BeautifulSoup

url = "https://newtype.us/search?q=gundam"

headers = {
    "User-Agent": "Mozilla/5.0"
}

def clean_text(text):
    return re.sub(r"\s+", " ", text).strip()

def main():
    try:
        response = requests.get(url, headers=headers, timeout=20)
        print("상태 코드:", response.status_code)

        if response.status_code != 200:
            print("사이트 접근 실패")
            return

        soup = BeautifulSoup(response.text, "html.parser")

        # 재고 상태로 보이는 텍스트 찾기
        stock_strings = soup.find_all(
            string=re.compile(
                r"(In stock|Out of stock|Only\s+\d+\s+left|<\s*\d+\s+left)",
                re.I
            )
        )

        print(f"재고 관련 텍스트 개수: {len(stock_strings)}\n")

        if not stock_strings:
            print("재고 텍스트를 찾지 못했어.")
            return

        found_count = 0
        seen_names = set()

        for stock_text_node in stock_strings:
            stock_text = clean_text(stock_text_node)

            node = stock_text_node.parent
            product_block = None

            # 상위 태그 몇 번 올라가면서 상품 카드 후보 찾기
            for _ in range(6):
                if node is None:
                    break

                links = node.find_all("a")
                if links:
                    product_block = node
                    break

                node = node.parent

            if product_block is None:
                continue

            # 링크 텍스트 중 상품명처럼 보이는 것 찾기
            link_texts = []
            for a in product_block.find_all("a"):
                text = clean_text(a.get_text(" ", strip=True))
                if text:
                    link_texts.append(text)

            candidates = []
            for text in link_texts:
                lower = text.lower()

                if lower in {"add to bag", "yes", "no"}:
                    continue

                if re.fullmatch(
                    r"(In stock|Out of stock|Only\s+\d+\s+left|<\s*\d+\s+left)",
                    text,
                    re.I
                ):
                    continue

                if len(text) < 4:
                    continue

                candidates.append(text)

            if not candidates:
                continue

            # 가장 긴 텍스트를 상품명으로 가정
            product_name = sorted(candidates, key=len, reverse=True)[0]

            if product_name in seen_names:
                continue

            seen_names.add(product_name)
            found_count += 1

            print(f"[{found_count}] 상품명: {product_name}")
            print(f"    재고상태: {stock_text}")
            print()

            # 너무 많이 나오면 일단 20개까지만 출력
            if found_count >= 20:
                break

        if found_count == 0:
            print("상품명과 재고 상태를 같이 뽑지 못했어.")
        else:
            print(f"총 {found_count}개 추출 성공")

    except Exception as e:
        print("에러 발생:", e)

if __name__ == "__main__":
    main()