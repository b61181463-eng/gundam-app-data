import requests

# 테스트할 사이트
url = "https://newtype.us/search?q=gundam"

headers = {
    "User-Agent": "Mozilla/5.0"
}

try:
    response = requests.get(url, headers=headers)

    print("상태 코드:", response.status_code)

    if response.status_code == 200:
        html = response.text

        print("\n페이지 일부 출력:\n")
        print(html[:1000])  # 앞부분 1000자만 출력

    else:
        print("사이트 접근 실패")

except Exception as e:
    print("에러 발생:", e)