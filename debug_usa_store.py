import requests

url = "https://www.usagundamstore.com/pages/search-results-page?q=rx-78-2+gundam"

headers = {
    "User-Agent": "Mozilla/5.0"
}

response = requests.get(url, headers=headers, timeout=20)

print("상태 코드:", response.status_code)

with open("usa_store_debug.html", "w", encoding="utf-8") as f:
    f.write(response.text)

print("usa_store_debug.html 저장 완료")
print("앞부분 2000자:\n")
print(response.text[:2000])