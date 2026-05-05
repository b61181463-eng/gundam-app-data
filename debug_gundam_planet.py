import requests

url = "https://www.gundamplanet.com/collections/sale-all-items"

headers = {
    "User-Agent": "Mozilla/5.0"
}

response = requests.get(url, headers=headers, timeout=20)

print("상태 코드:", response.status_code)

with open("gundam_planet_debug.html", "w", encoding="utf-8") as f:
    f.write(response.text)

print("gundam_planet_debug.html 저장 완료")
print("앞부분 2000자:\n")
print(response.text[:2000])