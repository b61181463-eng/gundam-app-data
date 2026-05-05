from pathlib import Path
import firebase_admin
from firebase_admin import credentials, firestore, messaging

BASE_DIR = Path(__file__).resolve().parent.parent
SERVICE_ACCOUNT_PATH = BASE_DIR / "serviceAccountKey.json"
ITEM_COLLECTION = "aggregated_items"


def init_firebase():
    if not firebase_admin._apps:
        cred = credentials.Certificate(str(SERVICE_ACCOUNT_PATH))
        firebase_admin.initialize_app(cred)
    return firestore.client()


def get_tokens(db):
    return [
        d.to_dict().get("token")
        for d in db.collection("user_push_tokens").stream()
        if d.to_dict().get("token")
    ]


def send_push(tokens, title, body):
    if not tokens:
        print("토큰 없음: 푸시는 건너뜀")
        return

    for token in tokens:
        try:
            messaging.send(
                messaging.Message(
                    notification=messaging.Notification(
                        title=title,
                        body=body,
                    ),
                    token=token,
                )
            )
            print(f"푸시 발송 완료: {title}")
        except Exception as e:
            print(f"푸시 실패: {e}")


def main():
    db = init_firebase()
    tokens = get_tokens(db)

    docs = list(db.collection(ITEM_COLLECTION).stream())
    print(f"검사 상품 수: {len(docs)}")

    alert_count = 0

    for doc in docs:
        data = doc.to_dict()

        name = data.get("name", "이름 없는 상품")
        status = data.get("normalizedStatus") or data.get("status")
        price = data.get("normalizedPrice") or data.get("price")

        last_status = data.get("lastAlertStatus")
        last_price = data.get("lastAlertPrice")

        alerts = []

        # 재입고 감지
        if last_status and last_status != "판매중" and status == "판매중":
            alerts.append({
                "type": "restock",
                "title": "재입고 알림",
                "body": f"{name} 상품이 판매중으로 바뀌었습니다.",
            })

        # 가격 하락 감지
        if isinstance(price, int) and isinstance(last_price, int):
            if price < last_price:
                alerts.append({
                    "type": "price_drop",
                    "title": "가격 하락 알림",
                    "body": f"{name} 가격이 {last_price:,}원 → {price:,}원으로 내려갔습니다.",
                })

        for alert in alerts:
            db.collection("app_notifications").add({
                "itemId": doc.id,
                "name": name,
                "type": alert["type"],
                "title": alert["title"],
                "body": alert["body"],
                "createdAt": firestore.SERVER_TIMESTAMP,
                "read": False,
            })

            send_push(tokens, alert["title"], alert["body"])
            alert_count += 1

        doc.reference.set({
            "lastAlertStatus": status,
            "lastAlertPrice": price,
            "lastAlertCheckedAt": firestore.SERVER_TIMESTAMP,
        }, merge=True)

    print(f"생성된 알림 수: {alert_count}")


if __name__ == "__main__":
    main()