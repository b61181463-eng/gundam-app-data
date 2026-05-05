from pathlib import Path

import firebase_admin
from firebase_admin import credentials, firestore, messaging


BASE_DIR = Path(__file__).resolve().parent.parent
SERVICE_ACCOUNT_PATH = BASE_DIR / "serviceAccountKey.json"


def init_firebase():
    if not firebase_admin._apps:
        cred = credentials.Certificate(str(SERVICE_ACCOUNT_PATH))
        firebase_admin.initialize_app(cred)

    return firestore.client()


def get_tokens(db):
    docs = db.collection("user_push_tokens").stream()
    tokens = []

    for doc in docs:
        data = doc.to_dict()
        token = data.get("token")
        if token:
            tokens.append(token)

    return tokens


def send_push(tokens, title, body):
    if not tokens:
        print("저장된 FCM 토큰이 없습니다.")
        return

    for token in tokens:
        message = messaging.Message(
            notification=messaging.Notification(
                title=title,
                body=body,
            ),
            token=token,
        )

        try:
            response = messaging.send(message)
            print(f"발송 성공: {response}")
        except Exception as e:
            print(f"발송 실패: {e}")


def main():
    db = init_firebase()

    tokens = get_tokens(db)

    print(f"토큰 수: {len(tokens)}")

    send_push(
        tokens=tokens,
        title="건담 재고 알림 테스트",
        body="푸시 발송 엔진이 정상 작동하는지 확인 중입니다.",
    )


if __name__ == "__main__":
    main()