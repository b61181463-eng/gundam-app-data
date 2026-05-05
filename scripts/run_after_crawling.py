import subprocess
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent


def run_script(script_name: str):
    script_path = BASE_DIR / "scripts" / script_name

    print(f"\n실행 중: {script_name}")

    result = subprocess.run(
        [sys.executable, str(script_path)],
        cwd=str(BASE_DIR),
        text=True,
    )

    if result.returncode != 0:
        raise RuntimeError(f"{script_name} 실행 실패")

    print(f"완료: {script_name}")


def main():
    # 1. Firestore 데이터 정규화
    run_script("normalize_firestore_items.py")

    # 2. 재입고 / 가격하락 알림 감지
    run_script("detect_stock_alerts.py")

    print("\n전체 후처리 완료")


if __name__ == "__main__":
    main()