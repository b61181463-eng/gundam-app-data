# Gundam App Roadmap Patch

## 추가/보강한 기능

1. 가격 변동 기록
   - `lib/services/price_history_service.dart` 추가
   - 상품 목록을 불러올 때 최저가를 로컬에 자동 기록
   - 상품 카드의 `가격` 버튼에서 최근 가격 기록과 상승/하락 요약 확인

2. 유저 재고 제보
   - `lib/services/user_stock_report_service.dart` 추가
   - 상품 카드의 `제보` 버튼에서 “있어요/없어요”와 메모를 Firestore `user_stock_reports` 컬렉션에 저장

3. 기존 기능 유지
   - 기존 가격 비교, 최저가 바로가기, 찜, 재입고 감지, 공지/등급/검색 필터 구조는 그대로 유지
   - `lib/screens/home_screen.dart`만 최소 수정

## 최종 압축에서 제외한 것

- `serviceAccountKey.json`: Firebase Admin 비밀키라서 공유/압축본 포함 위험
- `.dart_tool/`, `build/`, `node_modules/`: 자동 생성/대용량 폴더
- `android/local.properties`: 개인 PC SDK 경로가 들어가는 로컬 파일

## 확인 필요

- 이 환경에는 Flutter/Dart 명령어가 없어 `flutter analyze`를 직접 실행하지 못했습니다.
- 네 PC에서 압축을 풀고 아래 명령어로 최종 확인하세요.

```bash
flutter pub get
flutter analyze
flutter run -d chrome
```

## Firestore 규칙 주의

`user_stock_reports` 컬렉션 쓰기가 막혀 있으면 앱에서 제보 저장 실패가 뜰 수 있습니다.
그 경우 Firestore Rules에서 해당 컬렉션의 create 권한을 열어야 합니다.

## 2026-05-04 Firestore 통일 패치

- `lib/services/firebase_service.dart`
  - Realtime Database import 제거
  - `stores` Firestore 컬렉션 기반 조회로 변경
- `lib/services/firebase_inventory_service.dart`
  - Realtime Database import 제거
  - `inventory_reports` Firestore 컬렉션에 `set(..., merge: true)` 방식으로 저장
- `test/widget_test.dart`
  - 존재하지 않는 `MyApp` 참조를 `GundamApp`으로 교체

### 현재 로드맵 반영 상태
- 가격 비교/최저가: 기존 `StockItem` 집계 구조 유지
- 가격 변동 기록: `PriceHistoryService` 유지
- 유저 재고 제보: `UserStockReportService` 유지, `user_stock_reports` Firestore 컬렉션 사용
- 찜/관심 상품: 기존 `favorites` Firestore 컬렉션 유지
- Realtime Database 의존성: 제거 완료
