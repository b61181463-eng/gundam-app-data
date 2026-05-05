class AutoUpdateService {
  AutoUpdateService._();
  static final AutoUpdateService instance = AutoUpdateService._();

  void start() {
    // Node 서버가 Firestore에 직접 저장하므로 앱에서는 아무것도 안 함
  }

  void stop() {}
}