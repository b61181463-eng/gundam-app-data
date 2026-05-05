import 'package:cloud_firestore/cloud_firestore.dart';
import 'package:firebase_messaging/firebase_messaging.dart';

class PushNotificationService {
  static final FirebaseMessaging _messaging = FirebaseMessaging.instance;
  static final FirebaseFirestore _db = FirebaseFirestore.instance;

  static Future<void> init() async {
    // 알림 권한 요청
    await _messaging.requestPermission(
      alert: true,
      badge: true,
      sound: true,
    );

    // Android FCM 토큰 가져오기
    final token = await _messaging.getToken();

    if (token != null) {
      await saveToken(token);
    }

    // 토큰이 바뀌면 다시 저장
    FirebaseMessaging.instance.onTokenRefresh.listen((newToken) async {
      await saveToken(newToken);
    });

    // 앱 켜져 있을 때 알림 수신
    FirebaseMessaging.onMessage.listen((RemoteMessage message) {
      print('FCM foreground message: ${message.notification?.title}');
    });

    // 알림 눌러서 앱 열었을 때
    FirebaseMessaging.onMessageOpenedApp.listen((RemoteMessage message) {
      print('FCM opened message: ${message.notification?.title}');
    });
  }

  static Future<void> saveToken(String token) async {
    await _db.collection('user_push_tokens').doc(token).set({
      'token': token,
      'platform': 'android',
      'createdAt': FieldValue.serverTimestamp(),
      'updatedAt': FieldValue.serverTimestamp(),
    }, SetOptions(merge: true));
  }
}