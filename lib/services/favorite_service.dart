import 'dart:convert';

import 'package:crypto/crypto.dart';
import 'package:firebase_messaging/firebase_messaging.dart';
import 'package:flutter/foundation.dart';
import 'package:shared_preferences/shared_preferences.dart';

class FavoriteService {
  static const String _favoritesKey = 'favorite_item_ids_v2';

  static Future<Set<String>> loadFavorites() async {
    final prefs = await SharedPreferences.getInstance();
    final ids = prefs.getStringList(_favoritesKey) ?? [];
    return ids.toSet();
  }

  static String topicForItemId(String itemId) {
    final digest =
        sha1.convert(utf8.encode(itemId)).toString().substring(0, 20);
    return 'fav_$digest';
  }

  static Future<Set<String>> toggleFavorite(String itemId) async {
    final prefs = await SharedPreferences.getInstance();
    final favorites = await loadFavorites();

    final isRemoving = favorites.contains(itemId);

    if (isRemoving) {
      favorites.remove(itemId);
    } else {
      favorites.add(itemId);
    }

    await prefs.setStringList(_favoritesKey, favorites.toList());
    debugPrint('찜 저장 완료: $itemId / 현재 개수: ${favorites.length}');

    if (!kIsWeb) {
      try {
        final topic = topicForItemId(itemId);

        if (isRemoving) {
          await FirebaseMessaging.instance.unsubscribeFromTopic(topic);
          debugPrint('토픽 구독 해제 성공: $topic');
        } else {
          await FirebaseMessaging.instance.subscribeToTopic(topic);
          debugPrint('토픽 구독 성공: $topic');
        }
      } catch (e) {
        debugPrint('토픽 구독/해제 실패: $e');
      }
    }

    return favorites;
  }

  static Future<void> resubscribeAllFavorites() async {
    if (kIsWeb) return;

    final favorites = await loadFavorites();

    for (final itemId in favorites) {
      try {
        await FirebaseMessaging.instance
            .subscribeToTopic(topicForItemId(itemId));
      } catch (e) {
        debugPrint('재구독 실패: $itemId / $e');
      }
    }
  }
}