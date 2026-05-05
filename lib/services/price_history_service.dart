import 'dart:convert';

import 'package:shared_preferences/shared_preferences.dart';

import 'stock_api.dart';

class PriceHistoryPoint {
  final int price;
  final DateTime checkedAt;

  const PriceHistoryPoint({
    required this.price,
    required this.checkedAt,
  });

  factory PriceHistoryPoint.fromMap(Map<String, dynamic> map) {
    return PriceHistoryPoint(
      price: map['price'] is num ? (map['price'] as num).toInt() : 0,
      checkedAt: DateTime.tryParse((map['checkedAt'] ?? '').toString()) ??
          DateTime.fromMillisecondsSinceEpoch(0),
    );
  }

  Map<String, dynamic> toMap() {
    return {
      'price': price,
      'checkedAt': checkedAt.toIso8601String(),
    };
  }
}

class PriceHistoryService {
  static const String _key = 'price_history_by_item_v1';
  static const int _maxPointsPerItem = 30;

  static int? priceToInt(String priceText) {
    final digits = priceText.replaceAll(RegExp(r'[^0-9]'), '');
    if (digits.isEmpty) return null;
    return int.tryParse(digits);
  }

  static String formatWon(int value) {
    final text = value.toString();
    final reg = RegExp(r'\B(?=(\d{3})+(?!\d))');
    return "${text.replaceAllMapped(reg, (_) => ',')}원";
  }

  static Future<Map<String, List<PriceHistoryPoint>>> _loadAll() async {
    final prefs = await SharedPreferences.getInstance();
    final raw = prefs.getString(_key);
    if (raw == null || raw.isEmpty) return {};

    try {
      final decoded = jsonDecode(raw) as Map<String, dynamic>;
      return decoded.map((key, value) {
        final list = value is List ? value : const [];
        return MapEntry(
          key,
          list
              .whereType<Map>()
              .map((e) => PriceHistoryPoint.fromMap(Map<String, dynamic>.from(e)))
              .where((e) => e.price > 0)
              .toList(),
        );
      });
    } catch (_) {
      return {};
    }
  }

  static Future<void> _saveAll(
    Map<String, List<PriceHistoryPoint>> all,
  ) async {
    final prefs = await SharedPreferences.getInstance();
    final encoded = all.map(
      (key, value) => MapEntry(key, value.map((e) => e.toMap()).toList()),
    );
    await prefs.setString(_key, jsonEncode(encoded));
  }

  static Future<void> recordSnapshots(List<StockItem> items) async {
    final all = await _loadAll();
    var changed = false;
    final now = DateTime.now();

    for (final item in items) {
      if (item.status == '공지') continue;
      final price = priceToInt(item.minPrice.isNotEmpty ? item.minPrice : item.price);
      if (price == null || price <= 0) continue;

      final history = List<PriceHistoryPoint>.from(all[item.itemId] ?? const []);
      if (history.isNotEmpty && history.last.price == price) continue;

      history.add(PriceHistoryPoint(price: price, checkedAt: now));
      if (history.length > _maxPointsPerItem) {
        history.removeRange(0, history.length - _maxPointsPerItem);
      }
      all[item.itemId] = history;
      changed = true;
    }

    if (changed) await _saveAll(all);
  }

  static Future<List<PriceHistoryPoint>> loadHistory(String itemId) async {
    final all = await _loadAll();
    return List<PriceHistoryPoint>.from(all[itemId] ?? const []);
  }

  static Future<String> trendLabel(String itemId) async {
    final history = await loadHistory(itemId);
    if (history.length < 2) return '가격 기록 부족';

    final first = history.first.price;
    final last = history.last.price;
    final diff = last - first;

    if (diff == 0) return '가격 유지';
    if (diff < 0) return '${formatWon(diff.abs())} 하락';
    return '${formatWon(diff)} 상승';
  }
}
