import 'package:flutter/foundation.dart';

class InventoryReport {
  final bool inStock;
  final String updatedAt;

  const InventoryReport({
    required this.inStock,
    required this.updatedAt,
  });
}

class InventoryReportService {
  static final ValueNotifier<Map<String, InventoryReport>> reports =
      ValueNotifier(<String, InventoryReport>{});

  static String makeItemId({
    required String countryCode,
    required String storeName,
    required String itemName,
  }) {
    return '$countryCode|$storeName|$itemName';
  }

  static InventoryReport? getReport(String itemId) {
    return reports.value[itemId];
  }

  static void report({
    required String itemId,
    required bool inStock,
  }) {
    final current = Map<String, InventoryReport>.from(reports.value);
    current[itemId] = InventoryReport(
      inStock: inStock,
      updatedAt: '방금 전',
    );
    reports.value = current;
  }
}