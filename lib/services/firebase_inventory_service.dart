import 'package:cloud_firestore/cloud_firestore.dart';

/// Firestore-only inventory report writer.
///
/// Stores local/admin inventory reports in `inventory_reports`. User-facing
/// stock sightings are handled separately by `UserStockReportService` in
/// `user_stock_reports`.
class FirebaseInventoryService {
  static final FirebaseFirestore _db = FirebaseFirestore.instance;

  static Future<void> saveReport({
    required String countryCode,
    required String storeName,
    required String itemName,
    required bool inStock,
  }) async {
    final docId = _safeDocId('$countryCode|$storeName|$itemName');

    await _db.collection('inventory_reports').doc(docId).set({
      'countryCode': countryCode,
      'storeName': storeName,
      'itemName': itemName,
      'inStock': inStock,
      'updatedAt': FieldValue.serverTimestamp(),
    }, SetOptions(merge: true));
  }

  static String _safeDocId(String value) {
    return value
        .trim()
        .replaceAll(RegExp(r'[/#?\\]'), '_')
        .replaceAll(RegExp(r'\s+'), ' ');
  }
}
