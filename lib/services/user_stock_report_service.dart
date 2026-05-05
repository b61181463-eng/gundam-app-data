import 'package:cloud_firestore/cloud_firestore.dart';

import 'stock_api.dart';

class UserStockReportService {
  static final FirebaseFirestore _db = FirebaseFirestore.instance;

  static Future<void> submitReport({
    required StockItem item,
    required bool inStock,
    String memo = '',
  }) async {
    await _db.collection('user_stock_reports').add({
      'itemId': item.itemId,
      'name': item.name.isNotEmpty ? item.name : item.title,
      'seller': item.mallName.isNotEmpty ? item.mallName : item.site,
      'resolvedUrl': item.resolvedUrl,
      'reportedInStock': inStock,
      'memo': memo.trim(),
      'createdAt': FieldValue.serverTimestamp(),
    });
  }
}
