import 'package:cloud_firestore/cloud_firestore.dart';
import '../models/stock_item.dart';

class StockChangeResult {
  final bool changed;
  final String oldStatus;
  final String newStatus;
  final String eventType;

  StockChangeResult({
    required this.changed,
    required this.oldStatus,
    required this.newStatus,
    required this.eventType,
  });
}

class StockService {
  final FirebaseFirestore firestore;

  StockService({FirebaseFirestore? firestore})
      : firestore = firestore ?? FirebaseFirestore.instance;

  String normalizeName(String name) {
    return name
        .toLowerCase()
        .replaceAll(RegExp(r'\s+'), ' ')
        .replaceAll(RegExp(r'[^a-z0-9가-힣/\-\s]'), '')
        .trim();
  }

  String normalizeStockStatus(String rawText) {
    final text = rawText.toLowerCase().trim();

    if (text.isEmpty) return 'unknown';

    if (text.contains('in stock') ||
        text.contains('available') ||
        text.contains('재고 있음') ||
        text.contains('구매 가능') ||
        text.contains('판매중') ||
        text.contains('재고있음')) {
      return 'in_stock';
    }

    if (text.contains('out of stock') ||
        text.contains('sold out') ||
        text.contains('품절') ||
        text.contains('일시품절') ||
        text.contains('재고 없음') ||
        text.contains('재고없음')) {
      return 'out_of_stock';
    }

    return 'unknown';
  }

  String stockFromQuantity(int qty) {
    if (qty > 0) return 'in_stock';
    if (qty == 0) return 'out_of_stock';
    return 'unknown';
  }

  String makeProductId(String site, String productUrl) {
    return '${site}_${productUrl.hashCode.abs()}';
  }

  StockChangeResult compareStockStatus({
    required String oldStatus,
    required String newStatus,
  }) {
    if (oldStatus == newStatus) {
      return StockChangeResult(
        changed: false,
        oldStatus: oldStatus,
        newStatus: newStatus,
        eventType: 'none',
      );
    }

    String eventType = 'none';

    if (oldStatus == 'out_of_stock' && newStatus == 'in_stock') {
      eventType = 'restock';
    } else if (oldStatus == 'in_stock' && newStatus == 'out_of_stock') {
      eventType = 'soldout';
    } else if (oldStatus == 'unknown' && newStatus == 'in_stock') {
      eventType = 'new_stock';
    }

    return StockChangeResult(
      changed: true,
      oldStatus: oldStatus,
      newStatus: newStatus,
      eventType: eventType,
    );
  }

  Future<void> processStockItem(StockItem item) async {
    final productRef = firestore.collection('products').doc(item.productId);
    final productSnap = await productRef.get();

    String oldStatus = 'unknown';

    if (productSnap.exists) {
      final data = productSnap.data()!;
      oldStatus = data['latestStockStatus'] ?? 'unknown';
    }

    final result = compareStockStatus(
      oldStatus: oldStatus,
      newStatus: item.stockStatus,
    );

    await productRef.set({
      'productId': item.productId,
      'site': item.site,
      'name': item.name,
      'normalizedName': item.normalizedName,
      'productUrl': item.productUrl,
      'imageUrl': item.imageUrl,
      'latestStockStatus': item.stockStatus,
      'latestRawStockText': item.rawStockText,
      'lastCheckedAt': FieldValue.serverTimestamp(),
      'updatedAt': FieldValue.serverTimestamp(),
    }, SetOptions(merge: true));

    await firestore.collection('stock_history').add({
      'productId': item.productId,
      'site': item.site,
      'name': item.name,
      'checkedAt': FieldValue.serverTimestamp(),
      'stockStatus': item.stockStatus,
      'rawStockText': item.rawStockText,
    });

    if (result.changed && result.eventType != 'none') {
      final watchSnapshot = await firestore
          .collection('watchlists')
          .where('productId', isEqualTo: item.productId)
          .get();

      final targetUserIds = watchSnapshot.docs
          .map((doc) => (doc.data())['userId'] as String)
          .toList();

      final now = DateTime.now();
      final duplicateCheckFrom = Timestamp.fromDate(
        now.subtract(const Duration(minutes: 30)),
      );

      final duplicateSnapshot = await firestore
          .collection('restock_events')
          .where('productId', isEqualTo: item.productId)
          .where('type', isEqualTo: result.eventType)
          .where('fromStatus', isEqualTo: result.oldStatus)
          .where('toStatus', isEqualTo: result.newStatus)
          .where('detectedAt', isGreaterThanOrEqualTo: duplicateCheckFrom)
          .limit(1)
          .get();

      final isDuplicate = duplicateSnapshot.docs.isNotEmpty;

      if (!isDuplicate) {
        await firestore.collection('restock_events').add({
          'productId': item.productId,
          'site': item.site,
          'name': item.name,
          'fromStatus': result.oldStatus,
          'toStatus': result.newStatus,
          'type': result.eventType,
          'targetUserIds': targetUserIds,
          'detectedAt': FieldValue.serverTimestamp(),
          'isRead': false,
        });
      }
    }
  }

  Future<void> processBatch(List<Map<String, dynamic>> crawledItems) async {
    for (final raw in crawledItems) {
      final site = raw['site'] ?? 'unknown_site';
      final name = raw['name'] ?? '이름 없음';
      final productUrl = raw['productUrl'] ?? '';
      final imageUrl = raw['imageUrl'] ?? '';
      final rawStockText = raw['rawStockText'] ?? '';

      final normalizedName = normalizeName(name);
      final productId = makeProductId(site, productUrl);
      final stockStatus = normalizeStockStatus(rawStockText);

      final item = StockItem(
        productId: productId,
        site: site,
        name: name,
        normalizedName: normalizedName,
        productUrl: productUrl,
        imageUrl: imageUrl,
        rawStockText: rawStockText,
        stockStatus: stockStatus,
      );

      await processStockItem(item);
    }
  }

  Future<void> insertDummyTestData() async {
    final testItems = [
      {
        'site': 'Gundam Planet',
        'name': 'RG 1/144 Unicorn Gundam',
        'productUrl': 'https://example.com/unicorn',
        'imageUrl': 'https://via.placeholder.com/300',
        'rawStockText': 'Out of Stock',
      },
      {
        'site': 'Gundam Planet',
        'name': 'MG 1/100 Barbatos',
        'productUrl': 'https://example.com/barbatos',
        'imageUrl': 'https://via.placeholder.com/300',
        'rawStockText': 'In Stock',
      },
    ];

    await processBatch(testItems);
  }

  Future<void> simulateRestockForUnicorn() async {
    final site = 'Gundam Planet';
    final name = 'RG 1/144 Unicorn Gundam';
    final productUrl = 'https://example.com/unicorn';

    final item = StockItem(
      productId: makeProductId(site, productUrl),
      site: site,
      name: name,
      normalizedName: normalizeName(name),
      productUrl: productUrl,
      imageUrl: 'https://via.placeholder.com/300',
      rawStockText: 'In Stock',
      stockStatus: 'in_stock',
    );

    await processStockItem(item);
  }

  Future<void> resetTestData() async {
    final site = 'Gundam Planet';
    final name = 'RG 1/144 Unicorn Gundam';
    final productUrl = 'https://example.com/unicorn';
    final productId = makeProductId(site, productUrl);

    await firestore.collection('products').doc(productId).set({
      'productId': productId,
      'site': site,
      'name': name,
      'normalizedName': normalizeName(name),
      'productUrl': productUrl,
      'imageUrl': 'https://via.placeholder.com/300',
      'latestStockStatus': 'out_of_stock',
      'latestRawStockText': 'Out of Stock',
      'lastCheckedAt': FieldValue.serverTimestamp(),
      'updatedAt': FieldValue.serverTimestamp(),
    }, SetOptions(merge: true));

    await firestore.collection('stock_history').add({
      'productId': productId,
      'site': site,
      'name': name,
      'checkedAt': FieldValue.serverTimestamp(),
      'stockStatus': 'out_of_stock',
      'rawStockText': 'Out of Stock',
    });
  }

  String statusLabel(String status) {
    switch (status) {
      case 'in_stock':
        return '재고 있음';
      case 'out_of_stock':
        return '품절';
      default:
        return '확인 불가';
    }
  }

  String eventLabel(String type) {
    switch (type) {
      case 'restock':
        return '재입고';
      case 'soldout':
        return '품절 전환';
      case 'new_stock':
        return '신규 재고 감지';
      default:
        return '변화 없음';
    }
  }
}