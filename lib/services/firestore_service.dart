import 'package:cloud_firestore/cloud_firestore.dart';
import '../models/store_model.dart';
import '../models/inventory_item.dart';

class FirestoreService {
  final FirebaseFirestore _db = FirebaseFirestore.instance;

  // ======================
  // 컬렉션 참조
  // ======================

  CollectionReference<Map<String, dynamic>> get _stores =>
      _db.collection('stores');

  CollectionReference<Map<String, dynamic>> get _inventory =>
      _db.collection('inventory');

  // ======================
  // 매장 조회
  // ======================

  Stream<List<StoreModel>> getStoresByCountry(String countryCode) {
    return _stores
        .where('countryCode', isEqualTo: countryCode)
        .snapshots()
        .map((snapshot) {
      return snapshot.docs
          .map((doc) => StoreModel.fromMap(doc.data(), doc.id))
          .toList();
    });
  }

  Stream<List<StoreModel>> searchStores({
    required String countryCode,
    required String query,
  }) {
    return getStoresByCountry(countryCode).map((stores) {
      if (query.trim().isEmpty) return stores;

      final q = query.toLowerCase().trim();

      return stores.where((store) {
        return store.name.toLowerCase().contains(q) ||
            store.city.toLowerCase().contains(q) ||
            store.keywords.any((k) => k.toLowerCase().contains(q));
      }).toList();
    });
  }

  // ======================
  // 재고 조회
  // ======================

  Stream<List<InventoryItem>> getInventoryByStore(String storeId) {
    return _inventory
        .where('storeId', isEqualTo: storeId)
        .snapshots()
        .map((snapshot) {
      return snapshot.docs
          .map((doc) => InventoryItem.fromMap(doc.data(), doc.id))
          .toList();
    });
  }

  Stream<List<InventoryItem>> getFilteredInventory({
    required String storeId,
    String? keyword,
    bool inStockOnly = false,
  }) {
    return getInventoryByStore(storeId).map((items) {
      var result = items;

      if (inStockOnly) {
        result = result.where((item) => item.inStock).toList();
      }

      if (keyword != null && keyword.trim().isNotEmpty) {
        final q = keyword.toLowerCase().trim();
        result = result.where((item) {
          return item.itemName.toLowerCase().contains(q) ||
              item.grade.toLowerCase().contains(q) ||
              item.brand.toLowerCase().contains(q);
        }).toList();
      }

      result.sort((a, b) => a.itemName.compareTo(b.itemName));
      return result;
    });
  }

  // ======================
  // 재고 수정
  // ======================

  Future<void> updateInventory({
    required String docId,
    required bool inStock,
    required int quantity,
  }) async {
    await _inventory.doc(docId).update({
      'inStock': inStock,
      'quantity': quantity,
      'updatedAt': FieldValue.serverTimestamp(),
    });
  }

  // ======================
  // 재고 추가 ⭐ (지금 문제였던 부분)
  // ======================

  Future<void> createInventoryItem({
    required String storeId,
    required String itemId,
    required String itemName,
    required String brand,
    required String grade,
    required int quantity,
  }) async {
    final bool inStock = quantity > 0;
    final String docId = '${storeId}_$itemId';

    await _inventory.doc(docId).set({
      'storeId': storeId,
      'itemId': itemId,
      'itemName': itemName,
      'brand': brand,
      'grade': grade,
      'inStock': inStock,
      'quantity': quantity,
      'updatedAt': FieldValue.serverTimestamp(),
    });
  }
  Future<void> deleteInventory(String docId) async {
    await _inventory.doc(docId).delete();
  }
  Stream<List<String>> getFavoriteItemIds(String userId) {
      return _db
        .collection('favorites')
        .where('userId', isEqualTo: userId)
        .snapshots()
        .map((snapshot) {
      return snapshot.docs.map((doc) => doc.data()['itemId'] as String).toList();
    });
  }

  Future<void> addFavorite({
    required String userId,
    required String storeId,
    required String itemId,
    required String itemName,
  }) async {
    final docId = '${userId}_${storeId}_$itemId';

    await _db.collection('favorites').doc(docId).set({
      'userId': userId,
      'storeId': storeId,
      'itemId': itemId,
      'itemName': itemName,
      'createdAt': FieldValue.serverTimestamp(),
    });
  }

  Future<void> removeFavorite({
    required String userId,
    required String storeId,
    required String itemId,
  }) async {
    final docId = '${userId}_${storeId}_$itemId';
    await _db.collection('favorites').doc(docId).delete();
  }

  Future<void> toggleFavorite({
    required String userId,
    required String storeId,
    required String itemId,
    required String itemName,
    required bool isFavorite,
  }) async {
    if (isFavorite) {
      await removeFavorite(
        userId: userId,
        storeId: storeId,
        itemId: itemId,
      );
    } else {
      await addFavorite(
        userId: userId,
        storeId: storeId,
        itemId: itemId,
        itemName: itemName,
      );
    }
  }
}