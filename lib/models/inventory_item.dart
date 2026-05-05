import 'package:cloud_firestore/cloud_firestore.dart';

class InventoryItem {
  final String id;
  final String storeId;
  final String itemId;
  final String itemName;
  final String brand;
  final String grade;
  final bool inStock;
  final int quantity;
  final DateTime? updatedAt;

  const InventoryItem({
    required this.id,
    required this.storeId,
    required this.itemId,
    required this.itemName,
    required this.brand,
    required this.grade,
    required this.inStock,
    required this.quantity,
    required this.updatedAt,
  });

  factory InventoryItem.fromMap(Map<String, dynamic> map, String documentId) {
    final timestamp = map['updatedAt'];

    return InventoryItem(
      id: documentId,
      storeId: map['storeId'] ?? '',
      itemId: map['itemId'] ?? '',
      itemName: map['itemName'] ?? '',
      brand: map['brand'] ?? '',
      grade: map['grade'] ?? '',
      inStock: map['inStock'] ?? false,
      quantity: map['quantity'] ?? 0,
      updatedAt: timestamp is Timestamp ? timestamp.toDate() : null,
    );
  }

  Map<String, dynamic> toMap() {
    return {
      'storeId': storeId,
      'itemId': itemId,
      'itemName': itemName,
      'brand': brand,
      'grade': grade,
      'inStock': inStock,
      'quantity': quantity,
      'updatedAt': FieldValue.serverTimestamp(),
    };
  }
}