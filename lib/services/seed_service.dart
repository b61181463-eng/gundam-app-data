import 'dart:convert';
import 'package:flutter/services.dart';
import 'package:cloud_firestore/cloud_firestore.dart';

class SeedService {
  final FirebaseFirestore _db = FirebaseFirestore.instance;

  Future<void> seedFromAsset(String assetPath) async {
    final jsonString = await rootBundle.loadString(assetPath);
    final Map<String, dynamic> data = jsonDecode(jsonString);

    final stores = List<Map<String, dynamic>>.from(data['stores'] ?? []);
    final inventory = List<Map<String, dynamic>>.from(data['inventory'] ?? []);

    final batch = _db.batch();

    for (final store in stores) {
      final storeId = store['id'] as String;
      final ref = _db.collection('stores').doc(storeId);
      batch.set(ref, store);
    }

    for (final item in inventory) {
      final docId = item['id'] as String;
      final ref = _db.collection('inventory').doc(docId);

      final payload = {
        ...item,
        'updatedAt': FieldValue.serverTimestamp(),
      };

      batch.set(ref, payload);
    }

    await batch.commit();
  }
}