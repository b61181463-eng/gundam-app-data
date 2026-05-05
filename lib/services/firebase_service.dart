import 'package:cloud_firestore/cloud_firestore.dart';

/// Firestore-only service for store metadata.
///
/// Legacy versions of this project used Firebase Realtime Database. The app is
/// now standardized on Cloud Firestore, so this class reads from the `stores`
/// collection and returns the same map-like shape older UI code expected.
class FirebaseService {
  final FirebaseFirestore _db = FirebaseFirestore.instance;

  Future<Map<String, dynamic>?> getStores(String countryCode) async {
    final snapshot = await _db
        .collection('stores')
        .where('countryCode', isEqualTo: countryCode)
        .get();

    if (snapshot.docs.isEmpty) return null;

    return {
      for (final doc in snapshot.docs) doc.id: doc.data(),
    };
  }
}
