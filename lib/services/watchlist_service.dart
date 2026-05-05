import 'package:cloud_firestore/cloud_firestore.dart';

class WatchlistService {
  final FirebaseFirestore firestore;

  WatchlistService({FirebaseFirestore? firestore})
      : firestore = firestore ?? FirebaseFirestore.instance;

  String makeWatchlistDocId(String userId, String productId) {
    return '${userId}_$productId';
  }

  Future<void> addToWatchlist({
    required String userId,
    required String productId,
    required String productName,
    required String site,
  }) async {
    final docId = makeWatchlistDocId(userId, productId);

    await firestore.collection('watchlists').doc(docId).set({
      'userId': userId,
      'productId': productId,
      'productName': productName,
      'site': site,
      'createdAt': FieldValue.serverTimestamp(),
    });
  }

  Future<void> removeFromWatchlist({
    required String userId,
    required String productId,
  }) async {
    final docId = makeWatchlistDocId(userId, productId);

    await firestore.collection('watchlists').doc(docId).delete();
  }

  Stream<bool> isWatching({
    required String userId,
    required String productId,
  }) {
    final docId = makeWatchlistDocId(userId, productId);

    return firestore
        .collection('watchlists')
        .doc(docId)
        .snapshots()
        .map((doc) => doc.exists);
  }

  Stream<QuerySnapshot> watchUserWatchlist(String userId) {
    return firestore
        .collection('watchlists')
        .where('userId', isEqualTo: userId)
        .snapshots();
  }

  Future<List<String>> getWatchingUserIds(String productId) async {
    final snapshot = await firestore
        .collection('watchlists')
        .where('productId', isEqualTo: productId)
        .get();

    return snapshot.docs
        .map((doc) => (doc.data())['userId'] as String)
        .toList();
  }
}