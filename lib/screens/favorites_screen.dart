import 'package:flutter/material.dart';
import 'package:cloud_firestore/cloud_firestore.dart';

class FavoritesScreen extends StatelessWidget {
  const FavoritesScreen({super.key});

  String getStatusText(String status) {
    switch (status) {
      case "in_stock":
        return "재고 있음";
      case "out_of_stock":
        return "품절";
      case "low_stock":
        return "재고 적음";
      case "check_required":
        return "확인 필요";
      default:
        return "정보 없음";
    }
  }

  Color getStatusColor(String status) {
    switch (status) {
      case "in_stock":
        return Colors.green;
      case "out_of_stock":
        return Colors.red;
      case "low_stock":
        return Colors.orange;
      case "check_required":
        return Colors.blue;
      default:
        return Colors.grey;
    }
  }

  String normalizeName(String name) {
    var text = name.toLowerCase().trim();

    final removeWords = [
      "bandai spirits",
      "bandai",
      "model kit",
      "plastic model",
      "plamo",
      "pre-order",
      "preorder",
      "gundam planet",
      "usa gundam store",
      "newtype",
    ];

    for (final word in removeWords) {
      text = text.replaceAll(word, " ");
    }

    text = text.replaceAll(RegExp(r"\(.*?\)"), " ");

    final replacements = {
      "mobile suit gundam": "gundam",
      "rx 78 2": "rx-78-2",
      "rx78 2": "rx-78-2",
      "rx 78-2": "rx-78-2",
      "rx78-2": "rx-78-2",
      "msn 04": "msn-04",
      "msn04": "msn-04",
      "hguc": "hg",
    };

    replacements.forEach((oldValue, newValue) {
      text = text.replaceAll(oldValue, newValue);
    });

    text = text.replaceAll(RegExp(r"[^a-z0-9가-힣/\-\s]"), " ");
    text = text.replaceAll(RegExp(r"\s+"), " ").trim();

    return text;
  }

  Future<void> _removeFavorite(String docId, BuildContext context) async {
    await FirebaseFirestore.instance.collection('favorites').doc(docId).delete();

    ScaffoldMessenger.of(context).showSnackBar(
      const SnackBar(content: Text('찜 해제됨')),
    );
  }

  @override
  Widget build(BuildContext context) {
    return StreamBuilder<QuerySnapshot<Map<String, dynamic>>>(
      stream: FirebaseFirestore.instance.collection('favorites').snapshots(),
      builder: (context, favSnapshot) {
        if (favSnapshot.hasError) {
          return const Center(
            child: Text("찜 목록을 불러오는 중 오류가 발생했습니다."),
          );
        }

        if (!favSnapshot.hasData) {
          return const Center(child: CircularProgressIndicator());
        }

        final favorites = favSnapshot.data!.docs;

        if (favorites.isEmpty) {
          return const Center(child: Text("찜한 상품이 없습니다"));
        }

        return ListView.builder(
          padding: const EdgeInsets.fromLTRB(16, 16, 16, 24),
          itemCount: favorites.length,
          itemBuilder: (context, index) {
            final favDoc = favorites[index];
            final favData = favDoc.data();

            final itemName = (favData['itemName'] ?? favData['name'] ?? '').toString();
            final storeName = (favData['storeName'] ?? '').toString();
            final lastKnownStatus =
                (favData['lastKnownStatus'] ?? 'unknown').toString();

            String normalizedName =
                (favData['normalizedName'] ?? '').toString().trim();

            if (normalizedName.isEmpty && itemName.isNotEmpty) {
              normalizedName = normalizeName(itemName);
            }

            if (normalizedName.isEmpty) {
              return Card(
                margin: const EdgeInsets.only(bottom: 12),
                child: ListTile(
                  title: Text(itemName.isEmpty ? '이름 없음' : itemName),
                  subtitle: const Text('상품 식별 정보가 없어 상태를 확인할 수 없습니다.'),
                  trailing: IconButton(
                    icon: const Icon(Icons.favorite, color: Colors.red),
                    onPressed: () => _removeFavorite(favDoc.id, context),
                  ),
                ),
              );
            }

            return StreamBuilder<QuerySnapshot<Map<String, dynamic>>>(
              stream: FirebaseFirestore.instance
                  .collection('aggregated_items')
                  .where('normalizedName', isEqualTo: normalizedName)
                  .limit(1)
                  .snapshots(),
              builder: (context, aggSnapshot) {
                String currentStatus = 'unknown';

                if (aggSnapshot.hasData &&
                    aggSnapshot.data != null &&
                    aggSnapshot.data!.docs.isNotEmpty) {
                  final aggData = aggSnapshot.data!.docs.first.data();
                  currentStatus =
                      (aggData['consensusStatus'] ?? 'unknown').toString();
                }

                final isRestocked = lastKnownStatus == "out_of_stock" &&
                    (currentStatus == "in_stock" || currentStatus == "low_stock");

                return Card(
                  margin: const EdgeInsets.only(bottom: 12),
                  shape: RoundedRectangleBorder(
                    borderRadius: BorderRadius.circular(14),
                  ),
                  child: Padding(
                    padding: const EdgeInsets.all(14),
                    child: Column(
                      crossAxisAlignment: CrossAxisAlignment.start,
                      children: [
                        Row(
                          children: [
                            const Icon(Icons.favorite, color: Colors.red),
                            const SizedBox(width: 8),
                            Expanded(
                              child: Text(
                                itemName.isEmpty ? '이름 없음' : itemName,
                                style: const TextStyle(
                                  fontWeight: FontWeight.bold,
                                  fontSize: 16,
                                ),
                              ),
                            ),
                            IconButton(
                              onPressed: () => _removeFavorite(favDoc.id, context),
                              icon: const Icon(Icons.delete_outline),
                            ),
                          ],
                        ),
                        if (storeName.isNotEmpty) ...[
                          const SizedBox(height: 4),
                          Text(
                            storeName,
                            style: TextStyle(
                              color: Colors.grey[700],
                              fontSize: 13,
                            ),
                          ),
                        ],
                        const SizedBox(height: 10),
                        Row(
                          children: [
                            Container(
                              padding: const EdgeInsets.symmetric(
                                horizontal: 10,
                                vertical: 5,
                              ),
                              decoration: BoxDecoration(
                                color: getStatusColor(currentStatus),
                                borderRadius: BorderRadius.circular(20),
                              ),
                              child: Text(
                                getStatusText(currentStatus),
                                style: const TextStyle(
                                  color: Colors.white,
                                  fontSize: 12,
                                  fontWeight: FontWeight.w600,
                                ),
                              ),
                            ),
                          ],
                        ),
                        if (isRestocked) ...[
                          const SizedBox(height: 10),
                          const Text(
                            "🔥 재입고됨!",
                            style: TextStyle(
                              color: Colors.orange,
                              fontWeight: FontWeight.bold,
                            ),
                          ),
                        ],
                      ],
                    ),
                  ),
                );
              },
            );
          },
        );
      },
    );
  }
}