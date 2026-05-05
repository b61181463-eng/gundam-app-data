import 'package:flutter/material.dart';
import 'package:cloud_firestore/cloud_firestore.dart';

class AggregatedInventoryScreen extends StatefulWidget {
  const AggregatedInventoryScreen({super.key});

  @override
  State<AggregatedInventoryScreen> createState() =>
      _AggregatedInventoryScreenState();
}

class _AggregatedInventoryScreenState extends State<AggregatedInventoryScreen> {
  final TextEditingController _searchController = TextEditingController();
  String _searchText = "";

  @override
  void dispose() {
    _searchController.dispose();
    super.dispose();
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

  IconData getStatusIcon(String status) {
    switch (status) {
      case "in_stock":
        return Icons.check_circle;
      case "out_of_stock":
        return Icons.cancel;
      case "low_stock":
        return Icons.warning_amber_rounded;
      case "check_required":
        return Icons.help_outline;
      default:
        return Icons.info_outline;
    }
  }

  String getSourceStatusText(dynamic quantity, String stockStatus) {
    final qty = quantity is num ? quantity.toInt() : 0;
    final lower = stockStatus.toLowerCase();

    if (lower.contains("out of stock") || lower.contains("sold out") || qty == 0) {
      return "품절";
    }

    if (lower.contains("only") || lower.contains("<") || (qty > 0 && qty <= 10)) {
      return "재고 적음";
    }

    if (qty == 999 || lower.contains("in stock")) {
      return "재고 있음";
    }

    if (qty > 10) {
      return "재고 있음";
    }

    return stockStatus.isNotEmpty ? stockStatus : "정보 없음";
  }

  Color getSourceStatusColor(dynamic quantity, String stockStatus) {
    final qty = quantity is num ? quantity.toInt() : 0;
    final lower = stockStatus.toLowerCase();

    if (lower.contains("out of stock") || lower.contains("sold out") || qty == 0) {
      return Colors.red;
    }

    if (lower.contains("only") || lower.contains("<") || (qty > 0 && qty <= 10)) {
      return Colors.orange;
    }

    return Colors.green;
  }

  String getQuantityText(dynamic quantity, String stockStatus) {
    final qty = quantity is num ? quantity.toInt() : 0;
    final lower = stockStatus.toLowerCase();

    if (lower.contains("out of stock") || lower.contains("sold out") || qty == 0) {
      return "수량 정보: 없음";
    }

    if (lower.contains("only")) {
      return "수량 정보: 거의 없음";
    }

    if (lower.contains("<") || (qty > 0 && qty <= 10)) {
      return "수량 정보: 적음";
    }

    if (qty == 999) {
      return "수량 정보: 재고 있음";
    }

    return "수량 정보: $qty개";
  }

  String formatTimestamp(Timestamp? timestamp) {
    if (timestamp == null) return "기록 없음";

    final date = timestamp.toDate().toLocal();
    final year = date.year.toString().padLeft(4, '0');
    final month = date.month.toString().padLeft(2, '0');
    final day = date.day.toString().padLeft(2, '0');
    final hour = date.hour.toString().padLeft(2, '0');
    final minute = date.minute.toString().padLeft(2, '0');

    return "$year-$month-$day $hour:$minute";
  }

  @override
  Widget build(BuildContext context) {
    return Column(
      children: [
        Padding(
          padding: const EdgeInsets.fromLTRB(16, 16, 16, 8),
          child: TextField(
            controller: _searchController,
            decoration: InputDecoration(
              hintText: "상품 검색",
              prefixIcon: const Icon(Icons.search),
              suffixIcon: _searchText.isNotEmpty
                  ? IconButton(
                      onPressed: () {
                        _searchController.clear();
                        setState(() {
                          _searchText = "";
                        });
                      },
                      icon: const Icon(Icons.close),
                    )
                  : null,
            ),
            onChanged: (value) {
              setState(() {
                _searchText = value.toLowerCase();
              });
            },
          ),
        ),
        Expanded(
          child: StreamBuilder<QuerySnapshot<Map<String, dynamic>>>(
            stream: FirebaseFirestore.instance
                .collection('aggregated_items')
                .orderBy('name')
                .snapshots(),
            builder: (context, snapshot) {
              if (snapshot.hasError) {
                return const Center(
                  child: Text("통합 재고를 불러오는 중 오류가 발생했습니다."),
                );
              }

              if (!snapshot.hasData) {
                return const Center(child: CircularProgressIndicator());
              }

              var docs = snapshot.data!.docs;

              docs = docs.where((doc) {
                final data = doc.data();
                final name = (data['name'] ?? '').toString().toLowerCase();
                return name.contains(_searchText);
              }).toList();

              if (docs.isEmpty) {
                return const Center(child: Text("상품 없음"));
              }

              return ListView.builder(
                padding: const EdgeInsets.fromLTRB(16, 8, 16, 24),
                itemCount: docs.length,
                itemBuilder: (context, index) {
                  final data = docs[index].data();

                  final name = (data['name'] ?? '').toString();
                  final status = (data['consensusStatus'] ?? 'unknown').toString();
                  final sourceCount = (data['sourceCount'] ?? 0) as int;
                  final sources = List<Map<String, dynamic>>.from(
                    data['sources'] ?? [],
                  );
                  final lastCheckedAt = data['lastCheckedAt'] as Timestamp?;

                  return Card(
                    margin: const EdgeInsets.only(bottom: 14),
                    shape: RoundedRectangleBorder(
                      borderRadius: BorderRadius.circular(16),
                    ),
                    elevation: 3,
                    child: Padding(
                      padding: const EdgeInsets.all(14),
                      child: Column(
                        crossAxisAlignment: CrossAxisAlignment.start,
                        children: [
                          Row(
                            crossAxisAlignment: CrossAxisAlignment.start,
                            children: [
                              Container(
                                width: 52,
                                height: 52,
                                decoration: BoxDecoration(
                                  color: getStatusColor(status).withOpacity(0.12),
                                  borderRadius: BorderRadius.circular(14),
                                ),
                                child: Icon(
                                  getStatusIcon(status),
                                  color: getStatusColor(status),
                                ),
                              ),
                              const SizedBox(width: 12),
                              Expanded(
                                child: Column(
                                  crossAxisAlignment: CrossAxisAlignment.start,
                                  children: [
                                    Text(
                                      name,
                                      style: const TextStyle(
                                        fontSize: 16,
                                        fontWeight: FontWeight.bold,
                                      ),
                                    ),
                                    const SizedBox(height: 8),
                                    Wrap(
                                      spacing: 8,
                                      runSpacing: 8,
                                      children: [
                                        Container(
                                          padding: const EdgeInsets.symmetric(
                                            horizontal: 10,
                                            vertical: 5,
                                          ),
                                          decoration: BoxDecoration(
                                            color: getStatusColor(status),
                                            borderRadius:
                                                BorderRadius.circular(20),
                                          ),
                                          child: Text(
                                            getStatusText(status),
                                            style: const TextStyle(
                                              color: Colors.white,
                                              fontSize: 12,
                                              fontWeight: FontWeight.w600,
                                            ),
                                          ),
                                        ),
                                        Container(
                                          padding: const EdgeInsets.symmetric(
                                            horizontal: 10,
                                            vertical: 5,
                                          ),
                                          decoration: BoxDecoration(
                                            color: Colors.blueGrey.withOpacity(0.1),
                                            borderRadius:
                                                BorderRadius.circular(20),
                                          ),
                                          child: Text(
                                            "$sourceCount개 사이트 확인",
                                            style: const TextStyle(
                                              fontSize: 12,
                                              fontWeight: FontWeight.w600,
                                            ),
                                          ),
                                        ),
                                      ],
                                    ),
                                  ],
                                ),
                              ),
                            ],
                          ),

                          const SizedBox(height: 12),

                          Row(
                            children: [
                              Icon(
                                Icons.access_time,
                                size: 15,
                                color: Colors.grey[600],
                              ),
                              const SizedBox(width: 5),
                              Expanded(
                                child: Text(
                                  "최근 크로스체크: ${formatTimestamp(lastCheckedAt)}",
                                  style: TextStyle(
                                    fontSize: 12,
                                    color: Colors.grey[600],
                                  ),
                                ),
                              ),
                            ],
                          ),

                          const SizedBox(height: 14),
                          const Divider(),
                          const SizedBox(height: 6),

                          const Text(
                            "사이트별 결과",
                            style: TextStyle(
                              fontWeight: FontWeight.bold,
                              fontSize: 14,
                            ),
                          ),

                          const SizedBox(height: 10),

                          ...sources.map((source) {
                            final storeName =
                                (source['storeName'] ?? '').toString();
                            final stockStatus =
                                (source['stockStatus'] ?? '').toString();
                            final quantity = source['quantity'];

                            final sourceStatusText =
                                getSourceStatusText(quantity, stockStatus);
                            final sourceColor =
                                getSourceStatusColor(quantity, stockStatus);

                            return Container(
                              margin: const EdgeInsets.only(bottom: 8),
                              padding: const EdgeInsets.all(10),
                              decoration: BoxDecoration(
                                color: Colors.grey.shade50,
                                borderRadius: BorderRadius.circular(12),
                                border: Border.all(
                                  color: Colors.grey.shade200,
                                ),
                              ),
                              child: Row(
                                crossAxisAlignment: CrossAxisAlignment.start,
                                children: [
                                  Container(
                                    width: 10,
                                    height: 10,
                                    margin: const EdgeInsets.only(top: 5),
                                    decoration: BoxDecoration(
                                      color: sourceColor,
                                      shape: BoxShape.circle,
                                    ),
                                  ),
                                  const SizedBox(width: 10),
                                  Expanded(
                                    child: Column(
                                      crossAxisAlignment:
                                          CrossAxisAlignment.start,
                                      children: [
                                        Text(
                                          storeName,
                                          style: const TextStyle(
                                            fontWeight: FontWeight.w600,
                                            fontSize: 13,
                                          ),
                                        ),
                                        const SizedBox(height: 3),
                                        Text(
                                          sourceStatusText,
                                          style: TextStyle(
                                            fontSize: 13,
                                            color: sourceColor,
                                            fontWeight: FontWeight.w600,
                                          ),
                                        ),
                                        const SizedBox(height: 2),
                                        Text(
                                          stockStatus,
                                          style: TextStyle(
                                            fontSize: 12,
                                            color: Colors.grey[700],
                                          ),
                                        ),
                                        const SizedBox(height: 2),
                                        Text(
                                          getQuantityText(quantity, stockStatus),
                                          style: TextStyle(
                                            fontSize: 12,
                                            color: Colors.grey[600],
                                          ),
                                        ),
                                      ],
                                    ),
                                  ),
                                ],
                              ),
                            );
                          }),
                        ],
                      ),
                    ),
                  );
                },
              );
            },
          ),
        ),
      ],
    );
  }
}