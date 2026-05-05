import 'package:cloud_firestore/cloud_firestore.dart';
import 'package:flutter/material.dart';
import '../services/stock_service.dart';

class EventsScreen extends StatefulWidget {
  final String userId;

  const EventsScreen({super.key, required this.userId});

  @override
  State<EventsScreen> createState() => _EventsScreenState();
}

class _EventsScreenState extends State<EventsScreen> {
  final StockService stockService = StockService();

  static const String currentCountry = 'KR';
  bool showOnlyMyEvents = true;

  Future<Set<String>> _loadWatchlistProductIds() async {
    final snapshot = await FirebaseFirestore.instance
        .collection('watchlists')
        .where('userId', isEqualTo: widget.userId)
        .get();

    return snapshot.docs
        .map((doc) => (doc.data())['productId'] as String)
        .toSet();
  }

  Future<void> _markAllAsRead() async {
    final snapshot = await FirebaseFirestore.instance
        .collection('restock_events')
        .where('country', isEqualTo: currentCountry)
        .where('isRead', isEqualTo: false)
        .get();

    for (final doc in snapshot.docs) {
      await doc.reference.update({'isRead': true});
    }
  }

  Future<int> _loadUnreadCount() async {
    final snapshot = await FirebaseFirestore.instance
        .collection('restock_events')
        .where('country', isEqualTo: currentCountry)
        .where('isRead', isEqualTo: false)
        .get();

    return snapshot.docs.length;
  }

  Future<void> _refresh() async {
    setState(() {});
    await Future.delayed(const Duration(milliseconds: 500));
  }

  Widget _buildSummaryCard({
    required String title,
    required String value,
    required IconData icon,
    required Color color,
  }) {
    return Container(
      width: 165,
      padding: const EdgeInsets.all(14),
      decoration: BoxDecoration(
        color: color.withOpacity(0.08),
        borderRadius: BorderRadius.circular(16),
        border: Border.all(color: color.withOpacity(0.25)),
      ),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Icon(icon, color: color, size: 20),
          const SizedBox(height: 10),
          Text(
            value,
            style: TextStyle(
              fontSize: 20,
              fontWeight: FontWeight.bold,
              color: color,
            ),
          ),
          const SizedBox(height: 4),
          Text(
            title,
            style: const TextStyle(
              fontSize: 12,
              fontWeight: FontWeight.w600,
            ),
          ),
        ],
      ),
    );
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: Text(showOnlyMyEvents ? 'KR 관심상품 이벤트' : 'KR 전체 이벤트'),
        actions: [
          IconButton(
            tooltip: showOnlyMyEvents ? 'KR 전체 이벤트 보기' : 'KR 관심상품 이벤트만 보기',
            icon: Icon(
              showOnlyMyEvents ? Icons.filter_alt : Icons.filter_alt_outlined,
            ),
            onPressed: () {
              setState(() {
                showOnlyMyEvents = !showOnlyMyEvents;
              });
            },
          ),
          IconButton(
            tooltip: '전체 읽음 처리',
            icon: const Icon(Icons.done_all),
            onPressed: () async {
              await _markAllAsRead();
              if (context.mounted) {
                ScaffoldMessenger.of(context).showSnackBar(
                  const SnackBar(content: Text('KR 이벤트 전체 읽음 처리 완료')),
                );
              }
              setState(() {});
            },
          ),
        ],
      ),
      body: RefreshIndicator(
        onRefresh: _refresh,
        child: FutureBuilder<Set<String>>(
          future: _loadWatchlistProductIds(),
          builder: (context, watchSnapshot) {
            if (watchSnapshot.connectionState == ConnectionState.waiting) {
              return const Center(child: CircularProgressIndicator());
            }

            if (watchSnapshot.hasError) {
              return Center(child: Text('관심상품 로딩 오류: ${watchSnapshot.error}'));
            }

            final watchlistIds = watchSnapshot.data ?? <String>{};

            return FutureBuilder<int>(
              future: _loadUnreadCount(),
              builder: (context, unreadSnapshot) {
                final unreadCount = unreadSnapshot.data ?? 0;

                return StreamBuilder<QuerySnapshot>(
                  stream: FirebaseFirestore.instance
                      .collection('restock_events')
                      .where('country', isEqualTo: currentCountry)
                      .orderBy('detectedAt', descending: true)
                      .snapshots(),
                  builder: (context, snapshot) {
                    if (snapshot.hasError) {
                      return Center(child: Text('오류 발생: ${snapshot.error}'));
                    }

                    if (!snapshot.hasData) {
                      return const Center(child: CircularProgressIndicator());
                    }

                    final allDocs = snapshot.data!.docs;

                    final docs = showOnlyMyEvents
                        ? allDocs.where((doc) {
                            final data = doc.data() as Map<String, dynamic>;
                            final productId = data['productId'] ?? '';
                            return watchlistIds.contains(productId);
                          }).toList()
                        : allDocs;

                    int restockCount = 0;
                    int soldoutCount = 0;
                    int newStockCount = 0;

                    for (final doc in docs) {
                      final data = doc.data() as Map<String, dynamic>;
                      final type = data['type'] ?? 'none';

                      if (type == 'restock') {
                        restockCount++;
                      } else if (type == 'soldout') {
                        soldoutCount++;
                      } else if (type == 'new_stock') {
                        newStockCount++;
                      }
                    }

                    return ListView(
                      physics: const AlwaysScrollableScrollPhysics(),
                      children: [
                        Padding(
                          padding: const EdgeInsets.fromLTRB(12, 12, 12, 4),
                          child: Wrap(
                            spacing: 10,
                            runSpacing: 10,
                            children: [
                              _buildSummaryCard(
                                title: 'KR 미확인 이벤트',
                                value: '$unreadCount',
                                icon: Icons.mark_email_unread,
                                color: Colors.orange,
                              ),
                              _buildSummaryCard(
                                title: 'KR 재입고',
                                value: '$restockCount',
                                icon: Icons.notifications_active,
                                color: Colors.red,
                              ),
                              _buildSummaryCard(
                                title: 'KR 품절 전환',
                                value: '$soldoutCount',
                                icon: Icons.remove_shopping_cart,
                                color: Colors.black54,
                              ),
                              _buildSummaryCard(
                                title: 'KR 신규 재고',
                                value: '$newStockCount',
                                icon: Icons.add_alert,
                                color: Colors.blue,
                              ),
                            ],
                          ),
                        ),
                        Padding(
                          padding: const EdgeInsets.fromLTRB(12, 12, 12, 4),
                          child: Row(
                            children: [
                              const Icon(Icons.flag, size: 18),
                              const SizedBox(width: 6),
                              Text(
                                showOnlyMyEvents
                                    ? 'KR 관심상품 이벤트 목록'
                                    : 'KR 전체 이벤트 목록',
                                style: const TextStyle(
                                  fontWeight: FontWeight.bold,
                                  fontSize: 16,
                                ),
                              ),
                            ],
                          ),
                        ),
                        if (docs.isEmpty)
                          const Padding(
                            padding: EdgeInsets.all(24),
                            child: Center(
                              child: Text('KR 이벤트가 없습니다.'),
                            ),
                          )
                        else
                          ...docs.map((doc) {
                            final data = doc.data() as Map<String, dynamic>;

                            final name = data['name'] ?? '이름 없음';
                            final site = data['site'] ?? '';
                            final country = data['country'] ?? '';
                            final fromStatus = data['fromStatus'] ?? 'unknown';
                            final toStatus = data['toStatus'] ?? 'unknown';
                            final type = data['type'] ?? 'none';
                            final productId = data['productId'] ?? '';
                            final isMyItem = watchlistIds.contains(productId);
                            final isRead = data['isRead'] ?? false;

                            Color badgeColor = Colors.grey;
                            IconData leadingIcon = Icons.info_outline;

                            if (type == 'restock') {
                              badgeColor = Colors.red;
                              leadingIcon = Icons.notifications_active;
                            } else if (type == 'soldout') {
                              badgeColor = Colors.black54;
                              leadingIcon = Icons.remove_shopping_cart;
                            } else if (type == 'new_stock') {
                              badgeColor = Colors.blue;
                              leadingIcon = Icons.add_alert;
                            }

                            return Card(
                              margin: const EdgeInsets.symmetric(
                                horizontal: 12,
                                vertical: 8,
                              ),
                              child: ListTile(
                                onTap: () async {
                                  await FirebaseFirestore.instance
                                      .collection('restock_events')
                                      .doc(doc.id)
                                      .update({
                                    'isRead': true,
                                  });
                                  setState(() {});
                                },
                                leading: CircleAvatar(
                                  backgroundColor: badgeColor.withOpacity(0.12),
                                  child: Icon(
                                    leadingIcon,
                                    color: badgeColor,
                                  ),
                                ),
                                title: Row(
                                  children: [
                                    Expanded(
                                      child: Text(
                                        name,
                                        maxLines: 2,
                                        overflow: TextOverflow.ellipsis,
                                        style: const TextStyle(
                                          fontWeight: FontWeight.bold,
                                        ),
                                      ),
                                    ),
                                    if (isMyItem)
                                      Container(
                                        margin: const EdgeInsets.only(left: 6),
                                        padding: const EdgeInsets.symmetric(
                                          horizontal: 6,
                                          vertical: 2,
                                        ),
                                        decoration: BoxDecoration(
                                          color: Colors.pink.shade50,
                                          borderRadius:
                                              BorderRadius.circular(8),
                                          border: Border.all(
                                            color: Colors.pink.shade200,
                                          ),
                                        ),
                                        child: const Text(
                                          '관심상품',
                                          style: TextStyle(
                                            fontSize: 10,
                                            fontWeight: FontWeight.bold,
                                            color: Colors.pink,
                                          ),
                                        ),
                                      ),
                                    if (!isRead)
                                      Container(
                                        margin: const EdgeInsets.only(left: 6),
                                        padding: const EdgeInsets.symmetric(
                                          horizontal: 6,
                                          vertical: 2,
                                        ),
                                        decoration: BoxDecoration(
                                          color: Colors.orange.shade50,
                                          borderRadius:
                                              BorderRadius.circular(8),
                                          border: Border.all(
                                            color: Colors.orange.shade200,
                                          ),
                                        ),
                                        child: const Text(
                                          'NEW',
                                          style: TextStyle(
                                            fontSize: 10,
                                            fontWeight: FontWeight.bold,
                                            color: Colors.orange,
                                          ),
                                        ),
                                      ),
                                  ],
                                ),
                                subtitle: Padding(
                                  padding: const EdgeInsets.only(top: 6),
                                  child: Column(
                                    crossAxisAlignment:
                                        CrossAxisAlignment.start,
                                    children: [
                                      Row(
                                        children: [
                                          Text(
                                            site,
                                            style: TextStyle(
                                              color: Colors.grey.shade700,
                                              fontWeight: FontWeight.w600,
                                            ),
                                          ),
                                          const SizedBox(width: 8),
                                          Container(
                                            padding: const EdgeInsets.symmetric(
                                              horizontal: 6,
                                              vertical: 2,
                                            ),
                                            decoration: BoxDecoration(
                                              color: Colors.black
                                                  .withOpacity(0.06),
                                              borderRadius:
                                                  BorderRadius.circular(8),
                                            ),
                                            child: Text(
                                              country,
                                              style: const TextStyle(
                                                fontSize: 10,
                                                fontWeight: FontWeight.bold,
                                              ),
                                            ),
                                          ),
                                        ],
                                      ),
                                      const SizedBox(height: 8),
                                      Text(
                                        '${stockService.statusLabel(fromStatus)} → ${stockService.statusLabel(toStatus)}',
                                      ),
                                    ],
                                  ),
                                ),
                                isThreeLine: true,
                                trailing: Container(
                                  padding: const EdgeInsets.symmetric(
                                    horizontal: 8,
                                    vertical: 6,
                                  ),
                                  decoration: BoxDecoration(
                                    color: badgeColor.withOpacity(0.12),
                                    borderRadius: BorderRadius.circular(10),
                                  ),
                                  child: Text(
                                    stockService.eventLabel(type),
                                    style: TextStyle(
                                      fontWeight: FontWeight.bold,
                                      color: badgeColor,
                                    ),
                                  ),
                                ),
                              ),
                            );
                          }),
                        const SizedBox(height: 32),
                      ],
                    );
                  },
                );
              },
            );
          },
        ),
      ),
    );
  }
}