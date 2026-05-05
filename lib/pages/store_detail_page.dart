import 'package:flutter/material.dart';
import '../models/gundam_store.dart';
import 'inventory_page.dart';

class StoreDetailPage extends StatelessWidget {
  final GundamStore store;
  final String countryCode;

  const StoreDetailPage({
    super.key,
    required this.store,
    required this.countryCode,
  });

  void _showPhoneMessage(BuildContext context) {
    final message = store.phone == '확인 중'
        ? '전화번호 정보가 없습니다.'
        : '전화 기능은 나중에 연결할 수 있어요.\n현재 번호: ${store.phone}';

    ScaffoldMessenger.of(context).showSnackBar(
      SnackBar(content: Text(message)),
    );
  }

  void _showMapMessage(BuildContext context) {
    ScaffoldMessenger.of(context).showSnackBar(
      SnackBar(content: Text('지도 기능은 다음 단계에서 연결할게요.\n주소: ${store.address}')),
    );
  }

  @override
  Widget build(BuildContext context) {
    final inStockCount = store.items.where((item) => item.inStock).length;

    return Scaffold(
      appBar: AppBar(
        title: Text(store.name),
      ),
      body: Padding(
        padding: const EdgeInsets.all(16),
        child: Column(
          children: [
            Card(
              elevation: 3,
              child: Padding(
                padding: const EdgeInsets.all(16),
                child: Row(
                  crossAxisAlignment: CrossAxisAlignment.start,
                  children: [
                    Icon(
                      Icons.storefront,
                      size: 44,
                      color: store.isOpen ? Colors.blue : Colors.grey,
                    ),
                    const SizedBox(width: 14),
                    Expanded(
                      child: Column(
                        crossAxisAlignment: CrossAxisAlignment.start,
                        children: [
                          Text(
                            store.name,
                            style: const TextStyle(
                              fontSize: 21,
                              fontWeight: FontWeight.bold,
                            ),
                          ),
                          const SizedBox(height: 8),
                          Container(
                            padding: const EdgeInsets.symmetric(
                              horizontal: 10,
                              vertical: 6,
                            ),
                            decoration: BoxDecoration(
                              color: store.isOpen
                                  ? Colors.green.withOpacity(0.12)
                                  : Colors.grey.withOpacity(0.15),
                              borderRadius: BorderRadius.circular(20),
                            ),
                            child: Text(
                              store.isOpen ? '운영중' : '준비중',
                              style: TextStyle(
                                color: store.isOpen
                                    ? Colors.green
                                    : Colors.grey[700],
                                fontWeight: FontWeight.w600,
                              ),
                            ),
                          ),
                        ],
                      ),
                    ),
                  ],
                ),
              ),
            ),
            const SizedBox(height: 16),
            Card(
              child: Padding(
                padding: const EdgeInsets.all(16),
                child: Column(
                  children: [
                    detailRow(Icons.location_on_outlined, '지역', store.city),
                    const SizedBox(height: 12),
                    detailRow(Icons.place_outlined, '주소', store.address),
                    const SizedBox(height: 12),
                    detailRow(Icons.phone_outlined, '전화', store.phone),
                    const SizedBox(height: 12),
                    detailRow(Icons.access_time_outlined, '영업시간', store.openHours),
                    const SizedBox(height: 12),
                    detailRow(
                      Icons.inventory_2_outlined,
                      '등록 상품',
                      '${store.items.length}개 (재고 있음 $inStockCount개)',
                    ),
                  ],
                ),
              ),
            ),
            const SizedBox(height: 16),
            Row(
              children: [
                Expanded(
                  child: OutlinedButton.icon(
                    onPressed: () => _showPhoneMessage(context),
                    icon: const Icon(Icons.call),
                    label: const Text('전화'),
                  ),
                ),
                const SizedBox(width: 12),
                Expanded(
                  child: OutlinedButton.icon(
                    onPressed: () => _showMapMessage(context),
                    icon: const Icon(Icons.map_outlined),
                    label: const Text('지도'),
                  ),
                ),
              ],
            ),
            const Spacer(),
            SizedBox(
              width: double.infinity,
              child: FilledButton.icon(
                onPressed: () {
                  Navigator.push(
                    context,
                    MaterialPageRoute(
                      builder: (context) => InventoryPage(
                        store: store,
                        countryCode: countryCode,
                      ),
                    ),
                  );
                },
                icon: const Icon(Icons.inventory_2),
                label: const Text('재고 보러 가기'),
              ),
            ),
          ],
        ),
      ),
    );
  }

  Widget detailRow(IconData icon, String title, String value) {
    return Row(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        Icon(icon, size: 20),
        const SizedBox(width: 8),
        SizedBox(
          width: 70,
          child: Text(
            title,
            style: const TextStyle(fontWeight: FontWeight.w600),
          ),
        ),
        Expanded(
          child: Text(value),
        ),
      ],
    );
  }
}