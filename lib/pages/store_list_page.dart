import 'package:flutter/material.dart';
import '../models/gundam_store.dart';
import '../services/store_data_service.dart';
import 'store_detail_page.dart';

class StoreListPage extends StatefulWidget {
  final String countryCode;
  final String countryName;

  const StoreListPage({
    super.key,
    required this.countryCode,
    required this.countryName,
  });

  @override
  State<StoreListPage> createState() => _StoreListPageState();
}

class _StoreListPageState extends State<StoreListPage> {
  final TextEditingController _searchController = TextEditingController();
  String searchText = '';
  late Future<List<GundamStore>> _storesFuture;

  @override
  void initState() {
    super.initState();
    _storesFuture = StoreDataService.loadStoresByCountry(widget.countryCode);
  }

  @override
  void dispose() {
    _searchController.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: Text('${widget.countryName} 건담베이스'),
        centerTitle: true,
      ),
      body: FutureBuilder<List<GundamStore>>(
        future: _storesFuture,
        builder: (context, snapshot) {
          if (snapshot.connectionState == ConnectionState.waiting) {
            return const Center(
              child: CircularProgressIndicator(),
            );
          }

          if (snapshot.hasError) {
            return Center(
              child: Padding(
                padding: const EdgeInsets.all(16),
                child: Text(
                  '데이터를 불러오는 중 오류가 발생했습니다.\n${snapshot.error}',
                  style: const TextStyle(fontSize: 16),
                ),
              ),
            );
          }

          final stores = snapshot.data ?? [];

          final filteredStores = stores.where((store) {
            final query = searchText.trim().toLowerCase();

            if (query.isEmpty) return true;

            return store.name.toLowerCase().contains(query) ||
                store.city.toLowerCase().contains(query) ||
                store.address.toLowerCase().contains(query);
          }).toList();

          return Padding(
            padding: const EdgeInsets.all(16),
            child: Column(
              children: [
                TextField(
                  controller: _searchController,
                  onChanged: (value) {
                    setState(() {
                      searchText = value;
                    });
                  },
                  decoration: InputDecoration(
                    hintText: '매장명, 지역, 주소 검색',
                    prefixIcon: const Icon(Icons.search),
                    border: OutlineInputBorder(
                      borderRadius: BorderRadius.circular(14),
                    ),
                    filled: true,
                  ),
                ),
                const SizedBox(height: 12),
                Align(
                  alignment: Alignment.centerLeft,
                  child: Text(
                    '매장 ${filteredStores.length}곳',
                    style: Theme.of(context).textTheme.bodyMedium,
                  ),
                ),
                const SizedBox(height: 12),
                Expanded(
                  child: filteredStores.isEmpty
                      ? const Center(
                          child: Text('검색 결과가 없습니다.'),
                        )
                      : ListView.builder(
                          itemCount: filteredStores.length,
                          itemBuilder: (context, index) {
                            final store = filteredStores[index];

                            return Card(
                              margin: const EdgeInsets.only(bottom: 14),
                              elevation: 2,
                              child: InkWell(
                                borderRadius: BorderRadius.circular(12),
                                onTap: () {
                                  Navigator.push(
                                    context,
                                    MaterialPageRoute(
                                      builder: (context) => StoreDetailPage(
                                        store: store,
                                        countryCode: widget.countryCode,
                                      ),
                                    ),
                                  );
                                },
                                child: Padding(
                                  padding: const EdgeInsets.all(16),
                                  child: Column(
                                    crossAxisAlignment:
                                        CrossAxisAlignment.start,
                                    children: [
                                      Row(
                                        children: [
                                          Icon(
                                            Icons.storefront,
                                            color: store.isOpen
                                                ? Colors.blue
                                                : Colors.grey,
                                            size: 28,
                                          ),
                                          const SizedBox(width: 10),
                                          Expanded(
                                            child: Text(
                                              store.name,
                                              style: const TextStyle(
                                                fontSize: 17,
                                                fontWeight: FontWeight.bold,
                                              ),
                                            ),
                                          ),
                                          Container(
                                            padding: const EdgeInsets.symmetric(
                                              horizontal: 10,
                                              vertical: 6,
                                            ),
                                            decoration: BoxDecoration(
                                              color: store.isOpen
                                                  ? Colors.green.withOpacity(0.12)
                                                  : Colors.grey.withOpacity(0.15),
                                              borderRadius:
                                                  BorderRadius.circular(20),
                                            ),
                                            child: Text(
                                              store.isOpen ? '운영중' : '준비중',
                                              style: TextStyle(
                                                color: store.isOpen
                                                    ? Colors.green
                                                    : Colors.grey[700],
                                                fontWeight: FontWeight.w600,
                                                fontSize: 12,
                                              ),
                                            ),
                                          ),
                                        ],
                                      ),
                                      const SizedBox(height: 12),
                                      Row(
                                        crossAxisAlignment:
                                            CrossAxisAlignment.start,
                                        children: [
                                          const Icon(
                                            Icons.location_on_outlined,
                                            size: 18,
                                          ),
                                          const SizedBox(width: 6),
                                          Expanded(
                                            child: Text(
                                              '${store.city} · ${store.address}',
                                              style: const TextStyle(fontSize: 14),
                                            ),
                                          ),
                                        ],
                                      ),
                                      const SizedBox(height: 8),
                                      Row(
                                        children: [
                                          const Icon(
                                            Icons.access_time_outlined,
                                            size: 18,
                                          ),
                                          const SizedBox(width: 6),
                                          Expanded(
                                            child: Text(
                                              '영업시간 ${store.openHours}',
                                              style: const TextStyle(fontSize: 14),
                                            ),
                                          ),
                                        ],
                                      ),
                                      const SizedBox(height: 8),
                                      Row(
                                        children: [
                                          const Icon(
                                            Icons.inventory_2_outlined,
                                            size: 18,
                                          ),
                                          const SizedBox(width: 6),
                                          Text(
                                            '등록 상품 ${store.items.length}개',
                                            style: const TextStyle(fontSize: 14),
                                          ),
                                          const Spacer(),
                                          const Icon(Icons.chevron_right),
                                        ],
                                      ),
                                    ],
                                  ),
                                ),
                              ),
                            );
                          },
                        ),
                ),
              ],
            ),
          );
        },
      ),
    );
  }
}