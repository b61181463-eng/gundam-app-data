import 'package:flutter/material.dart';
import '../models/gundam_item.dart';
import '../models/gundam_store.dart';
import '../services/inventory_report_service.dart';
import 'item_detail_page.dart';

class InventoryPage extends StatefulWidget {
  final GundamStore store;
  final String countryCode;

  const InventoryPage({
    super.key,
    required this.store,
    required this.countryCode,
  });

  @override
  State<InventoryPage> createState() => _InventoryPageState();
}

class _InventoryPageState extends State<InventoryPage> {
  final TextEditingController _searchController = TextEditingController();
  String searchText = '';
  bool onlyInStock = false;
  String selectedGrade = '전체';

  List<String> get availableGrades {
    final grades = widget.store.items.map((item) => item.grade).toSet().toList();
    grades.sort();
    return ['전체', ...grades];
  }

  @override
  void dispose() {
    _searchController.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return ValueListenableBuilder<Map<String, InventoryReport>>(
      valueListenable: InventoryReportService.reports,
      builder: (context, reports, _) {
        final filteredItems = widget.store.items.where((item) {
          final itemId = InventoryReportService.makeItemId(
            countryCode: widget.countryCode,
            storeName: widget.store.name,
            itemName: item.name,
          );

          final override = reports[itemId];
          final effectiveInStock = override?.inStock ?? item.inStock;

          final query = searchText.toLowerCase();
          final matchesSearch = item.name.toLowerCase().contains(query) ||
              item.grade.toLowerCase().contains(query);
          final matchesStock = !onlyInStock || effectiveInStock;
          final matchesGrade =
              selectedGrade == '전체' || item.grade == selectedGrade;

          return matchesSearch && matchesStock && matchesGrade;
        }).toList();

        return Scaffold(
          appBar: AppBar(
            title: Text(widget.store.name),
          ),
          body: Padding(
            padding: const EdgeInsets.all(16),
            child: Column(
              children: [
                Card(
                  child: ListTile(
                    leading: const Icon(Icons.location_on_outlined),
                    title: Text(widget.store.name),
                    subtitle: Text(
                      '${widget.store.city}\n${widget.store.address}\n영업시간: ${widget.store.openHours}',
                    ),
                    isThreeLine: true,
                  ),
                ),
                const SizedBox(height: 12),
                TextField(
                  controller: _searchController,
                  onChanged: (value) {
                    setState(() {
                      searchText = value;
                    });
                  },
                  decoration: InputDecoration(
                    hintText: '상품명 또는 등급 검색',
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
                    '등급 필터',
                    style: Theme.of(context).textTheme.titleMedium,
                  ),
                ),
                const SizedBox(height: 8),
                SingleChildScrollView(
                  scrollDirection: Axis.horizontal,
                  child: Row(
                    children: availableGrades.map((grade) {
                      final isSelected = selectedGrade == grade;

                      return Padding(
                        padding: const EdgeInsets.only(right: 8),
                        child: ChoiceChip(
                          label: Text(grade),
                          selected: isSelected,
                          onSelected: (_) {
                            setState(() {
                              selectedGrade = grade;
                            });
                          },
                        ),
                      );
                    }).toList(),
                  ),
                ),
                const SizedBox(height: 8),
                SwitchListTile(
                  contentPadding: EdgeInsets.zero,
                  title: const Text('재고 있음만 보기'),
                  value: onlyInStock,
                  onChanged: (value) {
                    setState(() {
                      onlyInStock = value;
                    });
                  },
                ),
                Align(
                  alignment: Alignment.centerLeft,
                  child: Text(
                    '검색 결과 ${filteredItems.length}개',
                    style: Theme.of(context).textTheme.bodyMedium,
                  ),
                ),
                const SizedBox(height: 8),
                Expanded(
                  child: filteredItems.isEmpty
                      ? const Center(
                          child: Text('조건에 맞는 재고가 없습니다.'),
                        )
                      : ListView.builder(
                          itemCount: filteredItems.length,
                          itemBuilder: (context, index) {
                            final item = filteredItems[index];
                            final itemId = InventoryReportService.makeItemId(
                              countryCode: widget.countryCode,
                              storeName: widget.store.name,
                              itemName: item.name,
                            );
                            final override = reports[itemId];
                            final effectiveInStock =
                                override?.inStock ?? item.inStock;
                            final effectiveUpdatedAt =
                                override?.updatedAt ?? item.updatedAt;

                            return Card(
                              margin: const EdgeInsets.only(bottom: 12),
                              child: ListTile(
                                leading: Icon(
                                  Icons.inventory_2,
                                  color: effectiveInStock
                                      ? Colors.green
                                      : Colors.red,
                                ),
                                title: Text(
                                  item.name,
                                  style: const TextStyle(
                                    fontWeight: FontWeight.bold,
                                  ),
                                ),
                                subtitle: Text(
                                  '${item.grade} · ${item.price} · '
                                  '${effectiveInStock ? '재고 있음' : '재고 없음'}\n'
                                  '업데이트: $effectiveUpdatedAt',
                                ),
                                isThreeLine: true,
                                trailing: const Icon(Icons.chevron_right),
                                onTap: () {
                                  Navigator.push(
                                    context,
                                    MaterialPageRoute(
                                      builder: (context) => ItemDetailPage(
                                        store: widget.store,
                                        item: item,
                                        countryCode: widget.countryCode,
                                      ),
                                    ),
                                  );
                                },
                              ),
                            );
                          },
                        ),
                ),
              ],
            ),
          ),
        );
      },
    );
  }
}