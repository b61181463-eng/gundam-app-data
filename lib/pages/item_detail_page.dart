import 'package:flutter/material.dart';
import '../models/gundam_item.dart';
import '../models/gundam_store.dart';
import '../services/alert_service.dart';
import '../services/inventory_report_service.dart';

class ItemDetailPage extends StatefulWidget {
  final GundamStore store;
  final GundamItem item;
  final String countryCode;

  const ItemDetailPage({
    super.key,
    required this.store,
    required this.item,
    required this.countryCode,
  });

  @override
  State<ItemDetailPage> createState() => _ItemDetailPageState();
}

class _ItemDetailPageState extends State<ItemDetailPage> {
  late String itemId;

  @override
  void initState() {
    super.initState();
    itemId = InventoryReportService.makeItemId(
      countryCode: widget.countryCode,
      storeName: widget.store.name,
      itemName: widget.item.name,
    );
  }

  @override
  Widget build(BuildContext context) {
    final alertId = AlertService.makeAlertId(
      countryCode: widget.countryCode,
      storeName: widget.store.name,
      itemName: widget.item.name,
    );

    return ValueListenableBuilder<Map<String, InventoryReport>>(
      valueListenable: InventoryReportService.reports,
      builder: (context, reports, _) {
        final override = reports[itemId];
        final isInStock = override?.inStock ?? widget.item.inStock;
        final updatedAt = override?.updatedAt ?? widget.item.updatedAt;

        return Scaffold(
          appBar: AppBar(
            title: Text(widget.item.name),
          ),
          body: Padding(
            padding: const EdgeInsets.all(16),
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Card(
                  elevation: 3,
                  child: Padding(
                    padding: const EdgeInsets.all(16),
                    child: Row(
                      children: [
                        Icon(
                          Icons.inventory_2,
                          size: 48,
                          color: isInStock ? Colors.green : Colors.red,
                        ),
                        const SizedBox(width: 16),
                        Expanded(
                          child: Column(
                            crossAxisAlignment: CrossAxisAlignment.start,
                            children: [
                              Text(
                                widget.item.name,
                                style: const TextStyle(
                                  fontSize: 22,
                                  fontWeight: FontWeight.bold,
                                ),
                              ),
                              const SizedBox(height: 8),
                              Text(
                                isInStock ? '현재 재고 있음' : '현재 재고 없음',
                                style: TextStyle(
                                  fontSize: 16,
                                  color: isInStock ? Colors.green : Colors.red,
                                  fontWeight: FontWeight.w600,
                                ),
                              ),
                            ],
                          ),
                        ),
                      ],
                    ),
                  ),
                ),
                const SizedBox(height: 20),
                const Text(
                  '상세 정보',
                  style: TextStyle(
                    fontSize: 20,
                    fontWeight: FontWeight.bold,
                  ),
                ),
                const SizedBox(height: 12),
                detailRow('매장', widget.store.name),
                detailRow('지역', widget.store.city),
                detailRow('주소', widget.store.address),
                detailRow('등급', widget.item.grade),
                detailRow('가격', widget.item.price),
                detailRow('재고 상태', isInStock ? '재고 있음' : '재고 없음'),
                detailRow('업데이트', updatedAt),
                const SizedBox(height: 16),
                Row(
                  children: [
                    Expanded(
                      child: FilledButton.icon(
                        onPressed: () {
                          InventoryReportService.report(
                            itemId: itemId,
                            inStock: true,
                          );

                          ScaffoldMessenger.of(context).showSnackBar(
                            const SnackBar(
                              content: Text('재고 있음으로 반영했어'),
                            ),
                          );
                        },
                        icon: const Icon(Icons.thumb_up),
                        label: const Text('재고 있음'),
                      ),
                    ),
                    const SizedBox(width: 10),
                    Expanded(
                      child: FilledButton.icon(
                        onPressed: () {
                          InventoryReportService.report(
                            itemId: itemId,
                            inStock: false,
                          );

                          ScaffoldMessenger.of(context).showSnackBar(
                            const SnackBar(
                              content: Text('재고 없음으로 반영했어'),
                            ),
                          );
                        },
                        icon: const Icon(Icons.thumb_down),
                        label: const Text('재고 없음'),
                      ),
                    ),
                  ],
                ),
                const Spacer(),
                ValueListenableBuilder<Set<String>>(
                  valueListenable: AlertService.alertIds,
                  builder: (context, alertIds, _) {
                    final isAlertOn = alertIds.contains(alertId);

                    if (isInStock) {
                      return SizedBox(
                        width: double.infinity,
                        child: FilledButton.icon(
                          onPressed: null,
                          icon: const Icon(Icons.check_circle),
                          label: const Text('현재 재고 있음'),
                        ),
                      );
                    }

                    return SizedBox(
                      width: double.infinity,
                      child: FilledButton.icon(
                        onPressed: () {
                          AlertService.toggleAlert(alertId);

                          final nowEnabled = !isAlertOn;

                          ScaffoldMessenger.of(context).showSnackBar(
                            SnackBar(
                              content: Text(
                                nowEnabled
                                    ? '${widget.item.name} 재입고 알림을 신청했어.'
                                    : '${widget.item.name} 재입고 알림을 해제했어.',
                              ),
                            ),
                          );
                        },
                        icon: Icon(
                          isAlertOn
                              ? Icons.notifications_active
                              : Icons.notifications_outlined,
                        ),
                        label: Text(
                          isAlertOn ? '알림 신청됨' : '재입고 알림 받기',
                        ),
                      ),
                    );
                  },
                ),
              ],
            ),
          ),
        );
      },
    );
  }

  Widget detailRow(String title, String value) {
    return Padding(
      padding: const EdgeInsets.symmetric(vertical: 8),
      child: Row(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          SizedBox(
            width: 90,
            child: Text(
              title,
              style: const TextStyle(fontWeight: FontWeight.w600),
            ),
          ),
          Expanded(
            child: Text(value),
          ),
        ],
      ),
    );
  }
}