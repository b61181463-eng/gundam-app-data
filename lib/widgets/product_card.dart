import 'package:flutter/material.dart';
import '../utils/item_helpers.dart';
import '../services/restock_prediction_service.dart';
import '../screens/price_history_screen.dart';

class ProductCard extends StatelessWidget {
  final Map<String, dynamic> data;
  final VoidCallback? onPrimaryPressed;
  final VoidCallback? onSecondaryPressed;
  final String primaryLabel;
  final String secondaryLabel;
  final IconData primaryIcon;
  final IconData secondaryIcon;
  final bool showKrBadge;

  const ProductCard({
    super.key,
    required this.data,
    required this.primaryLabel,
    required this.secondaryLabel,
    required this.primaryIcon,
    required this.secondaryIcon,
    this.onPrimaryPressed,
    this.onSecondaryPressed,
    this.showKrBadge = true,
  });

  String _predictionStatus(Map<String, dynamic> data) {
    final normalized = data['normalizedStatus']?.toString().trim();
    if (normalized != null && normalized.isNotEmpty) return normalized;

    final status = data['status']?.toString().trim();
    if (status != null && status.isNotEmpty) return status;

    return ItemHelpers.itemStockLabel(data);
  }

  int? _predictionPrice(Map<String, dynamic> data) {
    final candidates = [
      data['normalizedPrice'],
      data['price'],
      data['lowestPrice'],
      data['minPrice'],
    ];

    for (final value in candidates) {
      if (value == null) continue;
      if (value is int) return value;
      if (value is num) return value.toInt();

      final text = value.toString();
      final onlyNumber = text.replaceAll(RegExp(r'[^0-9]'), '');
      if (onlyNumber.isEmpty) continue;

      final parsed = int.tryParse(onlyNumber);
      if (parsed != null && parsed > 0) return parsed;
    }

    return null;
  }



  String _productKey(Map<String, dynamic> data) {
    final candidates = [
      data['groupKey'],
      data['normalizedName'],
      data['id'],
      data['docId'],
      data['name'],
      data['title'],
    ];

    for (final value in candidates) {
      final text = value?.toString().trim() ?? '';
      if (text.isNotEmpty) return text;
    }

    return ItemHelpers.itemName(data);
  }

  Widget _buildBadge({
    required String text,
    required Color textColor,
    required Color bgColor,
    required Color borderColor,
  }) {
    return Container(
      padding: const EdgeInsets.symmetric(horizontal: 8, vertical: 4),
      decoration: BoxDecoration(
        color: bgColor,
        borderRadius: BorderRadius.circular(999),
        border: Border.all(color: borderColor),
      ),
      child: Text(
        text,
        style: TextStyle(
          fontSize: 12,
          fontWeight: FontWeight.bold,
          color: textColor,
        ),
      ),
    );
  }

  Widget? _buildChangeBadge(Map<String, dynamic> data) {
    if (ItemHelpers.isRestocked(data)) {
      return _buildBadge(
        text: '재입고',
        textColor: Colors.green.shade900,
        bgColor: Colors.green.shade50,
        borderColor: Colors.green.shade200,
      );
    }

    if (ItemHelpers.isStatusChanged(data)) {
      return _buildBadge(
        text: '변동',
        textColor: Colors.purple.shade900,
        bgColor: Colors.purple.shade50,
        borderColor: Colors.purple.shade200,
      );
    }

    return null;
  }

  Widget? _buildVerificationBadge(Map<String, dynamic> data) {
    if (ItemHelpers.isCrossChecked(data)) {
      return _buildBadge(
        text: '교차검증',
        textColor: Colors.teal.shade900,
        bgColor: Colors.teal.shade50,
        borderColor: Colors.teal.shade200,
      );
    }
    return null;
  }

  Widget _buildPredictionBadge(RestockPrediction prediction) {
    final Color textColor = prediction.score >= 70
        ? Colors.red.shade700
        : prediction.score >= 40
            ? Colors.orange.shade800
            : Colors.grey.shade700;

    final Color bgColor = prediction.score >= 70
        ? Colors.red.shade50
        : prediction.score >= 40
            ? Colors.orange.shade50
            : Colors.grey.shade100;

    final Color borderColor = prediction.score >= 70
        ? Colors.red.shade100
        : prediction.score >= 40
            ? Colors.orange.shade100
            : Colors.grey.shade300;

    return _buildBadge(
      text: '${prediction.label} · ${prediction.score}점',
      textColor: textColor,
      bgColor: bgColor,
      borderColor: borderColor,
    );
  }

  @override
  Widget build(BuildContext context) {
    final name = ItemHelpers.itemName(data);
    final store = ItemHelpers.itemStore(data);
    final price = ItemHelpers.itemPrice(data);
    final stockLabel = ItemHelpers.itemStockLabel(data);
    final imageUrl = ItemHelpers.itemImageUrl(data);
    final inStock = ItemHelpers.isInStock(data);
    final isNotice = ItemHelpers.isNoticeItem(data);
    final noticeDate = ItemHelpers.noticeDateText(data);
    final isToday = ItemHelpers.isTodayNotice(data);
    final changeBadge = _buildChangeBadge(data);
    final verificationBadge = _buildVerificationBadge(data);

    final prediction = RestockPredictionService.predict(
      status: _predictionStatus(data),
      price: _predictionPrice(data),
    );

    return Card(
      elevation: isToday ? 3 : 2,
      shape: RoundedRectangleBorder(
        borderRadius: BorderRadius.circular(14),
        side: isToday
            ? BorderSide(color: Colors.orange.shade200, width: 1.2)
            : BorderSide.none,
      ),
      child: Padding(
        padding: const EdgeInsets.all(12),
        child: Row(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            ClipRRect(
              borderRadius: BorderRadius.circular(10),
              child: imageUrl.isNotEmpty
                  ? Image.network(
                      imageUrl,
                      width: 92,
                      height: 92,
                      fit: BoxFit.cover,
                      errorBuilder: (_, __, ___) {
                        return Container(
                          width: 92,
                          height: 92,
                          color: Colors.grey.shade200,
                          alignment: Alignment.center,
                          child: const Icon(Icons.image_not_supported),
                        );
                      },
                    )
                  : Container(
                      width: 92,
                      height: 92,
                      color: Colors.grey.shade200,
                      alignment: Alignment.center,
                      child: const Icon(Icons.image),
                    ),
            ),
            const SizedBox(width: 12),
            Expanded(
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  Wrap(
                    spacing: 8,
                    runSpacing: 6,
                    crossAxisAlignment: WrapCrossAlignment.center,
                    children: [
                      SizedBox(
                        width: MediaQuery.of(context).size.width - 190,
                        child: Text(
                          name,
                          maxLines: 2,
                          overflow: TextOverflow.ellipsis,
                          style: const TextStyle(
                            fontSize: 15,
                            fontWeight: FontWeight.bold,
                          ),
                        ),
                      ),
                      if (showKrBadge)
                        _buildBadge(
                          text: 'KR',
                          textColor: Colors.red,
                          bgColor: Colors.red.shade50,
                          borderColor: Colors.red.shade200,
                        ),
                      if (isNotice && isToday)
                        _buildBadge(
                          text: 'NEW',
                          textColor: Colors.orange.shade900,
                          bgColor: Colors.orange.shade50,
                          borderColor: Colors.orange.shade200,
                        ),
                      if (changeBadge != null) changeBadge,
                      if (verificationBadge != null) verificationBadge,
                      if (!isNotice) _buildPredictionBadge(prediction),
                    ],
                  ),
                  const SizedBox(height: 8),
                  Text(
                    '판매처: $store',
                    style: TextStyle(color: Colors.grey.shade700),
                  ),
                  const SizedBox(height: 4),
                  if (isNotice) ...[
                    Text(
                      '유형: 건담베이스 공지',
                      style: TextStyle(
                        color: Colors.blueGrey.shade700,
                        fontWeight: FontWeight.w600,
                      ),
                    ),
                    const SizedBox(height: 4),
                    Text(
                      '공지일: $noticeDate',
                      style: TextStyle(
                        color: isToday
                            ? Colors.orange.shade800
                            : Colors.grey.shade700,
                        fontWeight:
                            isToday ? FontWeight.w700 : FontWeight.normal,
                      ),
                    ),
                    const SizedBox(height: 4),
                    Text(
                      '상태: $stockLabel',
                      style: TextStyle(
                        color: isToday
                            ? Colors.orange.shade800
                            : Colors.deepOrange,
                        fontWeight: FontWeight.w700,
                      ),
                    ),
                  ] else ...[
                    Text(
                      '가격: $price',
                      style: TextStyle(color: Colors.grey.shade700),
                    ),
                    const SizedBox(height: 4),
                    Text(
                      '재고: $stockLabel',
                      style: TextStyle(
                        color: inStock ? Colors.green : Colors.black87,
                        fontWeight: FontWeight.w600,
                      ),
                    ),
                  ],
                  const SizedBox(height: 10),
                  Row(
                    children: [
                      Expanded(
                        child: OutlinedButton.icon(
                          onPressed: onSecondaryPressed,
                          icon: Icon(secondaryIcon),
                          label: Text(secondaryLabel),
                        ),
                      ),
                      const SizedBox(width: 8),
                      Expanded(
                        child: OutlinedButton.icon(
                          onPressed: () {
                            Navigator.push(
                              context,
                              MaterialPageRoute(
                                builder: (_) => PriceHistoryScreen(
                                  productKey: _productKey(data),
                                  productName: name,
                                ),
                              ),
                            );
                          },
                          icon: const Icon(Icons.show_chart, size: 18),
                          label: const Text('가격'),
                        ),
                      ),
                      const SizedBox(width: 8),
                      Expanded(
                        child: ElevatedButton.icon(
                          onPressed: onPrimaryPressed,
                          icon: Icon(primaryIcon),
                          label: Text(primaryLabel),
                        ),
                      ),
                    ],
                  ),
                ],
              ),
            ),
          ],
        ),
      ),
    );
  }
}
