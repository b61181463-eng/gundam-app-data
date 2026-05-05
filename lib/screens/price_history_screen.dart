import 'package:cloud_firestore/cloud_firestore.dart';
import 'package:flutter/material.dart';

class PriceHistoryScreen extends StatelessWidget {
  final String productKey;
  final String productName;

  const PriceHistoryScreen({
    super.key,
    required this.productKey,
    required this.productName,
  });

  @override
  Widget build(BuildContext context) {
    final query = FirebaseFirestore.instance
        .collection('price_history')
        .doc(productKey)
        .collection('records')
        .orderBy('createdAt', descending: false);

    return Scaffold(
      appBar: AppBar(
        title: const Text('가격 변동 그래프'),
      ),
      body: StreamBuilder<QuerySnapshot<Map<String, dynamic>>>(
        stream: query.snapshots(),
        builder: (context, snapshot) {
          if (snapshot.hasError) {
            return const Center(child: Text('가격 기록을 불러오지 못했습니다.'));
          }

          if (!snapshot.hasData) {
            return const Center(child: CircularProgressIndicator());
          }

          final records = snapshot.data!.docs.map((doc) {
            final data = doc.data();
            return PriceRecord.fromMap(data);
          }).where((r) => r.price > 0).toList();

          if (records.isEmpty) {
            return const Center(
              child: Text('아직 가격 변동 기록이 없습니다.'),
            );
          }

          final prices = records.map((e) => e.price).toList();
          final minPrice = prices.reduce((a, b) => a < b ? a : b);
          final maxPrice = prices.reduce((a, b) => a > b ? a : b);
          final avgPrice =
              (prices.reduce((a, b) => a + b) / prices.length).round();

          return ListView(
            padding: const EdgeInsets.all(16),
            children: [
              Text(
                productName,
                style: const TextStyle(
                  fontSize: 18,
                  fontWeight: FontWeight.bold,
                ),
              ),

              const SizedBox(height: 16),

              Row(
                children: [
                  _StatCard(title: '최저가', value: '${minPrice}원'),
                  const SizedBox(width: 8),
                  _StatCard(title: '최고가', value: '${maxPrice}원'),
                  const SizedBox(width: 8),
                  _StatCard(title: '평균가', value: '${avgPrice}원'),
                ],
              ),

              const SizedBox(height: 20),

              Container(
                height: 220,
                padding: const EdgeInsets.all(12),
                decoration: BoxDecoration(
                  color: Colors.white,
                  borderRadius: BorderRadius.circular(16),
                  border: Border.all(color: Colors.black12),
                ),
                child: CustomPaint(
                  painter: PriceChartPainter(records),
                  child: const SizedBox.expand(),
                ),
              ),

              const SizedBox(height: 20),

              const Text(
                '가격 기록',
                style: TextStyle(
                  fontSize: 16,
                  fontWeight: FontWeight.bold,
                ),
              ),

              const SizedBox(height: 8),

              ...records.reversed.map((record) {
                return Card(
                  child: ListTile(
                    title: Text('${record.price}원'),
                    subtitle: Text(record.storeName.isEmpty
                        ? '판매처 정보 없음'
                        : record.storeName),
                    trailing: Text(_formatDate(record.createdAt)),
                  ),
                );
              }),
            ],
          );
        },
      ),
    );
  }
}

class PriceRecord {
  final int price;
  final String storeName;
  final DateTime? createdAt;

  PriceRecord({
    required this.price,
    required this.storeName,
    required this.createdAt,
  });

  factory PriceRecord.fromMap(Map<String, dynamic> map) {
    final rawPrice = map['price'];
    int price = 0;

    if (rawPrice is int) {
      price = rawPrice;
    } else if (rawPrice is double) {
      price = rawPrice.round();
    } else if (rawPrice is String) {
      price = int.tryParse(rawPrice.replaceAll(RegExp(r'[^0-9]'), '')) ?? 0;
    }

    DateTime? createdAt;
    final rawDate = map['createdAt'];

    if (rawDate is Timestamp) {
      createdAt = rawDate.toDate();
    } else if (rawDate is String) {
      createdAt = DateTime.tryParse(rawDate);
    }

    return PriceRecord(
      price: price,
      storeName: map['storeName']?.toString() ?? '',
      createdAt: createdAt,
    );
  }
}

class _StatCard extends StatelessWidget {
  final String title;
  final String value;

  const _StatCard({
    required this.title,
    required this.value,
  });

  @override
  Widget build(BuildContext context) {
    return Expanded(
      child: Container(
        padding: const EdgeInsets.all(12),
        decoration: BoxDecoration(
          color: Colors.grey.shade100,
          borderRadius: BorderRadius.circular(14),
        ),
        child: Column(
          children: [
            Text(title, style: const TextStyle(fontSize: 12)),
            const SizedBox(height: 4),
            Text(
              value,
              style: const TextStyle(fontWeight: FontWeight.bold),
            ),
          ],
        ),
      ),
    );
  }
}

class PriceChartPainter extends CustomPainter {
  final List<PriceRecord> records;

  PriceChartPainter(this.records);

  @override
  void paint(Canvas canvas, Size size) {
    if (records.length < 2) return;

    final prices = records.map((e) => e.price).toList();
    final minPrice = prices.reduce((a, b) => a < b ? a : b).toDouble();
    final maxPrice = prices.reduce((a, b) => a > b ? a : b).toDouble();

    final range = maxPrice - minPrice == 0 ? 1 : maxPrice - minPrice;

    final linePaint = Paint()
      ..strokeWidth = 3
      ..style = PaintingStyle.stroke
      ..strokeCap = StrokeCap.round;

    final dotPaint = Paint()
      ..style = PaintingStyle.fill;

    final axisPaint = Paint()
      ..strokeWidth = 1
      ..color = Colors.black12;

    final path = Path();

    for (int i = 0; i < records.length; i++) {
      final x = records.length == 1
          ? size.width / 2
          : (size.width / (records.length - 1)) * i;

      final normalized = (records[i].price - minPrice) / range;
      final y = size.height - (normalized * size.height);

      if (i == 0) {
        path.moveTo(x, y);
      } else {
        path.lineTo(x, y);
      }
    }

    canvas.drawLine(
      Offset(0, size.height),
      Offset(size.width, size.height),
      axisPaint,
    );

    canvas.drawPath(path, linePaint);

    for (int i = 0; i < records.length; i++) {
      final x = records.length == 1
          ? size.width / 2
          : (size.width / (records.length - 1)) * i;

      final normalized = (records[i].price - minPrice) / range;
      final y = size.height - (normalized * size.height);

      canvas.drawCircle(Offset(x, y), 4, dotPaint);
    }
  }

  @override
  bool shouldRepaint(covariant PriceChartPainter oldDelegate) {
    return oldDelegate.records != records;
  }
}

String _formatDate(DateTime? date) {
  if (date == null) return '-';

  return '${date.month}/${date.day}';
}