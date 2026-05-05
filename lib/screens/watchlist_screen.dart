import 'package:cloud_firestore/cloud_firestore.dart';
import 'package:flutter/material.dart';

import '../utils/item_helpers.dart';
import '../utils/launch_utils.dart';
import '../utils/region_filter.dart';
import '../widgets/product_card.dart';

class WatchlistScreen extends StatelessWidget {
  const WatchlistScreen({super.key});

  String _sourceLabel(String raw) {
    switch (raw.trim().toLowerCase()) {
      case 'gundambase_notice':
        return '건담베이스';
      case 'gundamshop':
        return '건담샵';
      case 'bnkrmall':
        return '반다이남코코리아몰';
      case 'smartstore':
        return '스마트스토어';
      default:
        return raw.trim().isEmpty ? '알 수 없음' : raw.trim();
    }
  }

  List<String> _verificationSourceLabels(Map<String, dynamic> data) {
    final raw = data['verificationSources'];

    if (raw is Iterable) {
      final result = <String>[];
      for (final item in raw) {
        final label = _sourceLabel(item.toString());
        if (!result.contains(label)) {
          result.add(label);
        }
      }
      return result;
    }

    final source = (data['source'] ?? '').toString();
    if (source.isNotEmpty) {
      return [_sourceLabel(source)];
    }

    return [];
  }

  String _sellerText(Map<String, dynamic> data) {
    final labels = _verificationSourceLabels(data);

    if (labels.isEmpty) {
      final fallback = (data['mallName'] ?? data['site'] ?? data['source'] ?? '')
          .toString()
          .trim();
      return fallback.isEmpty ? '알 수 없음' : fallback;
    }

    if (labels.length == 1) {
      return labels.first;
    }

    return '${labels.first} 외 ${labels.length - 1}곳';
  }

  Map<String, dynamic> _enrichData(Map<String, dynamic> data) {
    final sellerText = _sellerText(data);

    return {
      ...data,
      'mallName': sellerText,
      'site': sellerText,
    };
  }

  @override
  Widget build(BuildContext context) {
    final watchlistRef = FirebaseFirestore.instance
        .collection('watchlist')
        .orderBy('createdAt', descending: true);

    return Scaffold(
      appBar: AppBar(
        title: const Text('관심상품'),
        centerTitle: true,
      ),
      body: StreamBuilder<QuerySnapshot>(
        stream: watchlistRef.snapshots(),
        builder: (context, snapshot) {
          if (snapshot.hasError) {
            return const Center(
              child: Text('관심상품을 불러오는 중 오류가 발생했습니다.'),
            );
          }

          if (snapshot.connectionState == ConnectionState.waiting) {
            return const Center(child: CircularProgressIndicator());
          }

          final docs = snapshot.data?.docs ?? [];

          final filteredDocs = docs.where((doc) {
            final data = doc.data() as Map<String, dynamic>;
            return RegionFilter.isKrItem(data) &&
                !ItemHelpers.shouldHideBrokenOrGeneric(data);
          }).toList();

          if (filteredDocs.isEmpty) {
            return const Center(
              child: Text('관심상품에 담긴 한국 상품이 없습니다.'),
            );
          }

          return ListView.separated(
            padding: const EdgeInsets.fromLTRB(12, 12, 12, 16),
            itemCount: filteredDocs.length,
            separatorBuilder: (_, __) => const SizedBox(height: 10),
            itemBuilder: (context, index) {
              final doc = filteredDocs[index];
              final rawData = doc.data() as Map<String, dynamic>;
              final data = _enrichData(rawData);
              final isNotice = ItemHelpers.isNoticeItem(data);

              return ProductCard(
                data: data,
                primaryLabel: '삭제',
                secondaryLabel: '보기',
                primaryIcon: Icons.delete_outline,
                secondaryIcon: Icons.open_in_new,
                onSecondaryPressed: () {
                  final productUrl = ItemHelpers.itemProductUrl(data);
                  LaunchUtils.openUrl(context, productUrl);
                },
                onPrimaryPressed: () async {
                  await FirebaseFirestore.instance
                      .collection('watchlist')
                      .doc(doc.id)
                      .delete();

                  if (!context.mounted) return;

                  ScaffoldMessenger.of(context).showSnackBar(
                    SnackBar(
                      content: Text(
                        isNotice ? '관심 공지를 삭제했습니다.' : '관심상품을 삭제했습니다.',
                      ),
                    ),
                  );
                },
              );
            },
          );
        },
      ),
    );
  }
}