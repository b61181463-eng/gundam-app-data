import 'package:flutter/material.dart';
import 'package:url_launcher/url_launcher.dart';

import '../services/grouped_product_service.dart';

class AggregatedInventoryScreen extends StatefulWidget {
  const AggregatedInventoryScreen({super.key});

  @override
  State<AggregatedInventoryScreen> createState() => _AggregatedInventoryScreenState();
}

enum _SortMode {
  availableFirst,
  priceLow,
  sellerCount,
  latest,
  name,
}

class _AggregatedInventoryScreenState extends State<AggregatedInventoryScreen> {
  final TextEditingController _searchController = TextEditingController();
  String _searchText = '';
  String _selectedSeller = '전체';
  String _selectedGrade = '전체';
  _SortMode _sortMode = _SortMode.availableFirst;

  @override
  void dispose() {
    _searchController.dispose();
    super.dispose();
  }

  String _cleanSearch(String raw) {
    return raw.toLowerCase().replaceAll(RegExp(r'[^a-z0-9가-힣]'), '');
  }

  List<String> _sellerOptions(List<GroupedProduct> products) {
    final set = <String>{};
    for (final product in products) {
      for (final offer in product.offers) {
        if (offer.seller.trim().isNotEmpty) set.add(offer.seller.trim());
      }
    }
    final list = set.toList()..sort();
    return ['전체', ...list];
  }

  List<String> _gradeOptions(List<GroupedProduct> products) {
    final set = <String>{};
    for (final product in products) {
      final grade = product.grade.trim();
      if (grade.isNotEmpty && grade != 'UNKNOWN') set.add(grade);
    }
    final preferred = ['PG', 'MGEX', 'MG', 'RG', 'HG', 'SD', 'MGSD', 'FULL MECHANICS', 'RE/100'];
    final list = set.toList()
      ..sort((a, b) {
        final ai = preferred.indexOf(a);
        final bi = preferred.indexOf(b);
        if (ai != -1 && bi != -1) return ai.compareTo(bi);
        if (ai != -1) return -1;
        if (bi != -1) return 1;
        return a.compareTo(b);
      });
    return ['전체', ...list];
  }

  List<GroupedProduct> _applyFilterAndSort(List<GroupedProduct> products) {
    var result = products.where((product) {
      if (_selectedSeller != '전체' &&
          !product.offers.any((offer) => offer.seller == _selectedSeller)) {
        return false;
      }

      if (_selectedGrade != '전체' && product.grade != _selectedGrade) {
        return false;
      }

      if (_searchText.trim().isEmpty) return true;
      final query = _cleanSearch(_searchText);
      final target = _cleanSearch(product.searchText);
      return target.contains(query);
    }).toList();

    result.sort((a, b) {
      switch (_sortMode) {
        case _SortMode.priceLow:
          final ap = a.minPriceInt;
          final bp = b.minPriceInt;
          if (ap != null && bp != null && ap != bp) return ap.compareTo(bp);
          if (ap == null && bp != null) return 1;
          if (ap != null && bp == null) return -1;
          return a.name.compareTo(b.name);
        case _SortMode.sellerCount:
          final sellerCompare = b.sellerCount.compareTo(a.sellerCount);
          if (sellerCompare != 0) return sellerCompare;
          return a.name.compareTo(b.name);
        case _SortMode.latest:
          final at = a.updatedAt;
          final bt = b.updatedAt;
          if (at != null && bt != null) return bt.compareTo(at);
          if (at == null && bt != null) return 1;
          if (at != null && bt == null) return -1;
          return a.name.compareTo(b.name);
        case _SortMode.name:
          return a.name.compareTo(b.name);
        case _SortMode.availableFirst:
          final ar = _statusRank(a.status);
          final br = _statusRank(b.status);
          if (ar != br) return ar.compareTo(br);
          final ap = a.minPriceInt;
          final bp = b.minPriceInt;
          if (ap != null && bp != null && ap != bp) return ap.compareTo(bp);
          return a.name.compareTo(b.name);
      }
    });

    return result;
  }

  int _statusRank(String status) {
    switch (status) {
      case '판매중':
        return 0;
      case '예약중':
        return 1;
      case '입고예정':
        return 2;
      case '품절':
        return 3;
      default:
        return 4;
    }
  }

  Color _statusColor(String status) {
    switch (status) {
      case '판매중':
        return Colors.green;
      case '예약중':
        return Colors.blue;
      case '입고예정':
        return Colors.orange;
      case '품절':
        return Colors.red;
      default:
        return Colors.grey;
    }
  }

  String _sortLabel(_SortMode mode) {
    switch (mode) {
      case _SortMode.availableFirst:
        return '판매중 우선';
      case _SortMode.priceLow:
        return '낮은 가격순';
      case _SortMode.sellerCount:
        return '판매처 많은순';
      case _SortMode.latest:
        return '최신 업데이트순';
      case _SortMode.name:
        return '이름순';
    }
  }

  String _formatDate(DateTime? value) {
    if (value == null) return '업데이트 확인중';
    final local = value.toLocal();
    final month = local.month.toString().padLeft(2, '0');
    final day = local.day.toString().padLeft(2, '0');
    final hour = local.hour.toString().padLeft(2, '0');
    final minute = local.minute.toString().padLeft(2, '0');
    return '$month/$day $hour:$minute';
  }

  Future<void> _openUrl(String url) async {
    if (url.trim().isEmpty) {
      ScaffoldMessenger.of(context).showSnackBar(
        const SnackBar(content: Text('열 수 있는 링크가 없어요.')),
      );
      return;
    }

    final uri = Uri.tryParse(url);
    if (uri == null) {
      ScaffoldMessenger.of(context).showSnackBar(
        const SnackBar(content: Text('링크 형식이 올바르지 않아요.')),
      );
      return;
    }

    final ok = await launchUrl(uri, mode: LaunchMode.externalApplication);
    if (!ok && mounted) {
      ScaffoldMessenger.of(context).showSnackBar(
        const SnackBar(content: Text('링크를 열지 못했어요.')),
      );
    }
  }

  @override
  Widget build(BuildContext context) {
    return StreamBuilder<List<GroupedProduct>>(
      stream: GroupedProductService.watchGroupedProducts(),
      builder: (context, snapshot) {
        if (snapshot.hasError) {
          return Center(
            child: Padding(
              padding: const EdgeInsets.all(24),
              child: Text('상품을 불러오는 중 오류가 발생했어요.\n${snapshot.error}'),
            ),
          );
        }

        if (!snapshot.hasData) {
          return const Center(child: CircularProgressIndicator());
        }

        final allProducts = snapshot.data!;
        final products = _applyFilterAndSort(allProducts);
        final sellerOptions = _sellerOptions(allProducts);
        final gradeOptions = _gradeOptions(allProducts);

        return Column(
          children: [
            _buildHeader(allProducts.length, products.length),
            _buildFilters(sellerOptions, gradeOptions),
            Expanded(
              child: products.isEmpty
                  ? const Center(child: Text('조건에 맞는 상품이 없어요.'))
                  : ListView.builder(
                      padding: const EdgeInsets.fromLTRB(16, 8, 16, 24),
                      itemCount: products.length,
                      itemBuilder: (context, index) {
                        return _ProductCard(
                          product: products[index],
                          statusColor: _statusColor(products[index].status),
                          formatDate: _formatDate,
                          onOpen: _openUrl,
                        );
                      },
                    ),
            ),
          ],
        );
      },
    );
  }

  Widget _buildHeader(int total, int visible) {
    final multiSellerText = visible == total ? '총 $total개 그룹' : '$visible / $total개 그룹';

    return Padding(
      padding: const EdgeInsets.fromLTRB(16, 16, 16, 8),
      child: Row(
        children: [
          Expanded(
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                const Text(
                  '최저가 비교',
                  style: TextStyle(fontSize: 20, fontWeight: FontWeight.bold),
                ),
                const SizedBox(height: 4),
                Text(
                  multiSellerText,
                  style: TextStyle(fontSize: 13, color: Colors.grey.shade600),
                ),
              ],
            ),
          ),
          IconButton(
            tooltip: '검색 초기화',
            onPressed: () {
              _searchController.clear();
              setState(() {
                _searchText = '';
                _selectedSeller = '전체';
                _selectedGrade = '전체';
                _sortMode = _SortMode.availableFirst;
              });
            },
            icon: const Icon(Icons.refresh),
          ),
        ],
      ),
    );
  }

  Widget _buildFilters(List<String> sellerOptions, List<String> gradeOptions) {
    return Padding(
      padding: const EdgeInsets.fromLTRB(16, 4, 16, 8),
      child: Column(
        children: [
          TextField(
            controller: _searchController,
            decoration: InputDecoration(
              hintText: '상품명, 등급, 판매처 검색',
              prefixIcon: const Icon(Icons.search),
              suffixIcon: _searchText.isNotEmpty
                  ? IconButton(
                      onPressed: () {
                        _searchController.clear();
                        setState(() => _searchText = '');
                      },
                      icon: const Icon(Icons.close),
                    )
                  : null,
              border: OutlineInputBorder(borderRadius: BorderRadius.circular(14)),
              isDense: true,
            ),
            onChanged: (value) => setState(() => _searchText = value),
          ),
          const SizedBox(height: 10),
          Row(
            children: [
              Expanded(
                child: _DropdownBox<String>(
                  value: sellerOptions.contains(_selectedSeller) ? _selectedSeller : '전체',
                  items: sellerOptions,
                  labelBuilder: (value) => value,
                  onChanged: (value) => setState(() => _selectedSeller = value),
                ),
              ),
              const SizedBox(width: 8),
              Expanded(
                child: _DropdownBox<String>(
                  value: gradeOptions.contains(_selectedGrade) ? _selectedGrade : '전체',
                  items: gradeOptions,
                  labelBuilder: (value) => value,
                  onChanged: (value) => setState(() => _selectedGrade = value),
                ),
              ),
            ],
          ),
          const SizedBox(height: 8),
          _DropdownBox<_SortMode>(
            value: _sortMode,
            items: _SortMode.values,
            labelBuilder: _sortLabel,
            onChanged: (value) => setState(() => _sortMode = value),
          ),
        ],
      ),
    );
  }
}

class _DropdownBox<T> extends StatelessWidget {
  final T value;
  final List<T> items;
  final String Function(T value) labelBuilder;
  final ValueChanged<T> onChanged;

  const _DropdownBox({
    required this.value,
    required this.items,
    required this.labelBuilder,
    required this.onChanged,
  });

  @override
  Widget build(BuildContext context) {
    return Container(
      padding: const EdgeInsets.symmetric(horizontal: 12),
      decoration: BoxDecoration(
        border: Border.all(color: Colors.grey.shade300),
        borderRadius: BorderRadius.circular(14),
      ),
      child: DropdownButtonHideUnderline(
        child: DropdownButton<T>(
          value: value,
          isExpanded: true,
          items: items
              .map(
                (item) => DropdownMenuItem<T>(
                  value: item,
                  child: Text(labelBuilder(item), overflow: TextOverflow.ellipsis),
                ),
              )
              .toList(),
          onChanged: (next) {
            if (next != null) onChanged(next);
          },
        ),
      ),
    );
  }
}

class _ProductCard extends StatelessWidget {
  final GroupedProduct product;
  final Color statusColor;
  final String Function(DateTime? value) formatDate;
  final Future<void> Function(String url) onOpen;

  const _ProductCard({
    required this.product,
    required this.statusColor,
    required this.formatDate,
    required this.onOpen,
  });

  @override
  Widget build(BuildContext context) {
    return Card(
      margin: const EdgeInsets.only(bottom: 12),
      shape: RoundedRectangleBorder(borderRadius: BorderRadius.circular(18)),
      elevation: 2,
      child: ExpansionTile(
        tilePadding: const EdgeInsets.fromLTRB(14, 8, 14, 8),
        childrenPadding: const EdgeInsets.fromLTRB(14, 0, 14, 14),
        leading: Container(
          width: 48,
          height: 48,
          decoration: BoxDecoration(
            color: statusColor.withOpacity(0.12),
            borderRadius: BorderRadius.circular(14),
          ),
          child: Icon(Icons.inventory_2_outlined, color: statusColor),
        ),
        title: Text(
          product.name,
          maxLines: 2,
          overflow: TextOverflow.ellipsis,
          style: const TextStyle(fontSize: 15.5, fontWeight: FontWeight.bold),
        ),
        subtitle: Padding(
          padding: const EdgeInsets.only(top: 8),
          child: Wrap(
            spacing: 6,
            runSpacing: 6,
            crossAxisAlignment: WrapCrossAlignment.center,
            children: [
              _Chip(label: product.status, color: statusColor, filled: true),
              _Chip(label: product.grade == 'UNKNOWN' ? '등급 확인중' : product.grade),
              _Chip(label: '${product.sellerCount}곳 비교'),
              _Chip(label: formatDate(product.updatedAt)),
            ],
          ),
        ),
        trailing: Column(
          mainAxisAlignment: MainAxisAlignment.center,
          crossAxisAlignment: CrossAxisAlignment.end,
          children: [
            const Text('최저가', style: TextStyle(fontSize: 11, color: Colors.grey)),
            Text(
              product.minPriceText,
              style: const TextStyle(fontSize: 15, fontWeight: FontWeight.w800),
            ),
          ],
        ),
        children: [
          const Divider(height: 14),
          Row(
            children: [
              Expanded(
                child: Text(
                  '최저가: ${product.bestOffer.seller}',
                  style: const TextStyle(fontWeight: FontWeight.bold),
                ),
              ),
              FilledButton.icon(
                onPressed: () => onOpen(product.bestOffer.url),
                icon: const Icon(Icons.open_in_new, size: 16),
                label: const Text('최저가 열기'),
              ),
            ],
          ),
          const SizedBox(height: 10),
          ...product.offers.map(
            (offer) => _OfferRow(
              offer: offer,
              isBest: offer.itemId == product.bestOffer.itemId,
              onOpen: onOpen,
            ),
          ),
        ],
      ),
    );
  }
}

class _Chip extends StatelessWidget {
  final String label;
  final Color? color;
  final bool filled;

  const _Chip({
    required this.label,
    this.color,
    this.filled = false,
  });

  @override
  Widget build(BuildContext context) {
    final effective = color ?? Colors.blueGrey;
    return Container(
      padding: const EdgeInsets.symmetric(horizontal: 9, vertical: 4),
      decoration: BoxDecoration(
        color: filled ? effective : effective.withOpacity(0.10),
        borderRadius: BorderRadius.circular(999),
      ),
      child: Text(
        label,
        style: TextStyle(
          fontSize: 11.5,
          fontWeight: FontWeight.w700,
          color: filled ? Colors.white : effective,
        ),
      ),
    );
  }
}

class _OfferRow extends StatelessWidget {
  final ProductOffer offer;
  final bool isBest;
  final Future<void> Function(String url) onOpen;

  const _OfferRow({
    required this.offer,
    required this.isBest,
    required this.onOpen,
  });

  Color _statusColor(String status) {
    switch (status) {
      case '판매중':
        return Colors.green;
      case '예약중':
        return Colors.blue;
      case '입고예정':
        return Colors.orange;
      case '품절':
        return Colors.red;
      default:
        return Colors.grey;
    }
  }

  @override
  Widget build(BuildContext context) {
    final color = _statusColor(offer.status);

    return Container(
      margin: const EdgeInsets.only(bottom: 8),
      padding: const EdgeInsets.all(10),
      decoration: BoxDecoration(
        color: isBest ? Colors.green.withOpacity(0.06) : Colors.grey.shade50,
        borderRadius: BorderRadius.circular(14),
        border: Border.all(
          color: isBest ? Colors.green.withOpacity(0.35) : Colors.grey.shade200,
        ),
      ),
      child: Row(
        children: [
          Expanded(
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Row(
                  children: [
                    Flexible(
                      child: Text(
                        offer.seller,
                        maxLines: 1,
                        overflow: TextOverflow.ellipsis,
                        style: const TextStyle(fontWeight: FontWeight.bold),
                      ),
                    ),
                    if (isBest) ...[
                      const SizedBox(width: 6),
                      const _Chip(label: '최저가', color: Colors.green, filled: true),
                    ],
                  ],
                ),
                const SizedBox(height: 4),
                Text(
                  offer.status,
                  style: TextStyle(color: color, fontWeight: FontWeight.w700, fontSize: 12),
                ),
              ],
            ),
          ),
          const SizedBox(width: 8),
          Text(
            offer.priceText,
            style: const TextStyle(fontWeight: FontWeight.w800),
          ),
          IconButton(
            tooltip: '판매처 열기',
            onPressed: () => onOpen(offer.url),
            icon: const Icon(Icons.open_in_new, size: 18),
          ),
        ],
      ),
    );
  }
}
