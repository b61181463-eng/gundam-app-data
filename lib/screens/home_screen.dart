import 'package:flutter/material.dart';
import 'package:url_launcher/url_launcher.dart';

import '../services/favorite_service.dart';
import '../services/price_history_service.dart';
import '../services/stock_api.dart';
import '../services/user_stock_report_service.dart';

enum HomeFilterType {
  all,
  notice,
  restock,
  favorites,
  unknownStatus,
}

enum GradeFilterType {
  all,
  pg,
  mg,
  rg,
  hg,
  sd,
}

class HomeScreen extends StatefulWidget {
  const HomeScreen({super.key});

  @override
  State<HomeScreen> createState() => _HomeScreenState();
}

class _HomeScreenState extends State<HomeScreen> with TickerProviderStateMixin {
  final TextEditingController _searchController = TextEditingController();
  final FocusNode _searchFocusNode = FocusNode();

  Set<String> _favoriteIds = {};

  String _searchQuery = '';
  HomeFilterType _filterType = HomeFilterType.all;
  GradeFilterType _gradeFilter = GradeFilterType.all;
  bool _isGradeExpanded = false;

  @override
  void initState() {
    super.initState();
    _loadFavorites();
  }

  Future<void> _loadFavorites() async {
    final favorites = await FavoriteService.loadFavorites();
    if (!mounted) return;
    setState(() {
      _favoriteIds = favorites;
    });
  }

  @override
  void dispose() {
    _searchController.dispose();
    _searchFocusNode.dispose();
    super.dispose();
  }

  Future<void> _refresh() async {
    await _loadFavorites();
    setState(() {});
  }

  Future<void> _toggleFavorite(StockItem item) async {
    try {
      final favorites = await FavoriteService.toggleFavorite(item.itemId);
      if (!mounted) return;

      final nowFavorite = favorites.contains(item.itemId);

      setState(() {
        _favoriteIds = favorites;
      });

      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(
          content: Text(
            nowFavorite ? '찜 목록에 추가했고 알림도 켰어요.' : '찜 목록에서 제거했고 알림도 껐어요.',
          ),
        ),
      );
    } catch (e) {
      if (!mounted) return;
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(
          content: Text('찜하기 처리 중 오류가 발생했어요: $e'),
        ),
      );
    }
  }

  bool _isFavorite(StockItem item) => _favoriteIds.contains(item.itemId);

  void _toggleNoticeMode() {
    setState(() {
      if (_filterType == HomeFilterType.notice) {
        _filterType = HomeFilterType.all;
      } else {
        _filterType = HomeFilterType.notice;
      }
    });
  }

  void _toggleFavoriteMode() {
    setState(() {
      if (_filterType == HomeFilterType.favorites) {
        _filterType = HomeFilterType.all;
      } else {
        _filterType = HomeFilterType.favorites;
      }
    });
  }

  void _toggleAllAndGrades() {
    setState(() {
      _filterType = HomeFilterType.all;
      _isGradeExpanded = !_isGradeExpanded;

      if (!_isGradeExpanded) {
        _gradeFilter = GradeFilterType.all;
      }
    });
  }

  String _searchNormalized(String text) {
    return text
        .toLowerCase()
        .replaceAll(RegExp(r'[\s\[\]\(\)\-_/.,:+]+'), '');
  }

  Future<void> _openExternalLink(StockItem item) async {
    final link = item.resolvedUrl.trim();

    if (link.isEmpty) {
      if (!mounted) return;
      ScaffoldMessenger.of(context).showSnackBar(
        const SnackBar(content: Text('연결할 상세 페이지 링크가 없습니다.')),
      );
      return;
    }

    final uri = Uri.tryParse(link);
    if (uri == null) {
      if (!mounted) return;
      ScaffoldMessenger.of(context).showSnackBar(
        const SnackBar(content: Text('페이지 주소 형식이 올바르지 않습니다.')),
      );
      return;
    }

    if (await canLaunchUrl(uri)) {
      await launchUrl(uri, mode: LaunchMode.externalApplication);
    } else {
      if (!mounted) return;
      ScaffoldMessenger.of(context).showSnackBar(
        const SnackBar(content: Text('페이지를 열 수 없습니다.')),
      );
    }
  }

  Color offerStatusColor(String status) {
    if (status == '판매중') return const Color(0xFF2E7D32);
    if (status == '예약중') return const Color(0xFFEF6C00);
    if (status == '품절') return const Color(0xFFC62828);
    if (status == '입고예정') return Colors.indigo;
    return Colors.grey;
  }

  void _showItemOffersSheet(StockItem item) {
    showModalBottomSheet(
      context: context,
      isScrollControlled: true,
      shape: const RoundedRectangleBorder(
        borderRadius: BorderRadius.vertical(top: Radius.circular(20)),
      ),
      builder: (context) {
        return DraggableScrollableSheet(
          expand: false,
          initialChildSize: 0.7,
          minChildSize: 0.5,
          maxChildSize: 0.9,
          builder: (context, scrollController) {
            return Padding(
              padding: const EdgeInsets.fromLTRB(16, 16, 16, 20),
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  Center(
                    child: Container(
                      width: 42,
                      height: 5,
                      decoration: BoxDecoration(
                        color: Colors.grey.shade300,
                        borderRadius: BorderRadius.circular(999),
                      ),
                    ),
                  ),
                  const SizedBox(height: 16),
                  Text(
                    _itemName(item),
                    style: const TextStyle(
                      fontSize: 18,
                      fontWeight: FontWeight.bold,
                      height: 1.35,
                    ),
                  ),
                  const SizedBox(height: 8),
                  Text(
                    '판매처 ${item.sellerCount}곳 · 최저가 ${item.minPrice.isNotEmpty ? item.minPrice : '-'}',
                    style: TextStyle(
                      color: Colors.grey.shade700,
                      fontWeight: FontWeight.w600,
                    ),
                  ),
                  const SizedBox(height: 16),
                  Expanded(
                    child: ListView.builder(
                      controller: scrollController,
                      itemCount: item.offers.length,
                      itemBuilder: (context, index) {
                        final offer = item.offers[index];
                        final isLowest = _isLowestOffer(item, offer);

                        return Container(
                          margin: const EdgeInsets.only(bottom: 10),
                          padding: const EdgeInsets.all(12),
                          decoration: BoxDecoration(
                            color: isLowest
                                ? Colors.teal.shade50
                                : Colors.grey.shade100,
                            borderRadius: BorderRadius.circular(12),
                            border: Border.all(
                              color: isLowest
                                  ? Colors.teal.shade100
                                  : Colors.transparent,
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
                                        Expanded(
                                          child: Text(
                                            offer.seller,
                                            maxLines: 1,
                                            overflow: TextOverflow.ellipsis,
                                            style: const TextStyle(
                                              fontWeight: FontWeight.bold,
                                            ),
                                          ),
                                        ),
                                        if (isLowest)
                                          _buildBadge(
                                            text: '최저가',
                                            bgColor: Colors.teal.shade100,
                                            textColor: Colors.teal.shade900,
                                          ),
                                      ],
                                    ),
                                    const SizedBox(height: 4),
                                    Text(
                                      offer.price.isNotEmpty ? offer.price : '-',
                                    ),
                                    const SizedBox(height: 2),
                                    Text(
                                      offer.status,
                                      style: TextStyle(
                                        fontSize: 12,
                                        color: offerStatusColor(offer.status),
                                        fontWeight: FontWeight.w800,
                                      ),
                                    ),
                                  ],
                                ),
                              ),
                              ElevatedButton(
                                onPressed: offer.resolvedUrl.trim().isEmpty
                                    ? null
                                    : () async {
                                        final uri =
                                            Uri.tryParse(offer.resolvedUrl);
                                        if (uri != null &&
                                            await canLaunchUrl(uri)) {
                                          await launchUrl(
                                            uri,
                                            mode:
                                                LaunchMode.externalApplication,
                                          );
                                        }
                                      },
                                child: const Text('보기'),
                              ),
                            ],
                          ),
                        );
                      },
                    ),
                  ),
                ],
              ),
            );
          },
        );
      },
    );
  }

  DateTime? _parseNoticeDate(String raw) {
    final text = raw.trim();
    if (text.isEmpty) return null;

    final normalized = text.replaceAll('.', '-').replaceAll('/', '-');

    try {
      return DateTime.parse(normalized);
    } catch (_) {}

    final match =
        RegExp(r'(20\d{2})-(\d{1,2})-(\d{1,2})').firstMatch(normalized);
    if (match != null) {
      final y = int.tryParse(match.group(1)!);
      final m = int.tryParse(match.group(2)!);
      final d = int.tryParse(match.group(3)!);
      if (y != null && m != null && d != null) {
        return DateTime(y, m, d);
      }
    }

    return null;
  }

  String _formatNoticeTitle(StockItem item) {
    final parsed = _parseNoticeDate(item.noticeDate) ?? DateTime.now();
    return '${parsed.year}년 ${parsed.month}월 ${parsed.day}일 건담 공지';
  }

  String _cleanDisplayName(String raw) {
    var text = raw.trim();
    if (text.isEmpty) return '이름 없는 상품';

    final preservedGradeBracket = RegExp(
      r'^\[(MGEX|MGSD|PG|MG|RG|HG|SD)\]\s*',
      caseSensitive: false,
    ).firstMatch(text);

    if (preservedGradeBracket != null) {
      return text.replaceAll(RegExp(r'\s+'), ' ').trim();
    }

    text = text.replaceFirst(
      RegExp(r'^(?:\[[^\]]+\]|\([^)]+\)|【[^】]+】)\s*'),
      '',
    );

    final gradeMatch = RegExp(
      r'(\[?(MGEX|MGSD|PG|MG|RG|HG|SD)\]?|RE/100|FULL MECHANICS)',
      caseSensitive: false,
    ).firstMatch(text);

    if (gradeMatch != null) {
      text = text.substring(gradeMatch.start).trim();
    }

    text = text.replaceFirstMapped(
      RegExp(r'^\((MGEX|MGSD|PG|MG|RG|HG|SD)\)\s*', caseSensitive: false),
      (m) => '${m.group(1)} ',
    );

    text = text.replaceAll(RegExp(r'\s+'), ' ').trim();

    return text.isEmpty ? '이름 없는 상품' : text;
  }

  String _itemName(StockItem item) {
    if (_isNoticeItem(item)) {
      return _formatNoticeTitle(item);
    }

    final raw = item.name.trim().isNotEmpty
        ? item.name.trim()
        : item.title.trim().isNotEmpty
            ? item.title.trim()
            : '';

    return _cleanDisplayName(raw);
  }

  String _sellerText(StockItem item) {
    if (_isNoticeItem(item)) return '건담 공지';
    if (item.sellerCount > 1) return '판매처 ${item.sellerCount}곳';
    if (item.offers.isNotEmpty && item.offers.first.seller.trim().isNotEmpty) {
      return item.offers.first.seller.trim();
    }
    if (item.mallName.trim().isNotEmpty) return item.mallName.trim();
    if (item.site.trim().isNotEmpty) return item.site.trim();
    return '알 수 없음';
  }

  bool _isNoticeItem(StockItem item) => item.status == '공지';
  bool _isUnknownStatus(StockItem item) => item.status == '상태 확인중';

  bool _hasAvailableOffer(StockItem item) {
    return item.status == '판매중' ||
        item.offers.any((offer) => offer.status == '판매중');
  }

  bool _isOnlySoldOut(StockItem item) {
    if (_isNoticeItem(item)) return false;
    if (_hasAvailableOffer(item)) return false;
    if (item.offers.isEmpty) return item.status == '품절';
    return item.offers.every((offer) => offer.status == '품절');
  }

  bool _isLowestOffer(StockItem item, StockOffer offer) {
    final offerPrice = offer.price.replaceAll(RegExp(r'[^0-9]'), '');
    final minPrice = item.minPrice.replaceAll(RegExp(r'[^0-9]'), '');
    return offerPrice.isNotEmpty && minPrice.isNotEmpty && offerPrice == minPrice;
  }

  bool _isCrossChecked(StockItem item) {
    return !_isNoticeItem(item) && item.sellerCount > 1;
  }

  String _itemGrade(StockItem item) {
    if (_isNoticeItem(item)) return '';

    final text = [
      _itemName(item),
      item.title,
    ].join(' ').toUpperCase();

    if (text.contains('MGSD')) return 'SD';
    if (text.contains('MGEX')) return 'MG';
    if (RegExp(r'(^|\s|\[|\()PG($|\s|\]|\))').hasMatch(text) ||
        text.contains('[PG]')) {
      return 'PG';
    }
    if (RegExp(r'(^|\s|\[|\()MG($|\s|\]|\))').hasMatch(text) ||
        text.contains('[MG]')) {
      return 'MG';
    }
    if (RegExp(r'(^|\s|\[|\()RG($|\s|\]|\))').hasMatch(text) ||
        text.contains('[RG]')) {
      return 'RG';
    }
    if (RegExp(r'(^|\s|\[|\()HG($|\s|\]|\))').hasMatch(text) ||
        text.contains('[HG]')) {
      return 'HG';
    }
    if (RegExp(r'(^|\s|\[|\()SD($|\s|\]|\))').hasMatch(text) ||
        text.contains('[SD]') ||
        text.contains('SDW') ||
        text.contains('BB전사')) {
      return 'SD';
    }
    return '';
  }

  bool _matchesGradeFilter(StockItem item) {
    if (_gradeFilter == GradeFilterType.all) return true;
    final grade = _itemGrade(item);

    switch (_gradeFilter) {
      case GradeFilterType.all:
        return true;
      case GradeFilterType.pg:
        return grade == 'PG';
      case GradeFilterType.mg:
        return grade == 'MG';
      case GradeFilterType.rg:
        return grade == 'RG';
      case GradeFilterType.hg:
        return grade == 'HG';
      case GradeFilterType.sd:
        return grade == 'SD';
    }
  }

  List<StockItem> _applyFilters(List<StockItem> items) {
    final typeFiltered = items.where((item) {
      switch (_filterType) {
        case HomeFilterType.all:
          return true;
        case HomeFilterType.notice:
          return _isNoticeItem(item);
        case HomeFilterType.restock:
          return item.isRestocked;
        case HomeFilterType.favorites:
          return _isFavorite(item);
        case HomeFilterType.unknownStatus:
          return _isUnknownStatus(item);
      }
    }).toList();

    final gradeFiltered = typeFiltered.where((item) {
      if (_isNoticeItem(item)) {
        return _gradeFilter == GradeFilterType.all;
      }
      return _matchesGradeFilter(item);
    }).toList();

    if (_searchNormalized(_searchQuery).isEmpty) {
      return gradeFiltered;
    }

    final q = _searchNormalized(_searchQuery);

    return gradeFiltered.where((item) {
      final name = _searchNormalized(_itemName(item));
      final seller = _searchNormalized(_sellerText(item));
      final grade = _searchNormalized(_itemGrade(item));
      final title = _searchNormalized(item.title);
      final site = _searchNormalized(item.site);
      final mallName = _searchNormalized(item.mallName);
      final stockText = _searchNormalized(item.stockText);
      final status = _searchNormalized(item.status);
      final offersText = _searchNormalized(
        item.offers
            .map((offer) => '${offer.seller} ${offer.price} ${offer.status}')
            .join(' '),
      );

      return name.contains(q) ||
          seller.contains(q) ||
          grade.contains(q) ||
          title.contains(q) ||
          site.contains(q) ||
          mallName.contains(q) ||
          stockText.contains(q) ||
          status.contains(q) ||
          offersText.contains(q);
    }).toList();
  }

  Widget _buildTopActionChip({
    required String label,
    required bool selected,
    required VoidCallback onTap,
    IconData? icon,
    double minWidth = 0,
  }) {
    return InkWell(
      borderRadius: BorderRadius.circular(999),
      onTap: onTap,
      child: ConstrainedBox(
        constraints: BoxConstraints(minWidth: minWidth),
        child: Container(
          padding: const EdgeInsets.symmetric(horizontal: 14, vertical: 10),
          decoration: BoxDecoration(
            color: selected ? Colors.black : Colors.white,
            borderRadius: BorderRadius.circular(999),
            border: Border.all(
              color: selected ? Colors.black : Colors.grey.shade300,
            ),
          ),
          child: Row(
            mainAxisSize: MainAxisSize.min,
            children: [
              if (icon != null) ...[
                Icon(
                  icon,
                  size: 16,
                  color: selected ? Colors.white : Colors.black87,
                ),
                const SizedBox(width: 6),
              ],
              Text(
                label,
                style: TextStyle(
                  fontSize: 13,
                  fontWeight: FontWeight.w700,
                  color: selected ? Colors.white : Colors.black87,
                ),
              ),
            ],
          ),
        ),
      ),
    );
  }

  String _emptyMessage() {
    switch (_filterType) {
      case HomeFilterType.all:
        return '표시할 한국 상품이 없습니다.';
      case HomeFilterType.notice:
        return '표시할 공지가 없습니다.';
      case HomeFilterType.restock:
        return '재입고 감지 상품이 없습니다.';
      case HomeFilterType.favorites:
        return '찜한 상품이 없습니다.';
      case HomeFilterType.unknownStatus:
        return '상태 확인중 상품이 없습니다.';
    }
  }

  Widget _summaryPill({
    required String label,
    required String value,
    required Color bgColor,
    required Color textColor,
  }) {
    return Container(
      padding: const EdgeInsets.symmetric(horizontal: 10, vertical: 8),
      decoration: BoxDecoration(
        color: bgColor,
        borderRadius: BorderRadius.circular(999),
      ),
      child: Text(
        '$label  $value',
        style: TextStyle(
          fontWeight: FontWeight.w700,
          color: textColor,
        ),
      ),
    );
  }

  Widget _buildSummaryCard(List<StockItem> items) {
    final normalCount = items.where((item) => !_isNoticeItem(item)).length;
    final noticeCount = items.where(_isNoticeItem).length;
    final restockedCount = items.where((item) => item.isRestocked).length;
    final favoriteCount = items.where(_isFavorite).length;
    final unknownCount = items.where(_isUnknownStatus).length;
    final crossCheckedCount = items.where(_isCrossChecked).length;

    return Container(
      width: double.infinity,
      padding: const EdgeInsets.all(12),
      decoration: BoxDecoration(
        color: Colors.orange.shade50,
        borderRadius: BorderRadius.circular(14),
        border: Border.all(color: Colors.orange.shade100),
      ),
      child: Wrap(
        spacing: 8,
        runSpacing: 8,
        children: [
          _summaryPill(
            label: '일반 상품',
            value: '$normalCount개',
            bgColor: Colors.green.shade100,
            textColor: Colors.green.shade900,
          ),
          _summaryPill(
            label: '공지',
            value: '$noticeCount개',
            bgColor: Colors.orange.shade100,
            textColor: Colors.orange.shade900,
          ),
          _summaryPill(
            label: '재입고',
            value: '$restockedCount개',
            bgColor: Colors.blue.shade100,
            textColor: Colors.blue.shade900,
          ),
          _summaryPill(
            label: '찜한 상품',
            value: '$favoriteCount개',
            bgColor: Colors.pink.shade100,
            textColor: Colors.pink.shade900,
          ),
          _summaryPill(
            label: '상태 확인중',
            value: '$unknownCount개',
            bgColor: Colors.grey.shade200,
            textColor: Colors.grey.shade900,
          ),
          _summaryPill(
            label: '가격비교',
            value: '$crossCheckedCount개',
            bgColor: Colors.teal.shade100,
            textColor: Colors.teal.shade900,
          ),
        ],
      ),
    );
  }

  Widget _buildRestockSection(List<StockItem> items) {
    final restockedItems =
        items.where((item) => item.isRestocked).take(5).toList();

    if (restockedItems.isEmpty) {
      return const SizedBox.shrink();
    }

    return Container(
      width: double.infinity,
      margin: const EdgeInsets.fromLTRB(12, 0, 12, 8),
      padding: const EdgeInsets.all(12),
      decoration: BoxDecoration(
        color: Colors.blue.shade50,
        borderRadius: BorderRadius.circular(14),
        border: Border.all(color: Colors.blue.shade100),
      ),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Text(
            '🔥 오늘 재입고',
            style: TextStyle(
              fontWeight: FontWeight.w800,
              fontSize: 15,
              color: Colors.blue.shade900,
            ),
          ),
          const SizedBox(height: 10),
          ...restockedItems.map((item) {
            final name = _itemName(item);
            final seller = _sellerText(item);
            final onlySoldOut = _isOnlySoldOut(item);

            return Padding(
              padding: const EdgeInsets.only(bottom: 8),
              child: InkWell(
                borderRadius: BorderRadius.circular(10),
                onTap: item.resolvedUrl.isEmpty
                    ? null
                    : () async {
                        await _openExternalLink(item);
                      },
                child: Container(
                  padding:
                      const EdgeInsets.symmetric(horizontal: 10, vertical: 10),
                  decoration: BoxDecoration(
                    color: onlySoldOut ? Colors.grey.shade50 : Colors.white,
                    borderRadius: BorderRadius.circular(10),
                  ),
                  child: Row(
                    children: [
                      const Icon(Icons.notifications_active, size: 18),
                      const SizedBox(width: 8),
                      Expanded(
                        child: Column(
                          crossAxisAlignment: CrossAxisAlignment.start,
                          children: [
                            Text(
                              name,
                              maxLines: 1,
                              overflow: TextOverflow.ellipsis,
                              style: const TextStyle(
                                fontWeight: FontWeight.w700,
                              ),
                            ),
                            const SizedBox(height: 2),
                            Text(
                              seller,
                              maxLines: 1,
                              overflow: TextOverflow.ellipsis,
                              style: TextStyle(
                                color: Colors.grey.shade700,
                                fontSize: 12,
                              ),
                            ),
                          ],
                        ),
                      ),
                      const SizedBox(width: 8),
                      Text(
                        item.status,
                        style: TextStyle(
                          color: Colors.blue.shade900,
                          fontWeight: FontWeight.w700,
                          fontSize: 12,
                        ),
                      ),
                    ],
                  ),
                ),
              ),
            );
          }),
        ],
      ),
    );
  }

  Color _statusColor(String status) {
    if (status.contains('판매중')) return const Color(0xFF2E7D32);
    if (status.contains('품절')) return const Color(0xFFC62828);
    if (status.contains('예약')) return const Color(0xFFEF6C00);
    if (status.contains('입고예정')) return Colors.indigo.shade700;
    if (status.contains('공지')) return Colors.orange.shade900;
    if (status.contains('확인중')) return Colors.grey.shade700;
    return Colors.grey.shade700;
  }

  Color _statusBgColor(String status) {
    if (status.contains('판매중')) return const Color(0xFFE8F5E9);
    if (status.contains('품절')) return const Color(0xFFFFEBEE);
    if (status.contains('예약')) return const Color(0xFFFFF3E0);
    if (status.contains('입고예정')) return Colors.indigo.shade50;
    if (status.contains('공지')) return Colors.orange.shade50;
    if (status.contains('확인중')) return const Color(0xFFF3F3F3);
    return const Color(0xFFF1F1F1);
  }

  Widget _buildBadge({
    required String text,
    required Color bgColor,
    required Color textColor,
  }) {
    return Container(
      margin: const EdgeInsets.only(right: 6, bottom: 6),
      padding: const EdgeInsets.symmetric(horizontal: 10, vertical: 6),
      decoration: BoxDecoration(
        color: bgColor,
        borderRadius: BorderRadius.circular(999),
      ),
      child: Text(
        text,
        style: TextStyle(
          color: textColor,
          fontWeight: FontWeight.w700,
          fontSize: 12,
        ),
      ),
    );
  }

  Widget _buildOfferRow(StockItem item) {
    if (_isNoticeItem(item) || item.offers.length <= 1) {
      return const SizedBox.shrink();
    }

    final previewOffers = item.offers.take(5).toList();

    return Column(
      children: [
        const SizedBox(height: 10),
        SizedBox(
          height: 36,
          child: ListView.separated(
            scrollDirection: Axis.horizontal,
            itemCount: previewOffers.length,
            separatorBuilder: (_, __) => const SizedBox(width: 6),
            itemBuilder: (context, index) {
              final offer = previewOffers[index];
              final isLowest = _isLowestOffer(item, offer);

              return InkWell(
                borderRadius: BorderRadius.circular(999),
                onTap: () => _showItemOffersSheet(item),
                child: Container(
                  padding:
                      const EdgeInsets.symmetric(horizontal: 10, vertical: 7),
                  decoration: BoxDecoration(
                    color: isLowest ? Colors.teal.shade50 : Colors.grey.shade100,
                    borderRadius: BorderRadius.circular(999),
                    border: Border.all(
                      color: isLowest ? Colors.teal.shade100 : Colors.grey.shade200,
                    ),
                  ),
                  child: Row(
                    mainAxisSize: MainAxisSize.min,
                    children: [
                      if (isLowest) ...[
                        Icon(
                          Icons.local_offer,
                          size: 13,
                          color: Colors.teal.shade800,
                        ),
                        const SizedBox(width: 4),
                      ],
                      Text(
                        '${offer.seller} · ${offer.price.isNotEmpty ? offer.price : '-'}',
                        style: TextStyle(
                          fontSize: 12,
                          fontWeight: FontWeight.w800,
                          color: isLowest ? Colors.teal.shade900 : Colors.black87,
                        ),
                      ),
                    ],
                  ),
                ),
              );
            },
          ),
        ),
      ],
    );
  }

  Future<void> _showPriceHistorySheet(StockItem item) async {
    final history = await PriceHistoryService.loadHistory(item.itemId);
    if (!mounted) return;

    showModalBottomSheet(
      context: context,
      shape: const RoundedRectangleBorder(
        borderRadius: BorderRadius.vertical(top: Radius.circular(20)),
      ),
      builder: (context) {
        final currentPrice = PriceHistoryService.priceToInt(
          item.minPrice.isNotEmpty ? item.minPrice : item.price,
        );
        final firstPrice = history.isNotEmpty ? history.first.price : currentPrice;
        final lastPrice = history.isNotEmpty ? history.last.price : currentPrice;
        final diff = firstPrice != null && lastPrice != null ? lastPrice - firstPrice : 0;

        return Padding(
          padding: const EdgeInsets.fromLTRB(16, 16, 16, 24),
          child: Column(
            mainAxisSize: MainAxisSize.min,
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [
              Center(
                child: Container(
                  width: 42,
                  height: 5,
                  decoration: BoxDecoration(
                    color: Colors.grey.shade300,
                    borderRadius: BorderRadius.circular(999),
                  ),
                ),
              ),
              const SizedBox(height: 16),
              const Text(
                '가격 변동 기록',
                style: TextStyle(fontSize: 18, fontWeight: FontWeight.bold),
              ),
              const SizedBox(height: 8),
              Text(
                _itemName(item),
                maxLines: 2,
                overflow: TextOverflow.ellipsis,
                style: TextStyle(color: Colors.grey.shade700),
              ),
              const SizedBox(height: 14),
              Container(
                width: double.infinity,
                padding: const EdgeInsets.all(12),
                decoration: BoxDecoration(
                  color: diff < 0
                      ? Colors.green.shade50
                      : diff > 0
                          ? Colors.red.shade50
                          : Colors.grey.shade100,
                  borderRadius: BorderRadius.circular(14),
                ),
                child: Text(
                  history.length < 2
                      ? '아직 가격 기록이 부족해요. 앱을 몇 번 새로고침하면 자동으로 쌓입니다.'
                      : diff == 0
                          ? '처음 기록 대비 가격 유지'
                          : diff < 0
                              ? '처음 기록 대비 ${PriceHistoryService.formatWon(diff.abs())} 하락'
                              : '처음 기록 대비 ${PriceHistoryService.formatWon(diff)} 상승',
                  style: const TextStyle(fontWeight: FontWeight.w800),
                ),
              ),
              const SizedBox(height: 12),
              if (history.isEmpty)
                Text(
                  '현재 최저가: ${item.minPrice.isNotEmpty ? item.minPrice : item.price}',
                  style: const TextStyle(fontWeight: FontWeight.w700),
                )
              else
                ...history.reversed.take(8).map((point) {
                  final date = point.checkedAt;
                  return Padding(
                    padding: const EdgeInsets.only(bottom: 8),
                    child: Row(
                      children: [
                        Expanded(
                          child: Text(
                            "${date.year}.${date.month.toString().padLeft(2, '0')}.${date.day.toString().padLeft(2, '0')} ${date.hour.toString().padLeft(2, '0')}:${date.minute.toString().padLeft(2, '0')}",
                            style: TextStyle(color: Colors.grey.shade700),
                          ),
                        ),
                        Text(
                          PriceHistoryService.formatWon(point.price),
                          style: const TextStyle(fontWeight: FontWeight.w800),
                        ),
                      ],
                    ),
                  );
                }),
            ],
          ),
        );
      },
    );
  }

  Future<void> _showUserReportSheet(StockItem item) async {
    final memoController = TextEditingController();

    await showModalBottomSheet(
      context: context,
      isScrollControlled: true,
      shape: const RoundedRectangleBorder(
        borderRadius: BorderRadius.vertical(top: Radius.circular(20)),
      ),
      builder: (context) {
        Future<void> submit(bool inStock) async {
          try {
            await UserStockReportService.submitReport(
              item: item,
              inStock: inStock,
              memo: memoController.text,
            );
            if (context.mounted) Navigator.pop(context);
            if (!mounted) return;
            ScaffoldMessenger.of(context).showSnackBar(
              const SnackBar(content: Text('재고 제보가 저장됐어요.')),
            );
          } catch (e) {
            if (!mounted) return;
            ScaffoldMessenger.of(context).showSnackBar(
              SnackBar(content: Text('제보 저장 실패: $e')),
            );
          }
        }

        return Padding(
          padding: EdgeInsets.fromLTRB(
            16,
            16,
            16,
            MediaQuery.of(context).viewInsets.bottom + 24,
          ),
          child: Column(
            mainAxisSize: MainAxisSize.min,
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [
              Center(
                child: Container(
                  width: 42,
                  height: 5,
                  decoration: BoxDecoration(
                    color: Colors.grey.shade300,
                    borderRadius: BorderRadius.circular(999),
                  ),
                ),
              ),
              const SizedBox(height: 16),
              const Text(
                '재고 제보',
                style: TextStyle(fontSize: 18, fontWeight: FontWeight.bold),
              ),
              const SizedBox(height: 8),
              Text(
                _itemName(item),
                maxLines: 2,
                overflow: TextOverflow.ellipsis,
                style: TextStyle(color: Colors.grey.shade700),
              ),
              const SizedBox(height: 12),
              TextField(
                controller: memoController,
                maxLines: 2,
                decoration: const InputDecoration(
                  labelText: '메모 선택 입력',
                  hintText: '예: 매장에 2개 봤어요 / 온라인 구매 가능',
                  border: OutlineInputBorder(),
                ),
              ),
              const SizedBox(height: 14),
              Row(
                children: [
                  Expanded(
                    child: OutlinedButton.icon(
                      onPressed: () => submit(false),
                      icon: const Icon(Icons.remove_shopping_cart),
                      label: const Text('없어요'),
                    ),
                  ),
                  const SizedBox(width: 10),
                  Expanded(
                    child: ElevatedButton.icon(
                      onPressed: () => submit(true),
                      icon: const Icon(Icons.inventory_2),
                      label: const Text('있어요'),
                    ),
                  ),
                ],
              ),
            ],
          ),
        );
      },
    );

    memoController.dispose();
  }

  Widget _buildProductCard(StockItem item) {
    final isFavorite = _isFavorite(item);
    final isNotice = _isNoticeItem(item);
    final name = _itemName(item);
    final seller = _sellerText(item);
    final price = item.price.isNotEmpty ? item.price : '-';
    final grade = _itemGrade(item);
    final onlySoldOut = _isOnlySoldOut(item);
    final primaryPrice = item.minPrice.isNotEmpty ? item.minPrice : price;

    return InkWell(
      borderRadius: BorderRadius.circular(16),
      onTap: () => _showItemOffersSheet(item),
      child: Container(
        decoration: BoxDecoration(
          color: onlySoldOut ? Colors.grey.shade50 : Colors.white,
          borderRadius: BorderRadius.circular(16),
          boxShadow: const [
            BoxShadow(
              color: Color(0x11000000),
              blurRadius: 10,
              offset: Offset(0, 3),
            ),
          ],
        ),
        padding: const EdgeInsets.all(14),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Wrap(
              children: [
                if (isNotice)
                  _buildBadge(
                    text: '건담 공지',
                    bgColor: Colors.orange.shade50,
                    textColor: Colors.orange.shade900,
                  ),
                if (item.isRestocked)
                  _buildBadge(
                    text: '재입고 감지',
                    bgColor: Colors.blue.shade50,
                    textColor: Colors.blue.shade900,
                  ),
                if (isFavorite)
                  _buildBadge(
                    text: '찜함',
                    bgColor: Colors.pink.shade50,
                    textColor: Colors.pink.shade900,
                  ),
                if (_isUnknownStatus(item))
                  _buildBadge(
                    text: '상태 확인중',
                    bgColor: Colors.grey.shade200,
                    textColor: Colors.grey.shade900,
                  ),
                if (!isNotice && grade.isNotEmpty)
                  _buildBadge(
                    text: grade,
                    bgColor: Colors.grey.shade100,
                    textColor: Colors.grey.shade900,
                  ),
                if (!isNotice && item.sellerCount > 1)
                  _buildBadge(
                    text: '최저가 비교',
                    bgColor: Colors.teal.shade50,
                    textColor: Colors.teal.shade900,
                  ),
                if (onlySoldOut)
                  _buildBadge(
                    text: '전 판매처 품절',
                    bgColor: Colors.red.shade50,
                    textColor: Colors.red.shade900,
                  ),
              ],
            ),
            const SizedBox(height: 6),
            Row(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Expanded(
                  child: Text(
                    name,
                    style: const TextStyle(
                      fontSize: 16,
                      fontWeight: FontWeight.w700,
                      height: 1.35,
                    ),
                  ),
                ),
                const SizedBox(width: 10),
                Container(
                  padding:
                      const EdgeInsets.symmetric(horizontal: 10, vertical: 6),
                  decoration: BoxDecoration(
                    color: _statusBgColor(item.status),
                    borderRadius: BorderRadius.circular(999),
                  ),
                  child: Text(
                    item.status,
                    style: TextStyle(
                      color: _statusColor(item.status),
                      fontSize: 12,
                      fontWeight: FontWeight.w700,
                    ),
                  ),
                ),
              ],
            ),
            const SizedBox(height: 10),
            Row(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Expanded(
                  child: Column(
                    crossAxisAlignment: CrossAxisAlignment.start,
                    children: [
                      Text(
                        seller,
                        style: const TextStyle(
                          fontSize: 13,
                          fontWeight: FontWeight.w600,
                          color: Colors.black87,
                        ),
                        overflow: TextOverflow.ellipsis,
                      ),
                      const SizedBox(height: 4),
                      if (!isNotice)
                        Text(
                          item.sellerCount > 1
                              ? '${item.sellerCount}곳 가격 비교 가능'
                              : '판매처 1곳',
                          style: TextStyle(
                            fontSize: 12,
                            color: Colors.grey.shade700,
                            fontWeight: FontWeight.w800,
                          ),
                        ),
                    ],
                  ),
                ),
                const SizedBox(width: 12),
                if (!isNotice)
                  Column(
                    crossAxisAlignment: CrossAxisAlignment.end,
                    children: [
                      Text(
                        item.sellerCount > 1 ? '최저가' : '가격',
                        style: TextStyle(
                          fontSize: 11,
                          color: Colors.grey.shade700,
                          fontWeight: FontWeight.w700,
                        ),
                      ),
                      const SizedBox(height: 2),
                      Text(
                        primaryPrice,
                        style: const TextStyle(
                          fontSize: 19,
                          fontWeight: FontWeight.w900,
                          color: Colors.red,
                        ),
                      ),
                      if (item.sellerCount > 1)
                        Text(
                          '${item.sellerCount}곳 비교',
                          style: TextStyle(
                            fontSize: 11,
                            color: Colors.teal.shade700,
                            fontWeight: FontWeight.w700,
                          ),
                        ),
                    ],
                  ),
              ],
            ),
            _buildOfferRow(item),
            const SizedBox(height: 14),
            Row(
              children: [
                Expanded(
                  child: ElevatedButton.icon(
                    onPressed: () async {
                      await _toggleFavorite(item);
                    },
                    icon: Icon(
                      isFavorite ? Icons.favorite : Icons.favorite_border,
                      size: 18,
                    ),
                    label: Text(isFavorite ? '찜해제' : '찜하기'),
                    style: ElevatedButton.styleFrom(
                      backgroundColor: Colors.white,
                      foregroundColor: isFavorite ? Colors.pink : Colors.black,
                      elevation: 0,
                      side: BorderSide(
                        color: isFavorite
                            ? Colors.pink.shade200
                            : Colors.grey.shade300,
                      ),
                      padding: const EdgeInsets.symmetric(vertical: 13),
                      shape: RoundedRectangleBorder(
                        borderRadius: BorderRadius.circular(12),
                      ),
                    ),
                  ),
                ),
                const SizedBox(width: 10),
                if (!isNotice) ...[
                  Expanded(
                    child: ElevatedButton.icon(
                      onPressed: () => _showUserReportSheet(item),
                      icon: const Icon(Icons.campaign_outlined, size: 18),
                      label: const Text('제보'),
                      style: ElevatedButton.styleFrom(
                        backgroundColor: Colors.white,
                        foregroundColor: Colors.blueGrey.shade800,
                        elevation: 0,
                        side: BorderSide(color: Colors.blueGrey.shade100),
                        padding: const EdgeInsets.symmetric(vertical: 13),
                        shape: RoundedRectangleBorder(
                          borderRadius: BorderRadius.circular(12),
                        ),
                      ),
                    ),
                  ),
                  const SizedBox(width: 10),
                  Expanded(
                    child: ElevatedButton.icon(
                      onPressed: () => _showPriceHistorySheet(item),
                      icon: const Icon(Icons.show_chart, size: 18),
                      label: const Text('가격'),
                      style: ElevatedButton.styleFrom(
                        backgroundColor: Colors.white,
                        foregroundColor: Colors.teal.shade800,
                        elevation: 0,
                        side: BorderSide(color: Colors.teal.shade100),
                        padding: const EdgeInsets.symmetric(vertical: 13),
                        shape: RoundedRectangleBorder(
                          borderRadius: BorderRadius.circular(12),
                        ),
                      ),
                    ),
                  ),
                  const SizedBox(width: 10),
                ],
                Expanded(
                  child: ElevatedButton.icon(
                    onPressed: item.sellerCount > 1
                        ? () => _showItemOffersSheet(item)
                        : item.resolvedUrl.isEmpty
                            ? null
                            : () async => _openExternalLink(item),
                    icon: Icon(
                      isNotice ? Icons.campaign : Icons.open_in_new,
                      size: 18,
                    ),
                    label: Text(isNotice ? '공지 보기' : item.sellerCount > 1 ? '판매처' : '보기'),
                    style: ElevatedButton.styleFrom(
                      backgroundColor: Colors.black,
                      foregroundColor: Colors.white,
                      disabledBackgroundColor: Colors.grey.shade400,
                      disabledForegroundColor: Colors.white,
                      elevation: 0,
                      padding: const EdgeInsets.symmetric(vertical: 13),
                      shape: RoundedRectangleBorder(
                        borderRadius: BorderRadius.circular(12),
                      ),
                    ),
                  ),
                ),
              ],
            ),
          ],
        ),
      ),
    );
  }

  Widget _buildProductList(List<StockItem> items) {
    final filteredItems = _applyFilters(items);

    return Column(
      children: [
        Padding(
          padding: const EdgeInsets.fromLTRB(12, 0, 12, 8),
          child: _buildSummaryCard(items),
        ),
        _buildRestockSection(items),
        Padding(
          padding: const EdgeInsets.fromLTRB(12, 0, 12, 4),
          child: Row(
            children: [
              Text(
                '표시 ${filteredItems.length}개',
                style: TextStyle(
                  color: Colors.grey.shade700,
                  fontWeight: FontWeight.w700,
                  fontSize: 12,
                ),
              ),
              const Spacer(),
              Text(
                '재입고 · 판매중 · 최저가순',
                style: TextStyle(
                  color: Colors.grey.shade600,
                  fontWeight: FontWeight.w700,
                  fontSize: 12,
                ),
              ),
            ],
          ),
        ),
        Expanded(
          child: filteredItems.isEmpty
              ? Center(child: Text(_emptyMessage()))
              : ListView.separated(
                  physics: const AlwaysScrollableScrollPhysics(),
                  padding: const EdgeInsets.fromLTRB(12, 8, 12, 16),
                  itemCount: filteredItems.length,
                  separatorBuilder: (_, __) => const SizedBox(height: 10),
                  itemBuilder: (context, index) {
                    return _buildProductCard(filteredItems[index]);
                  },
                ),
        ),
      ],
    );
  }

  String _gradeLabel(GradeFilterType type) {
    switch (type) {
      case GradeFilterType.all:
        return '전체';
      case GradeFilterType.pg:
        return 'PG';
      case GradeFilterType.mg:
        return 'MG';
      case GradeFilterType.rg:
        return 'RG';
      case GradeFilterType.hg:
        return 'HG';
      case GradeFilterType.sd:
        return 'SD';
    }
  }

  Widget _buildGradeExpandableSection() {
    return AnimatedCrossFade(
      firstChild: const SizedBox.shrink(),
      secondChild: Padding(
        padding: const EdgeInsets.only(top: 10),
        child: SingleChildScrollView(
          scrollDirection: Axis.horizontal,
          child: Row(
            children: GradeFilterType.values.map((type) {
              final minWidth = type == GradeFilterType.all ? 72.0 : 56.0;
              return Padding(
                padding: const EdgeInsets.only(right: 8),
                child: _buildTopActionChip(
                  label: _gradeLabel(type),
                  selected: _gradeFilter == type,
                  onTap: () {
                    setState(() {
                      _gradeFilter = type;
                    });
                  },
                  minWidth: minWidth,
                ),
              );
            }).toList(),
          ),
        ),
      ),
      crossFadeState: _isGradeExpanded
          ? CrossFadeState.showSecond
          : CrossFadeState.showFirst,
      duration: const Duration(milliseconds: 220),
    );
  }

  Widget _buildSearchSection() {
    final isNoticeMode = _filterType == HomeFilterType.notice;
    final isFavoriteMode = _filterType == HomeFilterType.favorites;

    return Padding(
      padding: const EdgeInsets.fromLTRB(12, 12, 12, 8),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          TextField(
            controller: _searchController,
            focusNode: _searchFocusNode,
            decoration: const InputDecoration(
              hintText: '한국 상품 / 판매처 검색',
              prefixIcon: Icon(Icons.search),
            ),
            onChanged: (value) {
              setState(() {
                _searchQuery = value;
              });
            },
          ),
          const SizedBox(height: 10),
          SingleChildScrollView(
            scrollDirection: Axis.horizontal,
            child: Row(
              children: [
                _buildTopActionChip(
                  label: '전체',
                  selected: _filterType == HomeFilterType.all,
                  onTap: _toggleAllAndGrades,
                  icon: _isGradeExpanded
                      ? Icons.keyboard_arrow_up
                      : Icons.keyboard_arrow_down,
                  minWidth: 84,
                ),
                const SizedBox(width: 8),
                _buildTopActionChip(
                  label: '재입고',
                  selected: _filterType == HomeFilterType.restock,
                  onTap: () {
                    setState(() {
                      _filterType = HomeFilterType.restock;
                    });
                  },
                  icon: Icons.local_fire_department_outlined,
                  minWidth: 96,
                ),
                const SizedBox(width: 8),
                _buildTopActionChip(
                  label: '상태 확인중',
                  selected: _filterType == HomeFilterType.unknownStatus,
                  onTap: () {
                    setState(() {
                      _filterType = HomeFilterType.unknownStatus;
                    });
                  },
                  icon: Icons.help_outline,
                  minWidth: 132,
                ),
                if (isNoticeMode) ...[
                  const SizedBox(width: 8),
                  _buildTopActionChip(
                    label: '공지 보는 중',
                    selected: true,
                    onTap: _toggleNoticeMode,
                    icon: Icons.campaign,
                    minWidth: 118,
                  ),
                ],
                if (isFavoriteMode) ...[
                  const SizedBox(width: 8),
                  _buildTopActionChip(
                    label: '찜한 상품 보는 중',
                    selected: true,
                    onTap: _toggleFavoriteMode,
                    icon: Icons.favorite,
                    minWidth: 140,
                  ),
                ],
              ],
            ),
          ),
          _buildGradeExpandableSection(),
        ],
      ),
    );
  }

  @override
  Widget build(BuildContext context) {
    final isNoticeMode = _filterType == HomeFilterType.notice;
    final isFavoriteMode = _filterType == HomeFilterType.favorites;

    return Scaffold(
      appBar: AppBar(
        title: Text(
          isNoticeMode
              ? '건담 공지'
              : isFavoriteMode
                  ? '찜한 상품'
                  : '건담 재고',
        ),
        centerTitle: true,
        actions: [
          IconButton(
            tooltip: isNoticeMode ? '전체 보기' : '공지 보기',
            onPressed: _toggleNoticeMode,
            icon: Icon(
              Icons.campaign,
              color: isNoticeMode ? Colors.orange : Colors.black,
            ),
          ),
          IconButton(
            tooltip: isFavoriteMode ? '전체 보기' : '찜한 상품 보기',
            onPressed: _toggleFavoriteMode,
            icon: Icon(
              isFavoriteMode ? Icons.favorite : Icons.favorite_border,
              color: isFavoriteMode ? Colors.pink : Colors.black,
            ),
          ),
        ],
      ),
      body: Column(
        children: [
          _buildSearchSection(),
          Expanded(
            child: StreamBuilder<List<StockItem>>(
              stream: StockApi.watchItems(),
              builder: (context, snapshot) {
                if (snapshot.hasError) {
                  return Center(
                    child: Text('상품 목록을 불러오는 중 오류가 발생했습니다.\n${snapshot.error}'),
                  );
                }

                if (!snapshot.hasData) {
                  return const Center(
                    child: CircularProgressIndicator(),
                  );
                }

                final items = snapshot.data ?? [];
                PriceHistoryService.recordSnapshots(items);

                return RefreshIndicator(
                  onRefresh: _refresh,
                  child: _buildProductList(items),
                );
              },
            ),
          ),
        ],
      ),
    );
  }
}