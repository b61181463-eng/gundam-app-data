import 'package:cloud_firestore/cloud_firestore.dart';

String _formatInt(int value) {
  final text = value.toString();
  final reg = RegExp(r'\B(?=(\d{3})+(?!\d))');
  return text.replaceAllMapped(reg, (_) => ',');
}

int? _priceToInt(dynamic value) {
  if (value == null) return null;
  if (value is int) return value;
  if (value is num) return value.toInt();
  final digits = value.toString().replaceAll(RegExp(r'[^0-9]'), '');
  if (digits.isEmpty) return null;
  return int.tryParse(digits);
}

String _priceText(dynamic raw, int? priceInt) {
  if (priceInt != null) return '${_formatInt(priceInt)}원';
  final text = (raw ?? '').toString().trim();
  if (text.isEmpty) return '가격 확인중';
  final parsed = _priceToInt(text);
  if (parsed != null) return '${_formatInt(parsed)}원';
  return text;
}

String _normalizeStatus(String raw) {
  final t = raw.toLowerCase().trim();
  if (t.contains('판매') || t.contains('구매가능') || t.contains('available') || t.contains('in stock')) {
    return '판매중';
  }
  if (t.contains('예약') || t.contains('pre-order') || t.contains('preorder')) {
    return '예약중';
  }
  if (t.contains('입고예정') || t.contains('coming soon')) {
    return '입고예정';
  }
  if (t.contains('품절') || t.contains('sold out') || t.contains('out of stock')) {
    return '품절';
  }
  if (t.contains('공지') || t.contains('notice')) return '공지';
  return '상태 확인중';
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
    case '공지':
      return 4;
    default:
      return 5;
  }
}

String _prettySeller(String mallName, String site) {
  final raw = ('$mallName $site').toLowerCase();
  if (raw.contains('gundamshop') || raw.contains('건담샵')) return '건담샵';
  if (raw.contains('modelsale') || raw.contains('모델세일')) return '모델세일';
  if (raw.contains('hobbyfactory') || raw.contains('하비팩토리')) return '하비팩토리';
  if (raw.contains('joyhobby') || raw.contains('조이하비')) return '조이하비';
  if (raw.contains('zeonshop') || raw.contains('지온샵')) return '지온샵';
  if (raw.contains('plamodelmania') || raw.contains('프라모델매니아')) return '프라모델매니아';
  if (raw.contains('gundamboom') || raw.contains('건담붐')) return '건담붐';
  if (raw.contains('gundamall') || raw.contains('건담몰')) return '건담몰';
  if (raw.contains('bnkr') || raw.contains('반다이남코코리아몰')) return '반다이남코코리아몰';
  if (raw.contains('gundamcity') || raw.contains('건담시티')) return '건담시티';
  if (raw.contains('gundambase') || raw.contains('건담베이스')) return '건담베이스';
  return mallName.trim().isNotEmpty ? mallName.trim() : site.trim();
}

String _bestUrl(Map<String, dynamic> data) {
  final candidates = [
    data['productUrl'],
    data['product_url'],
    data['detailUrl'],
    data['detail_url'],
    data['url'],
    data['link'],
  ];
  for (final c in candidates) {
    final text = (c ?? '').toString().trim();
    if (text.startsWith('http')) return text;
  }
  return '';
}

String _extractGrade(String text) {
  final upper = text.toUpperCase();
  if (RegExp(r'(^|[^A-Z0-9])MG\s*EX([^A-Z0-9]|$)').hasMatch(upper)) return 'MGEX';
  if (RegExp(r'(^|[^A-Z0-9])MG\s*SD([^A-Z0-9]|$)').hasMatch(upper)) return 'MGSD';
  if (upper.contains('FULL MECHANICS')) return 'FULL MECHANICS';
  if (upper.contains('RE/100')) return 'RE/100';
  if (RegExp(r'(^|[^A-Z0-9])PG([^A-Z0-9]|$)').hasMatch(upper)) return 'PG';
  if (RegExp(r'(^|[^A-Z0-9])RG[A-Z]*([^A-Z0-9]|$)').hasMatch(upper)) return 'RG';
  if (RegExp(r'(^|[^A-Z0-9])MG([^A-Z0-9]|$)').hasMatch(upper)) return 'MG';
  if (RegExp(r'(^|[^A-Z0-9])HG[A-Z]*([^A-Z0-9]|$)').hasMatch(upper)) return 'HG';
  if (RegExp(r'(^|[^A-Z0-9])EG([^A-Z0-9]|$)').hasMatch(upper)) return 'EG';
  if (RegExp(r'(^|[^A-Z0-9])SD(?:[- ]?EX(?:[- ]?STANDARD)?)?([^A-Z0-9]|$)').hasMatch(upper)) return 'SD';
  return 'UNKNOWN';
}

String _canonicalKey(String raw) {
  var text = raw.toLowerCase().trim();
  text = text
      .replaceAll('&nbsp;', ' ')
      .replaceAll('&amp;', '&')
      .replaceAll('ⅱ', 'ii')
      .replaceAll('ⅲ', 'iii')
      .replaceAll('ver.ka', 'verka')
      .replaceAll('ver ka', 'verka')
      .replaceAll('version ka', 'verka')
      .replaceAll('master grade', 'mg')
      .replaceAll('high grade', 'hg')
      .replaceAll('real grade', 'rg')
      .replaceAll('perfect grade', 'pg')
      .replaceAll('full mechanics', 'fullmechanics')
      .replaceAll('re/100', 're100');

  text = text.replaceAll(RegExp(r'1\s*/\s*100'), '1/100');
  text = text.replaceAll(RegExp(r'1\s*/\s*144'), '1/144');
  text = text.replaceAll(RegExp(r'1\s*/\s*60'), '1/60');

  const removeWords = [
    '반다이남코코리아몰', '건담샵', '건담베이스', '하비팩토리', '건담시티', '모델세일',
    '조이하비', '건담붐', '프라모델매니아', '지온샵', '건담몰',
    '예약판매', '예약중', '입고예정', '판매중', '품절', 'md추천', '강력추천',
    '프라모델', '건프라', 'plastic model', 'model kit', 'mobile suit',
  ];
  for (final word in removeWords) {
    text = text.replaceAll(word.toLowerCase(), ' ');
  }

  text = text.replaceAll(RegExp(r'\[[0-9]{1,4}\]'), ' ');
  text = text.replaceAll(RegExp(r'\s+\d{6,}$'), ' ');
  text = text.replaceAll(RegExp(r'[^a-z0-9가-힣/\- ]+'), ' ');
  text = text.replaceAll(RegExp(r'\s+'), ' ').trim();

  final tokens = text
      .split(' ')
      .map((e) => e.trim())
      .where((e) => e.isNotEmpty)
      .where((e) => e.length >= 2)
      .toList();

  tokens.sort();
  return tokens.join('_');
}

bool _badPlaceholderName(String name) {
  final t = name.trim().toLowerCase();
  if (t.isEmpty) return true;
  if (t == '상품상세 | 반다이남코코리아몰') return true;
  if (t.contains('상품상세') && t.contains('반다이남코코리아몰')) return true;
  if (t == '상품명' || t == 'bnkrmall') return true;
  return false;
}

class ProductOffer {
  final String seller;
  final String site;
  final String priceText;
  final int? priceInt;
  final String status;
  final String url;
  final String itemId;
  final DateTime? updatedAt;

  const ProductOffer({
    required this.seller,
    required this.site,
    required this.priceText,
    required this.priceInt,
    required this.status,
    required this.url,
    required this.itemId,
    required this.updatedAt,
  });
}

class GroupedProduct {
  final String key;
  final String name;
  final String grade;
  final List<ProductOffer> offers;
  final ProductOffer bestOffer;
  final DateTime? updatedAt;

  const GroupedProduct({
    required this.key,
    required this.name,
    required this.grade,
    required this.offers,
    required this.bestOffer,
    required this.updatedAt,
  });

  int get sellerCount => offers.map((e) => e.seller).toSet().length;
  String get minPriceText => bestOffer.priceText;
  int? get minPriceInt => bestOffer.priceInt;
  String get status => bestOffer.status;
  String get sellerSummary => sellerCount > 1 ? '${bestOffer.seller} 외 ${sellerCount - 1}곳' : bestOffer.seller;

  bool get hasAvailable => offers.any((e) => e.status == '판매중');
  bool get hasMultipleSellers => sellerCount > 1;

  String get searchText {
    return [
      name,
      grade,
      key,
      for (final offer in offers) offer.seller,
      for (final offer in offers) offer.status,
    ].join(' ').toLowerCase();
  }
}

class GroupedProductService {
  static final FirebaseFirestore _db = FirebaseFirestore.instance;
  static const String collectionName = 'aggregated_items';

  static Stream<List<GroupedProduct>> watchGroupedProducts() {
    return _db.collection(collectionName).snapshots().map((snapshot) {
      final rawGroups = <String, List<_RawProduct>>{};

      for (final doc in snapshot.docs) {
        final data = doc.data();
        final rawName = (data['displayName'] ?? data['name'] ?? data['title'] ?? '').toString().trim();
        final rawTitle = (data['displayName'] ?? data['title'] ?? '').toString().trim();
        final name = rawName.isNotEmpty ? rawName : rawTitle;
        if (_badPlaceholderName(name)) continue;

        final status = _normalizeStatus('${data['status'] ?? ''} ${data['stockText'] ?? ''}');
        if (status == '공지') continue;

        final mallName = (data['mallName'] ?? data['mall_name'] ?? '').toString();
        final site = (data['site'] ?? '').toString();
        final seller = _prettySeller(mallName, site);
        final priceInt = _priceToInt(data['priceInt']) ?? _priceToInt(data['price']);
        final priceText = _priceText(data['price'], priceInt);
        final url = _bestUrl(data);
        final grade = (data['grade'] ?? '').toString().trim().isNotEmpty
            ? data['grade'].toString().trim()
            : _extractGrade('$name $rawTitle');

        final productKey = (data['productKey'] ?? data['normalizedName'] ?? '').toString().trim();
        String productGrade = grade;
        String canonical = '';
        if (productKey.contains('|')) {
          final parts = productKey.split('|');
          if (parts.isNotEmpty && parts.first.trim().isNotEmpty) productGrade = parts.first.trim();
          canonical = parts.length > 1 ? parts.sublist(1).join('|').trim() : '';
          canonical = _canonicalKey(canonical);
        } else {
          final canonicalBase = productKey.isNotEmpty ? productKey : name;
          canonical = _canonicalKey(canonicalBase);
        }

        // 너무 짧은 키는 서로 다른 상품을 위험하게 합칠 수 있어서 문서 단위로 유지.
        final groupKey = canonical.length >= 6
            ? '${productGrade.toUpperCase()}_$canonical'
            : '${site}_${doc.id}';

        final updatedAtRaw = data['updatedAt'];
        final updatedAt = updatedAtRaw is Timestamp ? updatedAtRaw.toDate() : null;

        rawGroups.putIfAbsent(groupKey, () => []).add(
              _RawProduct(
                key: groupKey,
                name: name,
                grade: grade,
                offer: ProductOffer(
                  seller: seller,
                  site: site,
                  priceText: priceText,
                  priceInt: priceInt,
                  status: status,
                  url: url,
                  itemId: doc.id,
                  updatedAt: updatedAt,
                ),
                updatedAt: updatedAt,
              ),
            );
      }

      final products = <GroupedProduct>[];

      for (final entry in rawGroups.entries) {
        final raws = entry.value;
        if (raws.isEmpty) continue;

        final offersBySeller = <String, ProductOffer>{};
        for (final raw in raws) {
          final current = offersBySeller[raw.offer.seller];
          if (current == null || _compareOffer(raw.offer, current) < 0) {
            offersBySeller[raw.offer.seller] = raw.offer;
          }
        }

        final offers = offersBySeller.values.toList()..sort(_compareOffer);
        if (offers.isEmpty) continue;

        raws.sort((a, b) => _compareOffer(a.offer, b.offer));
        final bestRaw = raws.first;
        final bestOffer = offers.first;
        final latest = raws
            .map((e) => e.updatedAt)
            .whereType<DateTime>()
            .fold<DateTime?>(null, (prev, next) {
          if (prev == null) return next;
          return next.isAfter(prev) ? next : prev;
        });

        products.add(
          GroupedProduct(
            key: entry.key,
            name: bestRaw.name,
            grade: bestRaw.grade,
            offers: offers,
            bestOffer: bestOffer,
            updatedAt: latest,
          ),
        );
      }

      products.sort((a, b) {
        final statusCompare = _statusRank(a.status).compareTo(_statusRank(b.status));
        if (statusCompare != 0) return statusCompare;
        final ap = a.minPriceInt;
        final bp = b.minPriceInt;
        if (ap != null && bp != null && ap != bp) return ap.compareTo(bp);
        if (ap == null && bp != null) return 1;
        if (ap != null && bp == null) return -1;
        return a.name.compareTo(b.name);
      });

      return products;
    });
  }

  static int _compareOffer(ProductOffer a, ProductOffer b) {
    final statusCompare = _statusRank(a.status).compareTo(_statusRank(b.status));
    if (statusCompare != 0) return statusCompare;

    final ap = a.priceInt;
    final bp = b.priceInt;
    if (ap != null && bp != null && ap != bp) return ap.compareTo(bp);
    if (ap == null && bp != null) return 1;
    if (ap != null && bp == null) return -1;

    return a.seller.compareTo(b.seller);
  }
}

class _RawProduct {
  final String key;
  final String name;
  final String grade;
  final ProductOffer offer;
  final DateTime? updatedAt;

  const _RawProduct({
    required this.key,
    required this.name,
    required this.grade,
    required this.offer,
    required this.updatedAt,
  });
}
