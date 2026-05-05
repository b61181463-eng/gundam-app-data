import 'dart:convert';

import 'package:cloud_firestore/cloud_firestore.dart';
import 'package:shared_preferences/shared_preferences.dart';

String _formatInt(int value) {
  final text = value.toString();
  final reg = RegExp(r'\B(?=(\d{3})+(?!\d))');
  return text.replaceAllMapped(reg, (_) => ',');
}

int? _priceToInt(String price) {
  final digits = price.replaceAll(RegExp(r'[^0-9]'), '');
  if (digits.isEmpty) return null;
  return int.tryParse(digits);
}

String _normalizePriceText(String raw) {
  final digits = raw.replaceAll(RegExp(r'[^0-9]'), '');
  if (digits.isEmpty) return raw.trim();
  final value = int.tryParse(digits);
  if (value == null) return raw.trim();
  return '${_formatInt(value)}원';
}

bool _containsAny(String text, List<String> keywords) {
  for (final keyword in keywords) {
    if (text.contains(keyword)) return true;
  }
  return false;
}

String _cleanDisplayNameRaw(String raw) {
  var text = raw.trim();
  if (text.isEmpty) return '';

  text = text
      .replaceAll('&nbsp;', ' ')
      .replaceAll('&#39;', "'")
      .replaceAll('&amp;', '&');

  // 🔥 깨진 문자 제거 (핵심)
  text = text.replaceAll(
    RegExp(r'[^\x00-\x7F가-힣a-zA-Z0-9/\[\]\(\)\- ]'),
    '',
  );

  // 🔥 긴 숫자 제거
  text = text.replaceAll(RegExp(r'\s+\d{6,}$'), '');

  // 🔥 공백 정리
  text = text.replaceAll(RegExp(r'\s+'), ' ').trim();

  // 특수 깨짐 패턴 제거
  text = text.replaceAll(RegExp(r'[�]+'), '');

  return text;
}

String _cleanForFilter(String raw) {
  var text = raw.trim().toUpperCase();
  text = text.replaceAll(RegExp(r'\s+'), ' ');
  text = text.replaceAll(RegExp(r'\s+\d{6,}$'), '');
  return text;
}

String _normalizeSiteKey(String raw) {
  return raw.trim().toLowerCase();
}

String _siteSpecificCanonicalName(StockItem item) {
  final source = [
    item.mallName,
    item.site,
    item.sourcePage,
  ].join(' ').toLowerCase();

  String text = item.name.trim().isNotEmpty
      ? item.name.trim()
      : item.title.trim();

  text = _canonicalName(text);

  // 공통 기본 정리
  text = text
      .replaceAll(RegExp(r'\bver\.?\s*ka\b', caseSensitive: false), ' verka ')
      .replaceAll(RegExp(r'\bfull\s*mechanics\b', caseSensitive: false), ' fullmechanics ')
      .replaceAll(RegExp(r'\bre\s*/?\s*100\b', caseSensitive: false), ' re100 ')
      .replaceAll(RegExp(r'\s+'), ' ')
      .trim();

  // 건담샵 전용
  if (source.contains('gundamshop') || source.contains('건담샵')) {
    text = text
        .replaceAll(RegExp(r'\[[^\]]+\]'), ' ')
        .replaceAll(RegExp(r'\b예약판매\b'), ' ')
        .replaceAll(RegExp(r'\b특가\b'), ' ')
        .replaceAll(RegExp(r'\s+\d{6,}$'), ' ')
        .replaceAll(RegExp(r'\s+'), ' ')
        .trim();
  }

  // 건담시티 전용
  if (source.contains('gundamcity') || source.contains('건담시티')) {
    text = text
        .replaceAll(RegExp(r'^\s*상품명\s*[:：]?\s*'), ' ')
        .replaceAll(RegExp(r'\b반다이\b'), ' ')
        .replaceAll(RegExp(r'\b남코\b'), ' ')
        .replaceAll(RegExp(r'\s+'), ' ')
        .trim();
  }

  // 하비팩토리 전용
  if (source.contains('hobbyfactory') || source.contains('하비팩토리')) {
    text = text
        .replaceAll(RegExp(r'\b해외판\b'), ' ')
        .replaceAll(RegExp(r'\b국내판\b'), ' ')
        .replaceAll(RegExp(r'\b재판\b'), ' ')
        .replaceAll(RegExp(r'\s+'), ' ')
        .trim();
  }

  // 모델세일 전용
  if (source.contains('modelsale') || source.contains('모델세일')) {
    text = text
        .replaceAll(RegExp(r'\b프라모델\b'), ' ')
        .replaceAll(RegExp(r'\bplastic\s*model\b', caseSensitive: false), ' ')
        .replaceAll(RegExp(r'\s+'), ' ')
        .trim();
  }

  return text;
}

String _buildSiteAwareGroupName(StockItem item) {
  final base = _siteSpecificCanonicalName(item);
  return _normalizeGroupName(base);
}

String _canonicalName(String raw) {
  var text = raw.trim();

  text = text.replaceAll(
    RegExp(r'^(?:\[[^\]]+\]|\([^)]+\)|【[^】]+】)\s*'),
    '',
  );

  text = text.replaceAll(
    RegExp(r'^(?:반다이남코코리아몰|건담샵|건담베이스|하비팩토리|건담시티|모델세일)\s*'),
    '',
  );

  text = text.replaceAll(RegExp(r'\b(?:예약판매|예약중|입고예정|판매중|품절)\b'), '');
  text = text.replaceAll(RegExp(r'\s+\d{6,}$'), '');
  text = text.replaceAll(RegExp(r'\s+'), ' ').trim();

  return text;
}

String _normalizeGroupName(String raw) {
  var text = raw.toLowerCase().trim();

  text = text
      .replaceAll('&nbsp;', ' ')
      .replaceAll('&#39;', "'")
      .replaceAll('&amp;', '&')
      .replaceAll('ⅱ', 'ii')
      .replaceAll('ⅲ', 'iii')
      .replaceAll('–', '-')
      .replaceAll('—', '-')
      .replaceAll('ver.ka', 'verka')
      .replaceAll('ver ka', 'verka')
      .replaceAll('version ka', 'verka')
      .replaceAll('master grade', 'mg')
      .replaceAll('mastergrade', 'mg')
      .replaceAll('high grade', 'hg')
      .replaceAll('highgrade', 'hg')
      .replaceAll('real grade', 'rg')
      .replaceAll('realgrade', 'rg')
      .replaceAll('perfect grade', 'pg')
      .replaceAll('perfectgrade', 'pg')
      .replaceAll('full mechanics', 'fullmechanics')
      .replaceAll('re/100', 're100');

  text = text.replaceAll(RegExp(r'1\s*/\s*100'), '1/100');
  text = text.replaceAll(RegExp(r'1\s*/\s*144'), '1/144');
  text = text.replaceAll(RegExp(r'1\s*/\s*60'), '1/60');

  const removes = [
    '반다이남코코리아몰',
    '건담샵',
    '건담베이스',
    '하비팩토리',
    '건담시티',
    '모델세일',
    '프라모델',
    '건프라',
    '예약판매',
    '예약중',
    '입고예정',
    '판매중',
    '품절',
    '건담',
    'gundam',
    'model kit',
    'modelkit',
    'plastic model',
    'plasticmodel',
    'the',
    'mobile suit',
  ];

  for (final remove in removes) {
    text = text.replaceAll(remove.toLowerCase(), ' ');
  }

  text = text.replaceAll(RegExp(r'\s+\d{6,}$'), ' ');
  text = text.replaceAll(RegExp(r'[\[\]\(\)【】]'), ' ');
  text = text.replaceAll(RegExp(r'[^a-z0-9가-힣/\- ]+'), ' ');

  final tokens = text
      .split(RegExp(r'\s+'))
      .where((e) => e.trim().isNotEmpty)
      .toList();

  const stopWords = {
    'mg',
    'hg',
    'rg',
    'pg',
    'sd',
    'mgex',
    'mgsd',
    're100',
    'fullmechanics',
    '1/100',
    '1/144',
    '1/60',
  };

  final filtered = <String>[];
  String grade = '';
  String scale = '';

  for (final token in tokens) {
    if (token == 'mgex' ||
        token == 'mgsd' ||
        token == 'mg' ||
        token == 'hg' ||
        token == 'rg' ||
        token == 'pg' ||
        token == 'sd' ||
        token == 're100' ||
        token == 'fullmechanics') {
      if (grade.isEmpty) grade = token;
      continue;
    }

    if (token == '1/100' || token == '1/144' || token == '1/60') {
      if (scale.isEmpty) scale = token;
      continue;
    }

    if (!stopWords.contains(token)) {
      filtered.add(token);
    }
  }

  filtered.sort();

  final result = [
    if (grade.isNotEmpty) grade,
    if (scale.isNotEmpty) scale,
    ...filtered,
  ].join('_');

  return result.trim();
}

String _extractGrade(String text) {
  final upper = text.toUpperCase();
  if (upper.contains('MGEX')) return 'MGEX';
  if (upper.contains('MGSD')) return 'MGSD';
  if (RegExp(r'(^|\s|\[|\()PG($|\s|\]|\))').hasMatch(upper)) return 'PG';
  if (RegExp(r'(^|\s|\[|\()MG($|\s|\]|\))').hasMatch(upper)) return 'MG';
  if (RegExp(r'(^|\s|\[|\()RG($|\s|\]|\))').hasMatch(upper)) return 'RG';
  if (RegExp(r'(^|\s|\[|\()HG($|\s|\]|\))').hasMatch(upper)) return 'HG';
  if (RegExp(r'(^|\s|\[|\()SD($|\s|\]|\))').hasMatch(upper)) return 'SD';
  return 'UNKNOWN';
}

int _offerStatusRank(String status) {
  if (status == '판매중') return 0;
  if (status == '예약중') return 1;
  if (status == '입고예정') return 2;
  if (status == '품절') return 3;
  if (status == '공지') return 4;
  return 5;
}

int _statusScore(String status) {
  if (status == '공지') return 5;
  if (status == '판매중') return 4;
  if (status == '예약중') return 3;
  if (status == '입고예정') return 2;
  if (status == '품절') return 1;
  return 0;
}

class StockOffer {
  final String seller;
  final String price;
  final String status;
  final String resolvedUrl;

  const StockOffer({
    required this.seller,
    required this.price,
    required this.status,
    required this.resolvedUrl,
  });
}

class StockItem {
  final String itemId;
  final String sourcePage;
  final String name;
  final String title;
  final String price;
  final String site;
  final String status;
  final String stockText;
  final String mallName;
  final String url;
  final String productUrl;
  final String resolvedUrl;
  final bool isRestocked;
  final List<StockOffer> offers;
  final int sellerCount;
  final String minPrice;
  final String noticeDate;

  const StockItem({
    required this.itemId,
    required this.sourcePage,
    required this.name,
    required this.title,
    required this.price,
    required this.site,
    required this.status,
    required this.stockText,
    required this.mallName,
    required this.url,
    required this.productUrl,
    required this.resolvedUrl,
    required this.isRestocked,
    required this.offers,
    required this.sellerCount,
    required this.minPrice,
    required this.noticeDate,
  });

  StockItem copyWith({
    String? itemId,
    String? sourcePage,
    String? name,
    String? title,
    String? price,
    String? site,
    String? status,
    String? stockText,
    String? mallName,
    String? url,
    String? productUrl,
    String? resolvedUrl,
    bool? isRestocked,
    List<StockOffer>? offers,
    int? sellerCount,
    String? minPrice,
    String? noticeDate,
  }) {
    return StockItem(
      itemId: itemId ?? this.itemId,
      sourcePage: sourcePage ?? this.sourcePage,
      name: name ?? this.name,
      title: title ?? this.title,
      price: price ?? this.price,
      site: site ?? this.site,
      status: status ?? this.status,
      stockText: stockText ?? this.stockText,
      mallName: mallName ?? this.mallName,
      url: url ?? this.url,
      productUrl: productUrl ?? this.productUrl,
      resolvedUrl: resolvedUrl ?? this.resolvedUrl,
      isRestocked: isRestocked ?? this.isRestocked,
      offers: offers ?? this.offers,
      sellerCount: sellerCount ?? this.sellerCount,
      minPrice: minPrice ?? this.minPrice,
      noticeDate: noticeDate ?? this.noticeDate,
    );
  }

  factory StockItem.fromFirestore(
    QueryDocumentSnapshot<Map<String, dynamic>> doc,
  ) {
    final data = doc.data();

    final sourcePage = _readString(data, ['sourcePage', 'source_page']);
    final rawName = _readString(data, ['name', 'item_name']);
    final rawTitle = _readString(data, ['title']);
    final price = _normalizePriceText(_readString(data, ['price']));
    final site = _readString(data, ['site', 'source']);
    final rawStatus = _readString(data, ['status']);
    final stockText = _readString(data, ['stockText', 'stock_text']);
    final mallName = _readString(data, ['mallName', 'mall_name', 'seller']);
    final url = _readString(data, ['url']);
    final productUrl = _readString(data, ['productUrl', 'product_url']);
    final noticeDate = _readString(data, [
      'noticeDate',
      'notice_date',
      'date',
      'createdAt',
      'created_at',
      'postedAt',
      'posted_at',
    ]);

    final cleanedName = _cleanDisplayNameRaw(rawName);
    final cleanedTitle = _cleanDisplayNameRaw(rawTitle);

    final resolvedUrl = _resolveBestUrl(
      sourcePage: sourcePage,
      site: site,
      mallName: mallName,
      productUrl: productUrl,
      url: url,
      detailUrl: _readString(data, ['detailUrl', 'detail_url']),
      link: _readString(data, ['link']),
      href: _readString(data, ['href']),
      productLink: _readString(data, ['productLink', 'product_link']),
      itemUrl: _readString(data, ['itemUrl', 'item_url']),
    );

    final normalizedStatus = _normalizeDisplayStatus(
      rawStatus: rawStatus,
      rawStockText: stockText,
      sourcePage: sourcePage,
      site: site,
      mallName: mallName,
      resolvedUrl: resolvedUrl,
      name: cleanedName,
      title: cleanedTitle,
    );

    return StockItem(
      itemId: doc.id,
      sourcePage: sourcePage,
      name: cleanedName,
      title: cleanedTitle,
      price: price,
      site: site,
      status: normalizedStatus,
      stockText: stockText,
      mallName: mallName,
      url: url,
      productUrl: productUrl,
      resolvedUrl: resolvedUrl,
      isRestocked: false,
      offers: [
        StockOffer(
          seller: mallName.isNotEmpty ? mallName : site,
          price: price,
          status: normalizedStatus,
          resolvedUrl: resolvedUrl,
        ),
      ],
      sellerCount: 1,
      minPrice: price,
      noticeDate: noticeDate,
    );
  }

  static String _readString(Map<String, dynamic> data, List<String> keys) {
    for (final key in keys) {
      final value = data[key];
      if (value == null) continue;

      if (value is Timestamp) {
        final dt = value.toDate();
        return '${dt.year.toString().padLeft(4, '0')}-${dt.month.toString().padLeft(2, '0')}-${dt.day.toString().padLeft(2, '0')}';
      }

      final text = value.toString().trim();
      if (text.isNotEmpty) return text;
    }
    return '';
  }

  static String _normalizeDisplayStatus({
    required String rawStatus,
    required String rawStockText,
    required String sourcePage,
    required String site,
    required String mallName,
    required String resolvedUrl,
    required String name,
    required String title,
  }) {
    final merged = [
      rawStatus,
      rawStockText,
      sourcePage,
      site,
      mallName,
      resolvedUrl,
      name,
      title,
    ].join(' ').toLowerCase();

    if (_containsAny(merged, [
      '판매중',
      '구매가능',
      '바로구매',
      '장바구니',
      'available',
      'in stock',
    ])) {
      return '판매중';
    }

    if (_containsAny(merged, [
      '품절',
      '일시품절',
      'sold out',
      'out of stock',
    ])) {
      return '품절';
    }

    if (_containsAny(merged, [
      '예약',
      '예약중',
      '예약판매',
      'preorder',
      'pre-order',
      'pre order',
    ])) {
      return '예약중';
    }

    if (_containsAny(merged, [
      '공지',
      'notice',
      '공지사항',
      '입고 안내',
    ])) {
      return '공지';
    }

    if (_containsAny(merged, [
      '입고예정',
      'coming soon',
    ])) {
      return '입고예정';
    }

    return '상태 확인중';
  }

  static String _resolveBestUrl({
    required String sourcePage,
    required String site,
    required String mallName,
    required String productUrl,
    required String url,
    required String detailUrl,
    required String link,
    required String href,
    required String productLink,
    required String itemUrl,
  }) {
    final candidates = <String>[
      productUrl,
      detailUrl,
      productLink,
      itemUrl,
      url,
      link,
      href,
    ].where((e) => e.trim().isNotEmpty).toList();

    final fixed = candidates
        .map((e) => _fixUrl(e, sourcePage, site, mallName))
        .where((e) => e.isNotEmpty)
        .toList();

    for (final c in fixed) {
      if (_looksLikeStrongDetailUrl(c)) return c;
    }

    for (final c in fixed) {
      if (_looksLikeDetailUrl(c)) return c;
    }

    return fixed.isNotEmpty ? fixed.first : '';
  }

  static bool _looksLikeStrongDetailUrl(String link) {
    final lower = link.toLowerCase();
    return lower.contains('/goods/view') ||
        lower.contains('/goods/detail') ||
        lower.contains('/product/detail') ||
        lower.contains('/product/') ||
        lower.contains('/item/') ||
        lower.contains('/shop/shopdetail') ||
        lower.contains('/m/product') ||
        lower.contains('/detail.php') ||
        lower.contains('product_no=') ||
        lower.contains('goodsno=') ||
        lower.contains('branduid=') ||
        lower.contains('gno=') ||
        lower.contains('itemno=') ||
        lower.contains('no=');
  }

  static bool _looksLikeDetailUrl(String link) {
    final lower = link.toLowerCase();
    if (lower.isEmpty) return false;

    const detailHints = [
      '/goods/view',
      '/goods/detail',
      '/product/detail',
      '/product/',
      '/item/',
      '/shop/shopdetail',
      '/m/product',
      '/detail.php',
      'goodsno=',
      'product_no=',
      'branduid=',
      'gno=',
      'itemno=',
      'view?',
      'no=',
      '/board/view',
      '/notice/view',
    ];

    for (final hint in detailHints) {
      if (lower.contains(hint)) return true;
    }

    const listingHints = [
      '/category',
      '/reserve.html',
      '/list',
      '/search',
      '/main',
      'cate=',
      'sort=',
      'page=',
      'mcode=',
      'scode=',
      'xcode=',
      'type=',
      'shopbrand',
      'product_list',
      'cat_detail',
    ];

    for (final hint in listingHints) {
      if (lower.contains(hint)) return false;
    }

    return true;
  }

  static String _fixUrl(
    String raw,
    String sourcePage,
    String site,
    String mallName,
  ) {
    final link = raw.trim();
    if (link.isEmpty) return '';

    if (link.startsWith('//')) return 'https:$link';
    if (link.startsWith('http://') || link.startsWith('https://')) return link;

    final base = _guessBaseUrl(sourcePage, site, mallName);
    if (base.isEmpty) return link;

    if (link.startsWith('/')) return '$base$link';
    return '$base/$link';
  }

  static String _guessBaseUrl(
    String sourcePage,
    String site,
    String mallName,
  ) {
    final combined = '$sourcePage $site $mallName'.toLowerCase();

    if (combined.contains('gundamshop') || combined.contains('건담샵')) {
      return 'https://www.gundamshop.co.kr';
    }
    if (combined.contains('gundambase') ||
        combined.contains('건담베이스') ||
        combined.contains('the gundam base')) {
      return 'https://www.thegundambase.co.kr';
    }
    if (combined.contains('bnkrmall') ||
        combined.contains('반케이알몰') ||
        combined.contains('반다이남코코리아몰')) {
      return 'https://bnkrmall.co.kr';
    }
    if (combined.contains('hobbyfactory') || combined.contains('하비팩토리')) {
      return 'https://www.hobbyfactory.kr';
    }
    if (combined.contains('gundamcity') || combined.contains('건담시티')) {
      return 'https://www.gundamcity.co.kr';
    }
    if (combined.contains('modelsale') || combined.contains('모델세일')) {
      return 'https://www.modelsale.co.kr';
    }

    return '';
  }
}

class StockApi {
  static final FirebaseFirestore _db = FirebaseFirestore.instance;
  static const String _collectionName = 'aggregated_items';
  static const String _statusCacheKey = 'kr_stock_status_cache_v8';
  static bool _isNoticeForMerge(StockItem item) => item.status == '공지';

  static bool _isBnkrItem(StockItem item) {
    return item.site.toLowerCase().contains('bnkr') ||
        item.mallName.contains('반다이남코코리아몰') ||
        item.sourcePage.toLowerCase().contains('bnkr');
  }

  static List<String> _matchTokens(StockItem item) {
    final text = [
      item.name,
      item.title,
    ].join(' ').toLowerCase();

    var normalized = text
        .replaceAll('&nbsp;', ' ')
        .replaceAll('&#39;', "'")
        .replaceAll('&amp;', '&')
        .replaceAll('ver.ka', 'verka')
        .replaceAll('ver ka', 'verka')
        .replaceAll('version ka', 'verka')
        .replaceAll('master grade', 'mg')
        .replaceAll('high grade', 'hg')
        .replaceAll('real grade', 'rg')
        .replaceAll('perfect grade', 'pg')
        .replaceAll('full mechanics', 'fullmechanics')
        .replaceAll('re/100', 're100');

    normalized = normalized.replaceAll(RegExp(r'1\s*/\s*100'), '1/100');
    normalized = normalized.replaceAll(RegExp(r'1\s*/\s*144'), '1/144');
    normalized = normalized.replaceAll(RegExp(r'1\s*/\s*60'), '1/60');

    normalized = normalized.replaceAll(RegExp(r'[\[\]\(\)【】]'), ' ');
    normalized = normalized.replaceAll(RegExp(r'[^a-z0-9가-힣/\- ]+'), ' ');
    normalized = normalized.replaceAll(RegExp(r'\s+'), ' ').trim();

    const stopWords = {
      '건담',
      'gundam',
      '건프라',
      '프라모델',
      'model',
      'kit',
      'plastic',
      'the',
      'mobile',
      'suit',
      'mg',
      'hg',
      'rg',
      'pg',
      'sd',
      'mgex',
      'mgsd',
      're100',
      'fullmechanics',
      '1/100',
      '1/144',
      '1/60',
      'verka',
      '판매중',
      '품절',
      '예약중',
      '입고예정',
    };

    final tokens = normalized
        .split(' ')
        .map((e) => e.trim())
        .where((e) => e.isNotEmpty && !stopWords.contains(e))
        .where((e) => e.length >= 2)
        .toList();

    tokens.sort();
    return tokens.toSet().toList();
  }

  static bool _isSimilarItem(StockItem a, StockItem b) {
    if (StockApi._isNoticeForMerge(a) || StockApi._isNoticeForMerge(b)) {
      return false;
    }

    final gradeA = _extractGrade('${a.name} ${a.title}');
    final gradeB = _extractGrade('${b.name} ${b.title}');

    // 🔥 핵심: 하나라도 UNKNOWN이면 합치지 않음
    if (gradeA != gradeB) {
      return false;
    }
    final priceA = _priceToInt(a.price);
    final priceB = _priceToInt(b.price);

    if (priceA != null && priceB != null) {
      final ratio = priceA > priceB
          ? priceA / priceB
          : priceB / priceA;

      // ❗ 가격 2배 이상 차이나면 다른 상품
      if (ratio >= 2.0) return false;
    }
    
    final tokensA = _matchTokens(a);
    final tokensB = _matchTokens(b);

    if (tokensA.isEmpty || tokensB.isEmpty) return false;

    final intersection = tokensA.where(tokensB.contains).toList();

    // 한국어 핵심 토큰 (3글자 이상) 1개라도 겹치면 OK
    final hasStrongKorean = intersection.any(
      (t) => RegExp(r'[가-힣]').hasMatch(t) && t.length >= 3,
    );

    if (hasStrongKorean) return true;

    // 영어/숫자 토큰은 2개 이상 겹쳐야
    if (intersection.length >= 2) return true;

    return false;
  }

  static Stream<List<StockItem>> watchItems() {
    return _db.collection(_collectionName).snapshots().asyncMap((snapshot) async {
      final allItems = snapshot.docs.map(StockItem.fromFirestore).toList();

      final krItems = allItems.where(_isKoreanItem).toList();

      final validItems = krItems
          .where(_isValidGundamItemOrNotice)
          .toList();

      final bnkrRaw = allItems.where((e) =>
          e.site.toLowerCase().contains('bnkr') ||
          e.mallName.contains('반다이남코코리아몰') ||
          e.sourcePage.toLowerCase().contains('bnkr')).length;
      print('BNKR 원본 문서 수: $bnkrRaw');

      final bnkrFiltered = krItems.where((e) =>
          e.site.toLowerCase().contains('bnkr') ||
          e.mallName.contains('반다이남코코리아몰') ||
          e.sourcePage.toLowerCase().contains('bnkr')).length;
      print('BNKR 한국필터 후: $bnkrFiltered');

      final bnkrValid = validItems.where((e) =>
          e.site.toLowerCase().contains('bnkr') ||
          e.mallName.contains('반다이남코코리아몰') ||
          e.sourcePage.toLowerCase().contains('bnkr')).length;
      print('BNKR 최종 필터 후: $bnkrValid');

      final grouped = _groupDuplicateItems(validItems);

      final bnkrGrouped = grouped.where((e) =>
          e.site.toLowerCase().contains('bnkr') ||
          e.mallName.contains('반다이남코코리아몰') ||
          e.sourcePage.toLowerCase().contains('bnkr') ||
          e.offers.any((o) => o.seller.contains('반다이남코코리아몰'))).length;
      print('BNKR 그룹 후 카드 수: $bnkrGrouped');

      final withRestock = await _applyRestockDetection(grouped);
      withRestock.sort(_sortItems);

      final multiSellerCount = grouped.where((e) => e.sellerCount > 1).length;
      print('판매처 2곳 이상 카드 수: $multiSellerCount');

      return withRestock;
    });
  }

  static bool _isKoreanItem(StockItem item) {
    final combined = [
      item.sourcePage,
      item.site,
      item.mallName,
      item.url,
      item.productUrl,
      item.resolvedUrl,
    ].join(' ').toLowerCase();

    const krKeywords = [
      'kr',
      'korea',
      'gundambase',
      'gundamshop',
      'bnkrmall',
      'hobbyfactory',
      'gundamcity',
      'modelsale',
      '건담베이스',
      '건담샵',
      '반케이알몰',
      '반다이남코코리아몰',
      '하비팩토리',
      '건담시티',
      '모델세일',
      '.co.kr',
    ];

    for (final keyword in krKeywords) {
      if (combined.contains(keyword)) return true;
    }
    return false;
  }

  static bool _isValidGundamItemOrNotice(StockItem item) {
    final text = [
      item.name,
      item.title,
      item.site,
      item.mallName,
    ].join(' ').toLowerCase();

    final isBnkr = item.site.toLowerCase().contains('bnkr') ||
        item.mallName.contains('반다이남코코리아몰') ||
        item.sourcePage.toLowerCase().contains('bnkr');

    // 공지는 무조건 통과
    if (item.status == '공지') return true;

    // 강력 제외
    const excludeKeywords = [
      '울트라맨',
      '마블',
      '피규어',
      '넨도로이드',
      '세피로스',
      '파이널 판타지',
      '하츠네 미쿠',
      'kof',
      '킹오파',
      '드래곤볼',
      '포켓몬',
      '원피스',
      '디지몬',
    ];

    for (final keyword in excludeKeywords) {
      if (text.contains(keyword)) return false;
    }

    // BNKR는 크롤링 단계에서 이미 한 번 건담 필터를 통과했으므로 통과
    if (isBnkr) {
      return true;
    }

    // 일반 사이트는 건담 관련 키워드 필수
    const includeKeywords = [
      '건담',
      'gundam',
      'hg',
      'mg',
      'rg',
      'pg',
      'sd',
      'mgex',
      'mgsd',
      '건프라',
      'gunpla',
    ];

    for (final keyword in includeKeywords) {
      if (text.contains(keyword)) return true;
    }

    return false;
  }

  static List<StockItem> _groupDuplicateItems(List<StockItem> items) {
    // 1차: 기존 키 기반 그룹핑
    final Map<String, List<StockItem>> exactGroups = {};

    for (final item in items) {
      final key = _groupKey(item);
      exactGroups.putIfAbsent(key, () => []).add(item);
    }

    // 2차: 퍼지 병합
    final List<List<StockItem>> mergedGroups = [];

    for (final group in exactGroups.values) {
      bool merged = false;

      for (final existing in mergedGroups) {
        final a = existing.first;
        final b = group.first;

        // BNKR는 퍼지 병합 금지: exact key 그룹만 유지
        if (_isBnkrItem(a) || _isBnkrItem(b)) {
          continue;
        }

        if (StockApi._isSimilarItem(a, b)) {
          existing.addAll(group);
          merged = true;
          break;
        }
      }

      if (!merged) {
        mergedGroups.add(List<StockItem>.from(group));
      }
    }

    final List<StockItem> result = [];

    for (final groupedItems in mergedGroups) {
      final representative = _chooseRepresentative(groupedItems);

      final offers = groupedItems
          .map((e) => StockOffer(
                seller: e.mallName.isNotEmpty ? e.mallName : e.site,
                price: _normalizePriceText(e.price),
                status: e.status,
                resolvedUrl: e.resolvedUrl,
              ))
          .toList();

      final uniqueOffers = <String, StockOffer>{};
      for (final offer in offers) {
        final key =
            '${offer.seller}|${_priceToInt(offer.price) ?? -1}|${offer.status}|${offer.resolvedUrl}';
        uniqueOffers[key] = offer;
      }

      final dedupedOffers = uniqueOffers.values.toList();

      dedupedOffers.sort((a, b) {
        final statusCompare =
            _offerStatusRank(a.status).compareTo(_offerStatusRank(b.status));
        if (statusCompare != 0) return statusCompare;

        final ap = _priceToInt(a.price);
        final bp = _priceToInt(b.price);

        if (ap == null && bp == null) return 0;
        if (ap == null) return 1;
        if (bp == null) return -1;

        return ap.compareTo(bp);
      });

      final minOffer = _pickBestOffer(dedupedOffers);
      final minPrice = minOffer?.price ?? representative.price;
      final bestUrl = (minOffer != null && minOffer.resolvedUrl.isNotEmpty)
          ? minOffer.resolvedUrl
          : representative.resolvedUrl;

      result.add(
        representative.copyWith(
          itemId: _groupKey(representative),
          offers: dedupedOffers,
          sellerCount: dedupedOffers.length,
          minPrice: minPrice,
          price: minPrice.isNotEmpty ? minPrice : representative.price,
          resolvedUrl: bestUrl,
          mallName: dedupedOffers.length > 1
              ? '${dedupedOffers.first.seller} 외 ${dedupedOffers.length - 1}곳'
              : representative.mallName,
        ),
      );
    }

    return result;
  }

  static StockOffer? _pickBestOffer(List<StockOffer> offers) {
    if (offers.isEmpty) return null;

    StockOffer? best;

    for (final offer in offers) {
      if (best == null) {
        best = offer;
        continue;
      }

      final aRank = _offerStatusRank(offer.status);
      final bRank = _offerStatusRank(best.status);

      if (aRank < bRank) {
        best = offer;
        continue;
      }

      if (aRank == bRank) {
        final ap = _priceToInt(offer.price);
        final bp = _priceToInt(best.price);

        if (ap != null && bp == null) {
          best = offer;
          continue;
        }

        if (ap != null && bp != null && ap < bp) {
          best = offer;
        }
      }
    }

    return best;
  }

  static String _groupKey(StockItem item) {
    if (_isBnkrItem(item)) {
      return 'BNKR_${item.itemId}';
    }
    
    if (item.status == '공지') {
      final title = _canonicalName(
        item.title.isNotEmpty ? item.title : item.name,
      );
      final date = item.noticeDate.isNotEmpty ? item.noticeDate : 'notice';
      return 'NOTICE_${date}_$title';
    }

    final siteAwareName = _buildSiteAwareGroupName(item);

    final grade = _extractGrade([
      item.name,
      item.title,
      siteAwareName,
    ].join(' '));

    return '${grade}_$siteAwareName';
  }

  static StockItem _chooseRepresentative(List<StockItem> items) {
    items.sort((a, b) {
      final sa = _statusScore(a.status);
      final sb = _statusScore(b.status);
      if (sa != sb) return sb.compareTo(sa);

      final ap = _priceToInt(a.price);
      final bp = _priceToInt(b.price);
      if (ap == null && bp == null) return 0;
      if (ap == null) return 1;
      if (bp == null) return -1;
      return ap.compareTo(bp);
    });

    return items.first;
  }

  static String _prettySellerName(String raw) {
    final text = raw.trim().toLowerCase();

    if (text.contains('gundamshop') || text.contains('건담샵')) return '건담샵';
    if (text.contains('gundamcity') || text.contains('건담시티')) return '건담시티';
    if (text.contains('hobbyfactory') || text.contains('하비팩토리')) return '하비팩토리';
    if (text.contains('modelsale') || text.contains('모델세일')) return '모델세일';
    if (text.contains('gundambase') || text.contains('건담베이스')) return '건담베이스';
    if (text.contains('bnkrmall') || text.contains('반다이남코코리아몰')) {
      return '반다이남코코리아몰';
    }

    return raw.trim();
  }

  static Future<List<StockItem>> _applyRestockDetection(List<StockItem> items) async {
    final prefs = await SharedPreferences.getInstance();
    final raw = prefs.getString(_statusCacheKey);

    Map<String, dynamic> previousMap = {};
    if (raw != null && raw.isNotEmpty) {
      try {
        previousMap = jsonDecode(raw) as Map<String, dynamic>;
      } catch (_) {
        previousMap = {};
      }
    }

    final Map<String, String> nextMap = {};
    final List<StockItem> result = [];

    for (final item in items) {
      final key = item.itemId;
      final currentStatus = _normalizedStatus(item);
      final previousStatus = (previousMap[key] ?? '').toString();

      final restocked = previousStatus.isNotEmpty &&
          (previousStatus == 'soldout' || previousStatus == 'reserve') &&
          currentStatus == 'onsale';

      nextMap[key] = currentStatus;
      result.add(item.copyWith(isRestocked: restocked));
    }

    await prefs.setString(_statusCacheKey, jsonEncode(nextMap));
    return result;
  }

  static String _normalizedStatus(StockItem item) {
    final raw = '${item.status} ${item.stockText}'.trim().toLowerCase();

    if (_containsAny(raw, ['판매중', '구매가능', 'available', 'in stock'])) {
      return 'onsale';
    }
    if (_containsAny(raw, ['품절', 'sold out', 'out of stock'])) {
      return 'soldout';
    }
    if (_containsAny(raw, ['예약', 'preorder', 'pre-order', 'pre order'])) {
      return 'reserve';
    }
    if (_containsAny(raw, ['공지', 'notice'])) {
      return 'notice';
    }
    if (_containsAny(raw, ['입고예정', 'coming soon'])) {
      return 'coming';
    }
    return 'unknown';
  }

  static int _sortItems(StockItem a, StockItem b) {
    if (a.status == '공지' && b.status != '공지') return -1;
    if (a.status != '공지' && b.status == '공지') return 1;

    if (a.isRestocked != b.isRestocked) {
      return a.isRestocked ? -1 : 1;
    }

    final aUnknown = a.status == '상태 확인중';
    final bUnknown = b.status == '상태 확인중';
    if (aUnknown != bUnknown) {
      return aUnknown ? 1 : -1;
    }

    final ap = _priceToInt(a.minPrice);
    final bp = _priceToInt(b.minPrice);
    if (ap != null && bp != null && ap != bp) {
      return ap.compareTo(bp);
    }

    final aName = _canonicalName(a.name.isNotEmpty ? a.name : a.title).toLowerCase();
    final bName = _canonicalName(b.name.isNotEmpty ? b.name : b.title).toLowerCase();
    return aName.compareTo(bName);
  }
}