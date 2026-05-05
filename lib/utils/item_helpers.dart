class ItemHelpers {
  static String safeString(dynamic value, {String fallback = '-'}) {
    if (value == null) return fallback;
    final text = value.toString().trim();
    return text.isEmpty ? fallback : text;
  }

  static bool isInStock(Map<String, dynamic> data) {
    final status = (data['status'] ?? '').toString().toLowerCase().trim();
    final stockText = (data['stockText'] ?? '').toString().toLowerCase().trim();
    final availability =
        (data['availability'] ?? '').toString().toLowerCase().trim();
    final stockStatus =
        (data['stockStatus'] ?? '').toString().toLowerCase().trim();

    final combined = '$status $stockText $availability $stockStatus';

    final stock = data['stock'];
    if (stock is num) {
      return stock > 0;
    }

    const inStockKeywords = [
      'in stock',
      'instock',
      'available',
      'buy now',
      'add to cart',
      '재고있음',
      '재고 있음',
      '재고 보유',
      '구매 가능',
      '주문 가능',
      '판매중',
      '판매 중',
      '재입고',
      '즉시 구매',
      '바로 구매',
      '장바구니',
      '구매하기',
    ];

    const outOfStockKeywords = [
      'out of stock',
      'sold out',
      'unavailable',
      '품절',
      '일시 품절',
      '재고없음',
      '재고 없음',
      '판매 종료',
      '구매 불가',
      '입고 예정',
    ];

    final hasInStock = inStockKeywords.any((k) => combined.contains(k));
    final hasOutOfStock = outOfStockKeywords.any((k) => combined.contains(k));

    if (hasOutOfStock) return false;
    if (hasInStock) return true;

    return false;
  }

  static String itemName(Map<String, dynamic> data) {
    return safeString(data['name'] ?? data['title']);
  }

  static String itemStore(Map<String, dynamic> data) {
    return safeString(
      data['mallName'] ?? data['site'] ?? data['source'],
      fallback: '한국 스토어',
    );
  }

  static String itemPrice(Map<String, dynamic> data) {
    return safeString(data['price'], fallback: '가격 정보 없음');
  }

  static String itemStockLabel(Map<String, dynamic> data) {
    final stockText = safeString(data['stockText'], fallback: '');
    final status = safeString(data['status'], fallback: '');
    final availability = safeString(data['availability'], fallback: '');
    final stockStatus = safeString(data['stockStatus'], fallback: '');

    if (stockText.isNotEmpty && stockText != '-') return stockText;
    if (status.isNotEmpty && status != '-') return status;
    if (availability.isNotEmpty && availability != '-') return availability;
    if (stockStatus.isNotEmpty && stockStatus != '-') return stockStatus;

    return '상태 확인 필요';
  }

  static String itemImageUrl(Map<String, dynamic> data) {
    return safeString(data['imageUrl'], fallback: '');
  }

  static String itemProductUrl(Map<String, dynamic> data) {
    return safeString(
      data['viewUrl'] ?? data['productUrl'] ?? data['url'] ?? data['link'],
      fallback: '',
    );
  }

  static bool isNoticeItem(Map<String, dynamic> data) {
    final sourceType = safeString(data['sourceType'], fallback: '').toLowerCase();
    final source = safeString(data['source'], fallback: '').toLowerCase();
    final status = safeString(data['status'], fallback: '').toLowerCase();

    return sourceType == 'notice_item' ||
        source.contains('notice') ||
        status.contains('입고 예정');
  }

  static String noticeDateText(Map<String, dynamic> data) {
    final raw = safeString(data['noticeDate'], fallback: '');
    if (raw.isEmpty || raw == '-') return '공지 날짜 없음';
    return raw;
  }

  static DateTime? parseNoticeDate(Map<String, dynamic> data) {
    final raw = safeString(data['noticeDate'], fallback: '');
    if (raw.isEmpty || raw == '-') return null;

    try {
      return DateTime.parse(raw);
    } catch (_) {
      return null;
    }
  }

  static bool isTodayNotice(Map<String, dynamic> data) {
    final date = parseNoticeDate(data);
    if (date == null) return false;

    final now = DateTime.now();
    return date.year == now.year &&
        date.month == now.month &&
        date.day == now.day;
  }

  static String changeType(Map<String, dynamic> data) {
    return safeString(data['changeType'], fallback: '').toLowerCase();
  }

  static bool hasRecentChange(Map<String, dynamic> data) {
    final type = changeType(data);
    return type.isNotEmpty && type != '-';
  }

  static bool isRestocked(Map<String, dynamic> data) {
    return changeType(data) == 'restocked';
  }

  static bool isNewlyAdded(Map<String, dynamic> data) {
    return changeType(data) == 'notice_added';
  }

  static bool isStatusChanged(Map<String, dynamic> data) {
    return changeType(data) == 'status_changed';
  }

  static String changeLabel(Map<String, dynamic> data) {
    final type = changeType(data);

    switch (type) {
      case 'restocked':
        return '재입고';
      case 'notice_added':
        return 'NEW';
      case 'status_changed':
        return '변동';
      case 'sold_out':
        return '품절';
      default:
        return '';
    }
  }

  static bool isCrossChecked(Map<String, dynamic> data) {
    return safeString(data['verificationStatus'], fallback: '')
            .toLowerCase() ==
        'cross_checked';
  }

  static int verificationCount(Map<String, dynamic> data) {
    final value = data['verificationCount'];
    if (value is int) return value;
    if (value is num) return value.toInt();
    return 0;
  }
  static bool shouldHideBrokenOrGeneric(Map<String, dynamic> data) {
    final name = safeString(data['name'] ?? data['title'], fallback: '').trim();

    if (name.isEmpty) return true;

    const blockedExact = {
      '프라모델',
      '건프라',
      '모형',
      '건담 프라모델',
      '건담프라모델',
      '프라모델 키트',
      '건담 키트',
      'mgsd',
      'hg',
      'mg',
      'rg',
      'pg',
      'sd',
      'eg',
      'bb',
      're/100',
      '30ms',
      '30mm',
    };

    const blockedContains = [
      '올라오고 있습니다',
      '건담베이스는',
      '확인 부탁',
      '확인 바랍니다',
      '매장별로 상이',
      '점포별로 상이',
      '유의사항',
      '공지 확인',
      '판매 방식',
      '출처',
      '댓글',
      '링크',
      '안내',
      '이벤트',
      '프라모델',
      'www',
      'http',
      'https',
      '.com',
      '.kr',
      '�',
      'Ã',
      'Â',
      '°ç',
      '´ã',
      '¿',
      '½',
      'À',
      'Ã¬',
      'Ã¥',
      'clamp',
      'sega',
      'tsuburaya',
      'trigger,akira',
      'copyright',
    ];

    final lower = name.toLowerCase();

    if (blockedExact.contains(lower)) return true;
    if (blockedContains.any((e) => lower.contains(e.toLowerCase()))) return true;

    // 저작권/회사명/연도 범위 패턴
    if (name.contains('©')) return true;
    if (RegExp(r'\b20\d{2}\s*-\s*20\d{2}\b').hasMatch(name)) return true;

    // 문장형 끝맺음
    if (lower.endsWith('합니다') ||
        lower.endsWith('합니다.') ||
        lower.endsWith('있습니다') ||
        lower.endsWith('있습니다.') ||
        lower.endsWith('드립니다') ||
        lower.endsWith('드립니다.') ||
        lower.endsWith('바랍니다') ||
        lower.endsWith('바랍니다.') ||
        lower.endsWith('입니다') ||
        lower.endsWith('입니다.') ||
        lower.endsWith('됩니다') ||
        lower.endsWith('됩니다.')) {
      return true;
    }

    // 공백이 너무 많으면 설명문일 가능성 큼
    if (' '.allMatches(name).length >= 8) return true;

    return false;
  }
}