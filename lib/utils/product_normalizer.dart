class ProductNormalizer {
  /// 상품명 정규화
  /// 같은 제품인데 쇼핑몰마다 이름이 다른 문제를 줄이기 위한 키 생성
  static String normalizeName(String rawName) {
    var name = rawName.toLowerCase();

    // 괄호 내용 제거
    name = name.replaceAll(RegExp(r'\([^)]*\)'), ' ');
    name = name.replaceAll(RegExp(r'\[[^\]]*\]'), ' ');

    // 브랜드/불필요 단어 제거
    final removeWords = [
      'bandai',
      '반다이',
      '반다이스피리츠',
      'banpresto',
      '건담프라모델',
      '프라모델',
      '재입고',
      '예약',
      '입고',
      '특가',
      '세일',
      '신상품',
      '당일발송',
      '국내배송',
      '정품',
      '한정판',
      '일본내수',
    ];

    for (final word in removeWords) {
      name = name.replaceAll(word, ' ');
    }

    // 등급 통일
    name = name
        .replaceAll('master grade', 'mg')
        .replaceAll('real grade', 'rg')
        .replaceAll('high grade', 'hg')
        .replaceAll('perfect grade', 'pg')
        .replaceAll('entry grade', 'eg')
        .replaceAll('super deformed', 'sd');

    // 스케일 제거
    name = name.replaceAll(RegExp(r'1\s*/\s*144'), ' ');
    name = name.replaceAll(RegExp(r'1\s*/\s*100'), ' ');
    name = name.replaceAll(RegExp(r'1\s*/\s*60'), ' ');

    // 특수문자 제거
    name = name.replaceAll(RegExp(r'[^a-z0-9가-힣\s]'), ' ');

    // 공백 정리
    name = name.replaceAll(RegExp(r'\s+'), ' ').trim();

    return name;
  }

  /// 등급 추출
  static String detectGrade(String rawName) {
    final name = rawName.toLowerCase();

    if (name.contains('mgex')) return 'MGEX';
    if (name.contains('mgsd')) return 'MGSD';
    if (RegExp(r'\bpg\b|perfect grade|퍼펙트').hasMatch(name)) return 'PG';
    if (RegExp(r'\bmg\b|master grade|마스터').hasMatch(name)) return 'MG';
    if (RegExp(r'\brg\b|real grade|리얼').hasMatch(name)) return 'RG';
    if (RegExp(r'\bhg\b|hgce|high grade|하이').hasMatch(name)) return 'HG';
    if (RegExp(r'\beg\b|entry grade|엔트리').hasMatch(name)) return 'EG';
    if (RegExp(r'\bsd\b|sdcs|sdw|super deformed').hasMatch(name)) return 'SD';

    return '기타';
  }

  /// 상태 정규화
  static String normalizeStatus(String rawStatus, {String? rawName}) {
    final status = rawStatus.toLowerCase();
    final name = (rawName ?? '').toLowerCase();
    final text = '$status $name';

    if (_containsAny(text, [
      '품절',
      'sold out',
      'soldout',
      '일시품절',
      '재고없음',
      'out of stock',
    ])) {
      return '품절';
    }

    if (_containsAny(text, [
      '예약',
      'pre-order',
      'preorder',
      'pre order',
      '예약판매',
      '예약중',
    ])) {
      return '예약중';
    }

    if (_containsAny(text, [
      '입고예정',
      '출시예정',
      'coming soon',
      '발매예정',
    ])) {
      return '입고예정';
    }

    if (_containsAny(text, [
      '판매중',
      '구매가능',
      '재고있음',
      'in stock',
      'available',
      '장바구니',
      '바로구매',
    ])) {
      return '판매중';
    }

    return '상태 확인중';
  }

  /// 가격 정리
  static int? normalizePrice(dynamic rawPrice) {
    if (rawPrice == null) return null;

    final text = rawPrice.toString();

    final onlyNumber = text.replaceAll(RegExp(r'[^0-9]'), '');

    if (onlyNumber.isEmpty) return null;

    final price = int.tryParse(onlyNumber);

    if (price == null) return null;

    // 너무 말도 안 되는 가격 제거
    if (price < 1000) return null;
    if (price > 3000000) return null;

    return price;
  }

  /// 최저가 비교에 넣을 수 있는 상품인지
  static bool canUseForLowestPrice({
    required String status,
    required int? price,
  }) {
    if (price == null) return false;
    if (status == '품절') return false;
    if (status == '상태 확인중') return false;

    return true;
  }

  /// 중복 묶기용 key 생성
  static String makeGroupKey(String rawName) {
    final normalized = normalizeName(rawName);
    final grade = detectGrade(rawName);

    return '$grade::$normalized';
  }

  static bool _containsAny(String text, List<String> keywords) {
    return keywords.any((keyword) => text.contains(keyword));
  }
}