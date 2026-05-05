class RegionFilter {
  static bool isKrItem(Map<String, dynamic> item) {
    final country = (item['country'] ?? '').toString().toUpperCase().trim();
    final region = (item['region'] ?? '').toString().toUpperCase().trim();
    final market = (item['market'] ?? '').toString().toUpperCase().trim();
    final countryCode = (item['countryCode'] ?? '').toString().toUpperCase().trim();
    final locale = (item['locale'] ?? '').toString().toUpperCase().trim();

    final site = (item['site'] ?? '').toString().toLowerCase().trim();
    final source = (item['source'] ?? '').toString().toLowerCase().trim();
    final sourceType = (item['sourceType'] ?? '').toString().toLowerCase().trim();
    final mallName = (item['mallName'] ?? '').toString().toLowerCase().trim();
    final seller = (item['seller'] ?? '').toString().toLowerCase().trim();
    final title = (item['name'] ?? item['title'] ?? '').toString().toLowerCase().trim();

    if (country == 'KR' ||
        region == 'KR' ||
        market == 'KR' ||
        countryCode == 'KR' ||
        locale == 'KR') {
      return true;
    }

    const krKeywords = [
      'korea',
      'korean',
      'kr',
      '건담베이스',
      '더건담베이스',
      '한국',
      '코리아',
      'gundamkorea',
      'thegundambase',
      'gunplakorea',
    ];

    final combined = '$site $source $sourceType $mallName $seller $title';

    return krKeywords.any((keyword) => combined.contains(keyword));
  }

  static String krLabel(Map<String, dynamic> item) {
    if (isKrItem(item)) return 'KR';
    return 'ETC';
  }
}