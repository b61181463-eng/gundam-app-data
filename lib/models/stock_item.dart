class StockItem {
  final String productId;
  final String site;
  final String name;
  final String normalizedName;
  final String productUrl;
  final String imageUrl;
  final String rawStockText;
  final String stockStatus;

  StockItem({
    required this.productId,
    required this.site,
    required this.name,
    required this.normalizedName,
    required this.productUrl,
    required this.imageUrl,
    required this.rawStockText,
    required this.stockStatus,
  });

  Map<String, dynamic> toMap() {
    return {
      'productId': productId,
      'site': site,
      'name': name,
      'normalizedName': normalizedName,
      'productUrl': productUrl,
      'imageUrl': imageUrl,
      'rawStockText': rawStockText,
      'stockStatus': stockStatus,
    };
  }

  factory StockItem.fromMap(Map<String, dynamic> map) {
    return StockItem(
      productId: map['productId'] ?? '',
      site: map['site'] ?? '',
      name: map['name'] ?? '',
      normalizedName: map['normalizedName'] ?? '',
      productUrl: map['productUrl'] ?? '',
      imageUrl: map['imageUrl'] ?? '',
      rawStockText: map['rawStockText'] ?? '',
      stockStatus: map['stockStatus'] ?? 'unknown',
    );
  }
}