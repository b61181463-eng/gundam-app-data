class GundamItem {
  final String name;
  final String stock;
  final String grade;
  final String price;
  final String updatedAt;

  const GundamItem({
    required this.name,
    required this.stock,
    required this.grade,
    required this.price,
    required this.updatedAt,
  });

  bool get inStock => stock == '재고 있음';

  factory GundamItem.fromJson(Map<String, dynamic> json) {
    return GundamItem(
      name: json['name'] as String,
      stock: json['stock'] as String,
      grade: json['grade'] as String,
      price: json['price'] as String,
      updatedAt: json['updatedAt'] as String,
    );
  }
}