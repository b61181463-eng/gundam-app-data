class StoreModel {
  final String id;
  final String countryCode;
  final String name;
  final String city;
  final List<String> keywords;

  const StoreModel({
    required this.id,
    required this.countryCode,
    required this.name,
    required this.city,
    required this.keywords,
  });

  factory StoreModel.fromMap(Map<String, dynamic> map, String documentId) {
    return StoreModel(
      id: map['id'] ?? documentId,
      countryCode: map['countryCode'] ?? '',
      name: map['name'] ?? '',
      city: map['city'] ?? '',
      keywords: List<String>.from(map['keywords'] ?? const []),
    );
  }

  Map<String, dynamic> toMap() {
    return {
      'id': id,
      'countryCode': countryCode,
      'name': name,
      'city': city,
      'keywords': keywords,
    };
  }
}