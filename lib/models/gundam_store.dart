import 'gundam_item.dart';

class GundamStore {
  final String name;
  final String city;
  final String address;
  final String phone;
  final String openHours;
  final bool isOpen;
  final List<GundamItem> items;

  const GundamStore({
    required this.name,
    required this.city,
    required this.address,
    required this.phone,
    required this.openHours,
    required this.isOpen,
    required this.items,
  });

  factory GundamStore.fromJson(Map<String, dynamic> json) {
    final itemsJson = json['items'] as List<dynamic>;

    return GundamStore(
      name: json['name'] as String,
      city: json['city'] as String,
      address: json['address'] as String,
      phone: json['phone'] as String,
      openHours: json['openHours'] as String,
      isOpen: json['isOpen'] as bool,
      items: itemsJson
          .map((item) => GundamItem.fromJson(item as Map<String, dynamic>))
          .toList(),
    );
  }
}