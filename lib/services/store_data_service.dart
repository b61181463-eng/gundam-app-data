import 'dart:convert';
import 'package:flutter/services.dart';
import '../models/gundam_store.dart';

class StoreDataService {
  static Future<List<GundamStore>> loadStoresByCountry(String countryCode) async {
    final path = 'assets/data/stores_${countryCode.toLowerCase()}.json';
    final jsonString = await rootBundle.loadString(path);
    final List<dynamic> jsonData = json.decode(jsonString);

    return jsonData
        .map((store) => GundamStore.fromJson(store as Map<String, dynamic>))
        .toList();
  }
}