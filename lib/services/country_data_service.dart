import 'dart:convert';
import 'package:flutter/services.dart';
import '../models/country.dart';

class CountryDataService {
  static Future<List<Country>> loadCountries() async {
    final jsonString =
        await rootBundle.loadString('assets/data/countries.json');
    final List<dynamic> jsonData = json.decode(jsonString);

    return jsonData
        .map((country) => Country.fromJson(country as Map<String, dynamic>))
        .toList();
  }
}