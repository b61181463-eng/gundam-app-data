import 'package:flutter/foundation.dart';

class AlertService {
  static final ValueNotifier<Set<String>> alertIds = ValueNotifier(<String>{});

  static String makeAlertId({
    required String countryCode,
    required String storeName,
    required String itemName,
  }) {
    return '$countryCode|$storeName|$itemName';
  }

  static bool isAlertEnabled(String alertId) {
    return alertIds.value.contains(alertId);
  }

  static void toggleAlert(String alertId) {
    final current = Set<String>.from(alertIds.value);

    if (current.contains(alertId)) {
      current.remove(alertId);
    } else {
      current.add(alertId);
    }

    alertIds.value = current;
  }
}