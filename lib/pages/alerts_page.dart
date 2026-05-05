import 'package:flutter/material.dart';
import '../services/alert_service.dart';

class AlertsPage extends StatelessWidget {
  const AlertsPage({super.key});

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: const Text('내 알림 목록'),
        centerTitle: true,
      ),
      body: ValueListenableBuilder<Set<String>>(
        valueListenable: AlertService.alertIds,
        builder: (context, alertIds, _) {
          final alerts = alertIds.toList();

          if (alerts.isEmpty) {
            return const Center(
              child: Text('신청한 재입고 알림이 없습니다.'),
            );
          }

          return ListView.builder(
            padding: const EdgeInsets.all(16),
            itemCount: alerts.length,
            itemBuilder: (context, index) {
              final alertId = alerts[index];
              final parts = alertId.split('|');

              final countryCode = parts.isNotEmpty ? parts[0] : '';
              final storeName = parts.length > 1 ? parts[1] : '';
              final itemName = parts.length > 2 ? parts[2] : '';

              return Card(
                margin: const EdgeInsets.only(bottom: 12),
                child: ListTile(
                  leading: const Icon(Icons.notifications_active),
                  title: Text(itemName),
                  subtitle: Text('$countryCode · $storeName'),
                  trailing: IconButton(
                    icon: const Icon(Icons.close),
                    onPressed: () {
                      AlertService.toggleAlert(alertId);
                    },
                  ),
                ),
              );
            },
          );
        },
      ),
    );
  }
}