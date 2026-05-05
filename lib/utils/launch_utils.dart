import 'package:flutter/material.dart';
import 'package:url_launcher/url_launcher.dart';

class LaunchUtils {
  static Future<void> openUrl(BuildContext context, String url) async {
    final cleaned = url.trim();

    if (cleaned.isEmpty) {
      if (context.mounted) {
        ScaffoldMessenger.of(context).showSnackBar(
          const SnackBar(content: Text('열 수 있는 링크가 없습니다.')),
        );
      }
      return;
    }

    final uri = Uri.tryParse(cleaned);

    if (uri == null) {
      if (context.mounted) {
        ScaffoldMessenger.of(context).showSnackBar(
          const SnackBar(content: Text('링크 형식이 올바르지 않습니다.')),
        );
      }
      return;
    }

    final success = await launchUrl(
      uri,
      mode: LaunchMode.externalApplication,
    );

    if (!success && context.mounted) {
      ScaffoldMessenger.of(context).showSnackBar(
        const SnackBar(content: Text('링크를 열지 못했습니다.')),
      );
    }
  }
}