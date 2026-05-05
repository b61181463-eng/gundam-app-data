import 'package:flutter/material.dart';
import 'package:url_launcher/url_launcher.dart';

class ProductLinkHelper {
  static String? extractLink(Map<String, dynamic> item) {
    final candidates = [
      item['productUrl'],
      item['url'],
      item['link'],
    ];

    for (final value in candidates) {
      if (value == null) continue;
      final text = value.toString().trim();
      if (text.isNotEmpty) {
        return text;
      }
    }

    return null;
  }

  static bool hasLink(Map<String, dynamic> item) {
    final link = extractLink(item);
    return link != null && link.isNotEmpty;
  }

  static Future<void> openProductPage(
    BuildContext context,
    Map<String, dynamic> item,
  ) async {
    final link = extractLink(item);

    if (link == null || link.isEmpty) {
      _showMessage(context, '이 상품은 이동할 링크가 없어요.');
      return;
    }

    Uri uri;
    try {
      uri = Uri.parse(link);
    } catch (_) {
      _showMessage(context, '링크 형식이 올바르지 않아요.');
      return;
    }

    try {
      final success = await launchUrl(
        uri,
        mode: LaunchMode.externalApplication,
      );

      if (!success) {
        _showMessage(context, '상품 페이지를 열지 못했어요.');
      }
    } catch (_) {
      _showMessage(context, '상품 페이지를 여는 중 오류가 발생했어요.');
    }
  }

  static void _showMessage(BuildContext context, String message) {
    if (!context.mounted) return;

    ScaffoldMessenger.of(context).showSnackBar(
      SnackBar(content: Text(message)),
    );
  }
}