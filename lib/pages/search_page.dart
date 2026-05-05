import 'package:flutter/material.dart';

class SearchPage extends StatelessWidget {
  const SearchPage({super.key});

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: const Text('전세계 검색'),
      ),
      body: const Center(
        child: Text('검색 기능은 다음 단계에서 확장할게!'),
      ),
    );
  }
}