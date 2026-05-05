import 'package:cloud_firestore/cloud_firestore.dart';
import 'package:flutter/material.dart';

class InventoryScreen extends StatefulWidget {
  final String storeId;
  final String storeName;
  final String sourceType;

  const InventoryScreen({
    super.key,
    required this.storeId,
    required this.storeName,
    required this.sourceType,
  });

  @override
  State<InventoryScreen> createState() => _InventoryScreenState();
}

class _InventoryScreenState extends State<InventoryScreen> {
  final TextEditingController _searchController = TextEditingController();
  String _searchText = '';

  bool get isAutoStore => widget.sourceType == 'web_scrape';
  bool get isManualStore => widget.sourceType == 'manual';

  CollectionReference<Map<String, dynamic>> get _itemsRef {
    return FirebaseFirestore.instance
        .collection('stores')
        .doc(widget.storeId)
        .collection('items');
  }

  CollectionReference<Map<String, dynamic>> get _favoritesRef {
    return FirebaseFirestore.instance.collection('favorites');
  }

  @override
  void dispose() {
    _searchController.dispose();
    super.dispose();
  }

  String _normalizeProductName(String name) {
    var text = name.toLowerCase().trim();

    final removeWords = [
      "bandai spirits",
      "bandai",
      "model kit",
      "plastic model",
      "plamo",
      "pre-order",
      "preorder",
      "gundam planet",
      "usa gundam store",
      "newtype",
    ];

    for (final word in removeWords) {
      text = text.replaceAll(word, " ");
    }

    text = text.replaceAll(RegExp(r"\(.*?\)"), " ");

    final replacements = {
      "mobile suit gundam": "gundam",
      "rx 78 2": "rx-78-2",
      "rx78 2": "rx-78-2",
      "rx 78-2": "rx-78-2",
      "rx78-2": "rx-78-2",
      "msn 04": "msn-04",
      "msn04": "msn-04",
      "hguc": "hg",
    };

    replacements.forEach((oldValue, newValue) {
      text = text.replaceAll(oldValue, newValue);
    });

    text = text.replaceAll(RegExp(r"[^a-z0-9가-힣/\-\s]"), " ");
    text = text.replaceAll(RegExp(r"\s+"), " ").trim();

    return text;
  }

  String _statusFromItem({
    required int quantity,
    required bool isSoldOut,
    required bool restocked,
    required String stockStatus,
  }) {
    if (stockStatus.trim().isNotEmpty) {
      final lower = stockStatus.toLowerCase();
      if (lower.contains('out of stock') || lower.contains('sold out')) {
        return 'out_of_stock';
      }
      if (lower.contains('only') || lower.contains('<')) {
        return 'low_stock';
      }
      if (lower.contains('in stock')) {
        return 'in_stock';
      }
    }

    if (isSoldOut || quantity == 0) return 'out_of_stock';
    if (quantity > 0 && quantity <= 10) return 'low_stock';
    if (restocked) return 'in_stock';
    return 'in_stock';
  }

  List<QueryDocumentSnapshot<Map<String, dynamic>>> _filterItems(
    List<QueryDocumentSnapshot<Map<String, dynamic>>> docs,
  ) {
    if (_searchText.trim().isEmpty) return docs;

    final keyword = _searchText.trim().toLowerCase();

    return docs.where((doc) {
      final data = doc.data();
      final name = (data['name'] ?? '').toString().toLowerCase();
      final stockStatus = (data['stockStatus'] ?? '').toString().toLowerCase();
      return name.contains(keyword) || stockStatus.contains(keyword);
    }).toList();
  }

  Future<bool> _isFavorite(String itemId) async {
    final favId = '${widget.storeId}_$itemId';
    final doc = await _favoritesRef.doc(favId).get();
    return doc.exists;
  }

  Future<void> _toggleFavorite({
    required String itemId,
    required String itemName,
    required int quantity,
    required bool isSoldOut,
    required bool restocked,
    required String stockStatus,
  }) async {
    final favId = '${widget.storeId}_$itemId';
    final favDoc = _favoritesRef.doc(favId);
    final existing = await favDoc.get();

    if (existing.exists) {
      await favDoc.delete();

      if (!mounted) return;
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(content: Text('$itemName 찜 해제됨')),
      );
    } else {
      final normalizedName = _normalizeProductName(itemName);
      final currentStatus = _statusFromItem(
        quantity: quantity,
        isSoldOut: isSoldOut,
        restocked: restocked,
        stockStatus: stockStatus,
      );

      await favDoc.set({
        'storeId': widget.storeId,
        'storeName': widget.storeName,
        'itemId': itemId,
        'itemName': itemName,
        'normalizedName': normalizedName,
        'quantity': quantity,
        'isSoldOut': isSoldOut,
        'restocked': restocked,
        'stockStatus': stockStatus,
        'lastKnownStatus': currentStatus,
        'createdAt': FieldValue.serverTimestamp(),
      });

      if (!mounted) return;
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(content: Text('$itemName 찜 추가됨')),
      );
    }

    setState(() {});
  }

  Future<void> _showAddItemDialog() async {
    final nameController = TextEditingController();
    final quantityController = TextEditingController(text: '0');
    bool restocked = false;

    await showDialog(
      context: context,
      builder: (context) {
        return StatefulBuilder(
          builder: (context, setDialogState) {
            return AlertDialog(
              title: const Text('상품 추가'),
              content: SingleChildScrollView(
                child: Column(
                  mainAxisSize: MainAxisSize.min,
                  children: [
                    TextField(
                      controller: nameController,
                      decoration: const InputDecoration(
                        labelText: '상품명',
                      ),
                    ),
                    const SizedBox(height: 12),
                    TextField(
                      controller: quantityController,
                      keyboardType: TextInputType.number,
                      decoration: const InputDecoration(
                        labelText: '수량',
                      ),
                    ),
                    const SizedBox(height: 12),
                    SwitchListTile(
                      contentPadding: EdgeInsets.zero,
                      title: const Text('재입고 표시'),
                      value: restocked,
                      onChanged: (value) {
                        setDialogState(() {
                          restocked = value;
                        });
                      },
                    ),
                  ],
                ),
              ),
              actions: [
                TextButton(
                  onPressed: () => Navigator.pop(context),
                  child: const Text('취소'),
                ),
                ElevatedButton(
                  onPressed: () async {
                    final name = nameController.text.trim();
                    final quantity =
                        int.tryParse(quantityController.text.trim()) ?? 0;

                    if (name.isEmpty) return;

                    await _itemsRef.add({
                      'name': name,
                      'quantity': quantity,
                      'isSoldOut': quantity == 0,
                      'restocked': quantity == 0 ? false : restocked,
                      'stockStatus': quantity == 0 ? 'Out of stock' : 'Manual',
                      'updatedAt': FieldValue.serverTimestamp(),
                      'lastSyncedAt': FieldValue.serverTimestamp(),
                    });

                    if (!mounted) return;
                    Navigator.pop(context);

                    ScaffoldMessenger.of(context).showSnackBar(
                      SnackBar(content: Text('$name 상품 추가됨')),
                    );
                  },
                  child: const Text('추가'),
                ),
              ],
            );
          },
        );
      },
    );
  }

  Future<void> _showEditItemDialog({
    required String itemId,
    required String currentName,
    required int currentQuantity,
    required bool currentRestocked,
  }) async {
    final nameController = TextEditingController(text: currentName);
    final quantityController =
        TextEditingController(text: currentQuantity.toString());
    bool restocked = currentRestocked;

    await showDialog(
      context: context,
      builder: (context) {
        return StatefulBuilder(
          builder: (context, setDialogState) {
            return AlertDialog(
              title: const Text('상품 수정'),
              content: SingleChildScrollView(
                child: Column(
                  mainAxisSize: MainAxisSize.min,
                  children: [
                    TextField(
                      controller: nameController,
                      decoration: const InputDecoration(
                        labelText: '상품명',
                      ),
                    ),
                    const SizedBox(height: 12),
                    TextField(
                      controller: quantityController,
                      keyboardType: TextInputType.number,
                      decoration: const InputDecoration(
                        labelText: '수량',
                      ),
                    ),
                    const SizedBox(height: 12),
                    SwitchListTile(
                      contentPadding: EdgeInsets.zero,
                      title: const Text('재입고 표시'),
                      value: restocked,
                      onChanged: (value) {
                        setDialogState(() {
                          restocked = value;
                        });
                      },
                    ),
                  ],
                ),
              ),
              actions: [
                TextButton(
                  onPressed: () => Navigator.pop(context),
                  child: const Text('취소'),
                ),
                ElevatedButton(
                  onPressed: () async {
                    final name = nameController.text.trim();
                    final quantity =
                        int.tryParse(quantityController.text.trim()) ?? 0;

                    if (name.isEmpty) return;

                    await _itemsRef.doc(itemId).update({
                      'name': name,
                      'quantity': quantity,
                      'isSoldOut': quantity == 0,
                      'restocked': quantity == 0 ? false : restocked,
                      'stockStatus': quantity == 0 ? 'Out of stock' : 'Manual',
                      'updatedAt': FieldValue.serverTimestamp(),
                      'lastSyncedAt': FieldValue.serverTimestamp(),
                    });

                    final favId = '${widget.storeId}_$itemId';
                    final favDoc = await _favoritesRef.doc(favId).get();

                    if (favDoc.exists) {
                      final normalizedName = _normalizeProductName(name);
                      final currentStatus = quantity == 0
                          ? 'out_of_stock'
                          : (quantity <= 10 ? 'low_stock' : 'in_stock');

                      await _favoritesRef.doc(favId).update({
                        'itemName': name,
                        'normalizedName': normalizedName,
                        'quantity': quantity,
                        'isSoldOut': quantity == 0,
                        'restocked': quantity == 0 ? false : restocked,
                        'stockStatus': quantity == 0 ? 'Out of stock' : 'Manual',
                        'lastKnownStatus': currentStatus,
                      });
                    }

                    if (!mounted) return;
                    Navigator.pop(context);

                    ScaffoldMessenger.of(context).showSnackBar(
                      SnackBar(content: Text('$name 상품 수정됨')),
                    );
                  },
                  child: const Text('저장'),
                ),
              ],
            );
          },
        );
      },
    );
  }

  Future<void> _deleteItem({
    required String itemId,
    required String itemName,
  }) async {
    await _itemsRef.doc(itemId).delete();

    final favId = '${widget.storeId}_$itemId';
    final favDoc = await _favoritesRef.doc(favId).get();

    if (favDoc.exists) {
      await _favoritesRef.doc(favId).delete();
    }

    if (!mounted) return;
    ScaffoldMessenger.of(context).showSnackBar(
      SnackBar(content: Text('$itemName 상품 삭제됨')),
    );
  }

  void _showDeleteDialog({
    required String itemId,
    required String itemName,
  }) {
    showDialog(
      context: context,
      builder: (context) {
        return AlertDialog(
          title: const Text('상품 삭제'),
          content: Text('$itemName 상품을 삭제할까요?'),
          actions: [
            TextButton(
              onPressed: () => Navigator.pop(context),
              child: const Text('취소'),
            ),
            ElevatedButton(
              onPressed: () async {
                await _deleteItem(
                  itemId: itemId,
                  itemName: itemName,
                );
                if (!mounted) return;
                Navigator.pop(context);
              },
              child: const Text('삭제'),
            ),
          ],
        );
      },
    );
  }

  Color _statusColor(bool isSoldOut, bool restocked) {
    if (isSoldOut) return Colors.red;
    if (restocked) return Colors.green;
    return Colors.blueGrey;
  }

  String _statusText(bool isSoldOut, bool restocked, String stockStatus) {
    if (stockStatus.trim().isNotEmpty) return stockStatus;
    if (isSoldOut) return '품절';
    if (restocked) return '재입고';
    return '판매중';
  }

  String _formatTimestamp(Timestamp? timestamp) {
    if (timestamp == null) return '동기화 시간 없음';

    final date = timestamp.toDate().toLocal();
    final year = date.year.toString().padLeft(4, '0');
    final month = date.month.toString().padLeft(2, '0');
    final day = date.day.toString().padLeft(2, '0');
    final hour = date.hour.toString().padLeft(2, '0');
    final minute = date.minute.toString().padLeft(2, '0');

    return '$year-$month-$day $hour:$minute';
  }

  Widget _buildStoreTypeBanner() {
    if (isAutoStore) {
      return Container(
        width: double.infinity,
        margin: const EdgeInsets.fromLTRB(16, 12, 16, 0),
        padding: const EdgeInsets.all(12),
        decoration: BoxDecoration(
          color: Colors.blue.withOpacity(0.08),
          borderRadius: BorderRadius.circular(14),
          border: Border.all(
            color: Colors.blue.withOpacity(0.15),
          ),
        ),
        child: const Row(
          children: [
            Icon(Icons.sync, color: Colors.blue),
            SizedBox(width: 8),
            Expanded(
              child: Text(
                '이 매장은 자동 동기화 매장입니다. 상품 추가/수정/삭제는 비활성화됩니다.',
                style: TextStyle(
                  color: Colors.blue,
                  fontWeight: FontWeight.w600,
                ),
              ),
            ),
          ],
        ),
      );
    }

    return Container(
      width: double.infinity,
      margin: const EdgeInsets.fromLTRB(16, 12, 16, 0),
      padding: const EdgeInsets.all(12),
      decoration: BoxDecoration(
        color: Colors.grey.withOpacity(0.08),
        borderRadius: BorderRadius.circular(14),
        border: Border.all(
          color: Colors.grey.withOpacity(0.15),
        ),
      ),
      child: const Row(
        children: [
          Icon(Icons.edit_note, color: Colors.black54),
          SizedBox(width: 8),
          Expanded(
            child: Text(
              '이 매장은 수동 관리 매장입니다. 상품을 직접 추가/수정/삭제할 수 있습니다.',
              style: TextStyle(
                color: Colors.black54,
                fontWeight: FontWeight.w600,
              ),
            ),
          ),
        ],
      ),
    );
  }

  @override
  Widget build(BuildContext context) {
    final theme = Theme.of(context);

    return Scaffold(
      appBar: AppBar(
        title: Text(widget.storeName),
      ),
      floatingActionButton: isManualStore
          ? FloatingActionButton(
              onPressed: _showAddItemDialog,
              child: const Icon(Icons.add),
            )
          : null,
      body: Column(
        children: [
          _buildStoreTypeBanner(),
          Padding(
            padding: const EdgeInsets.fromLTRB(16, 16, 16, 8),
            child: TextField(
              controller: _searchController,
              onChanged: (value) {
                setState(() {
                  _searchText = value;
                });
              },
              decoration: InputDecoration(
                hintText: '상품명 또는 재고 상태 검색',
                prefixIcon: const Icon(Icons.search),
                suffixIcon: _searchText.isNotEmpty
                    ? IconButton(
                        onPressed: () {
                          _searchController.clear();
                          setState(() {
                            _searchText = '';
                          });
                        },
                        icon: const Icon(Icons.close),
                      )
                    : null,
              ),
            ),
          ),
          Expanded(
            child: StreamBuilder<QuerySnapshot<Map<String, dynamic>>>(
              stream: _itemsRef.orderBy('name').snapshots(),
              builder: (context, snapshot) {
                if (snapshot.hasError) {
                  return Center(
                    child: Padding(
                      padding: const EdgeInsets.all(24),
                      child: Text(
                        '재고를 불러오는 중 오류가 발생했습니다.\n${snapshot.error}',
                        textAlign: TextAlign.center,
                      ),
                    ),
                  );
                }

                if (snapshot.connectionState == ConnectionState.waiting) {
                  return const Center(child: CircularProgressIndicator());
                }

                if (!snapshot.hasData || snapshot.data!.docs.isEmpty) {
                  return _buildEmptyState(
                    icon: Icons.inventory_2_outlined,
                    title: '등록된 상품이 없습니다.',
                    subtitle: isAutoStore
                        ? '자동 동기화 후 여기에 상품이 표시됩니다.'
                        : '오른쪽 아래 + 버튼으로 상품을 추가해보세요.',
                  );
                }

                final allDocs = snapshot.data!.docs;
                final filteredDocs = _filterItems(allDocs);

                if (filteredDocs.isEmpty) {
                  return _buildEmptyState(
                    icon: Icons.search_off,
                    title: '검색 결과가 없습니다.',
                    subtitle: '다른 상품명이나 상태로 검색해보세요.',
                  );
                }

                return ListView.builder(
                  padding: const EdgeInsets.fromLTRB(16, 8, 16, 24),
                  itemCount: filteredDocs.length,
                  itemBuilder: (context, index) {
                    final doc = filteredDocs[index];
                    final data = doc.data();

                    final itemId = doc.id;
                    final itemName = (data['name'] ?? '이름 없음').toString();
                    final quantity = ((data['quantity'] ?? 0) as num).toInt();
                    final restocked = (data['restocked'] ?? false) as bool;
                    final stockStatus = (data['stockStatus'] ?? '').toString();
                    final lastSyncedAt = data['lastSyncedAt'] as Timestamp?;
                    final isSoldOut = data['isSoldOut'] != null
                        ? data['isSoldOut'] as bool
                        : quantity == 0;

                    return FutureBuilder<bool>(
                      future: _isFavorite(itemId),
                      builder: (context, favoriteSnapshot) {
                        final isFavorite = favoriteSnapshot.data ?? false;

                        return Card(
                          margin: const EdgeInsets.only(bottom: 14),
                          child: Padding(
                            padding: const EdgeInsets.all(16),
                            child: Column(
                              crossAxisAlignment: CrossAxisAlignment.start,
                              children: [
                                Row(
                                  children: [
                                    Container(
                                      width: 56,
                                      height: 56,
                                      decoration: BoxDecoration(
                                        color: theme.colorScheme.primary
                                            .withOpacity(0.12),
                                        borderRadius: BorderRadius.circular(16),
                                      ),
                                      child: Icon(
                                        Icons.inventory_2,
                                        color: theme.colorScheme.primary,
                                      ),
                                    ),
                                    const SizedBox(width: 14),
                                    Expanded(
                                      child: Column(
                                        crossAxisAlignment:
                                            CrossAxisAlignment.start,
                                        children: [
                                          Text(
                                            itemName,
                                            style: const TextStyle(
                                              fontSize: 17,
                                              fontWeight: FontWeight.bold,
                                            ),
                                          ),
                                          const SizedBox(height: 6),
                                          Text(
                                            '수량: $quantity개',
                                            style: TextStyle(
                                              fontSize: 14,
                                              color: Colors.grey[700],
                                            ),
                                          ),
                                        ],
                                      ),
                                    ),
                                    IconButton(
                                      onPressed: () {
                                        _toggleFavorite(
                                          itemId: itemId,
                                          itemName: itemName,
                                          quantity: quantity,
                                          isSoldOut: isSoldOut,
                                          restocked: restocked,
                                          stockStatus: stockStatus,
                                        );
                                      },
                                      icon: Icon(
                                        isFavorite
                                            ? Icons.favorite
                                            : Icons.favorite_border,
                                        color: isFavorite ? Colors.red : null,
                                      ),
                                    ),
                                  ],
                                ),
                                const SizedBox(height: 14),
                                Wrap(
                                  spacing: 8,
                                  runSpacing: 8,
                                  children: [
                                    Container(
                                      padding: const EdgeInsets.symmetric(
                                        horizontal: 10,
                                        vertical: 6,
                                      ),
                                      decoration: BoxDecoration(
                                        color: _statusColor(
                                          isSoldOut,
                                          restocked,
                                        ).withOpacity(0.12),
                                        borderRadius: BorderRadius.circular(30),
                                      ),
                                      child: Text(
                                        _statusText(
                                          isSoldOut,
                                          restocked,
                                          stockStatus,
                                        ),
                                        style: TextStyle(
                                          fontWeight: FontWeight.w600,
                                          color: _statusColor(
                                            isSoldOut,
                                            restocked,
                                          ),
                                        ),
                                      ),
                                    ),
                                  ],
                                ),
                                const SizedBox(height: 12),
                                Row(
                                  crossAxisAlignment: CrossAxisAlignment.start,
                                  children: [
                                    Icon(
                                      Icons.sync,
                                      size: 16,
                                      color: Colors.grey[600],
                                    ),
                                    const SizedBox(width: 6),
                                    Expanded(
                                      child: Text(
                                        '마지막 동기화: ${_formatTimestamp(lastSyncedAt)}',
                                        style: TextStyle(
                                          fontSize: 13,
                                          color: Colors.grey[600],
                                        ),
                                      ),
                                    ),
                                  ],
                                ),
                                if (isManualStore) ...[
                                  const SizedBox(height: 14),
                                  Row(
                                    children: [
                                      Expanded(
                                        child: OutlinedButton.icon(
                                          onPressed: () {
                                            _showEditItemDialog(
                                              itemId: itemId,
                                              currentName: itemName,
                                              currentQuantity: quantity,
                                              currentRestocked: restocked,
                                            );
                                          },
                                          icon: const Icon(Icons.edit_outlined),
                                          label: const Text('수정'),
                                        ),
                                      ),
                                      const SizedBox(width: 10),
                                      Expanded(
                                        child: OutlinedButton.icon(
                                          onPressed: () {
                                            _showDeleteDialog(
                                              itemId: itemId,
                                              itemName: itemName,
                                            );
                                          },
                                          icon:
                                              const Icon(Icons.delete_outline),
                                          label: const Text('삭제'),
                                          style: OutlinedButton.styleFrom(
                                            foregroundColor: Colors.red,
                                            side: const BorderSide(
                                              color: Colors.red,
                                            ),
                                          ),
                                        ),
                                      ),
                                    ],
                                  ),
                                ],
                              ],
                            ),
                          ),
                        );
                      },
                    );
                  },
                );
              },
            ),
          ),
        ],
      ),
    );
  }

  Widget _buildEmptyState({
    required IconData icon,
    required String title,
    required String subtitle,
  }) {
    return Center(
      child: Padding(
        padding: const EdgeInsets.all(24),
        child: Column(
          mainAxisSize: MainAxisSize.min,
          children: [
            Icon(
              icon,
              size: 72,
              color: Colors.grey[400],
            ),
            const SizedBox(height: 16),
            Text(
              title,
              style: const TextStyle(
                fontSize: 18,
                fontWeight: FontWeight.bold,
              ),
              textAlign: TextAlign.center,
            ),
            const SizedBox(height: 8),
            Text(
              subtitle,
              textAlign: TextAlign.center,
              style: TextStyle(
                fontSize: 14,
                color: Colors.grey[600],
              ),
            ),
          ],
        ),
      ),
    );
  }
}