import 'package:flutter_test/flutter_test.dart';
import 'package:gundam_app/main.dart';

void main() {
  testWidgets('GundamApp builds', (WidgetTester tester) async {
    await tester.pumpWidget(const GundamApp());
    expect(find.byType(GundamApp), findsOneWidget);
  });
}
