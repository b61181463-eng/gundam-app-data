class RestockPrediction {
  final int score;
  final String label;
  final String reason;

  const RestockPrediction({
    required this.score,
    required this.label,
    required this.reason,
  });
}

class RestockPredictionService {
  static RestockPrediction predict({
    required String status,
    required int? price,
    DateTime? updatedAt,
    int reportCount = 0,
    int historyCount = 0,
  }) {
    int score = 0;

    if (status == '품절') {
      score += 25;
    }
    if (status == '상태 확인중') {
      score += 10;
    }
    if (status == '판매중') {
      score -= 20;
    }
    if (status == '예약중' || status == '입고예정') {
      score += 35;
    }

    if (price != null && price > 0) {
      score += 10;
    }

    if (updatedAt != null) {
      final days = DateTime.now().difference(updatedAt).inDays;
      if (days <= 1) {
        score += 20;
      } else if (days <= 3) {
        score += 15;
      } else if (days <= 7) {
        score += 10;
      } else if (days >= 30) {
        score -= 10;
      }
    }

    if (reportCount >= 3) {
      score += 20;
    } else if (reportCount >= 1) {
      score += 10;
    }

    if (historyCount >= 5) {
      score += 15;
    } else if (historyCount >= 2) {
      score += 8;
    }

    score = score.clamp(0, 100);

    if (status == '판매중') {
      return RestockPrediction(
        score: score,
        label: '현재 판매중',
        reason: '현재 구매 가능한 상품입니다.',
      );
    }

    if (score >= 70) {
      return RestockPrediction(
        score: score,
        label: '입고 가능성 높음',
        reason: '최근 기록과 제보 기준으로 재입고 가능성이 높습니다.',
      );
    }

    if (score >= 40) {
      return RestockPrediction(
        score: score,
        label: '입고 가능성 보통',
        reason: '일부 기록은 있지만 확실한 패턴은 부족합니다.',
      );
    }

    return RestockPrediction(
      score: score,
      label: '입고 가능성 낮음',
      reason: '최근 입고/제보 기록이 부족합니다.',
    );
  }
}
