const axios = require("axios");
const cheerio = require("cheerio");
const crypto = require("crypto");
const admin = require("firebase-admin");
const functions = require("firebase-functions");

admin.initializeApp();
const db = admin.firestore();

const REGION = "asia-northeast3";

/**
 * 1차 전략
 * - 건담베이스 "입고 예정 공지" 수집
 * - 현재는 상품형 재고보다 공지형 데이터에 맞춤
 * - 원문 구조가 자주 바뀔 수 있어 파서 실패 시 rawText도 함께 저장
 *
 * 사용 방법:
 * 1) 수동 실행:
 *    firebase deploy --only functions
 *    배포 후 HTTPS URL 호출
 *
 * 2) 앱/브라우저에서:
 *    https://<region>-<project-id>.cloudfunctions.net/fetchKRGundamBaseNotices
 *
 * 나중에:
 * - Scheduler 붙여 5~15분 간격 자동 수집
 * - stock_events 컬렉션으로 변화 기록
 */

// 1차 수집 대상 URL
// 접근 가능한 공개 페이지를 우선 사용.
// 공식 카페/인스타는 자동 수집 안정성이 낮아서 1차는 공개 기사/미러 기반으로 시작.
const SOURCE_URLS = [
  "https://m.ruliweb.com/news/board/1002/read/2414882",
  "https://m.ruliweb.com/news/board/1002/read/2414884"
];

// 공지 제목에서 건담베이스 관련인지 확인
function isRelevantTitle(title = "") {
  const t = title.toLowerCase();
  return (
    t.includes("건") &&
    t.includes("베이스")
  );
}

// 공지 제목에서 날짜 뽑기
function extractNoticeDate(text = "") {
  // 예: 2026년 3월 7일 / 3월 7일
  const full = text.match(/(20\d{2})\s*년\s*(\d{1,2})\s*월\s*(\d{1,2})\s*일/);
  if (full) {
    const [, y, m, d] = full;
    return `${y}-${String(m).padStart(2, "0")}-${String(d).padStart(2, "0")}`;
  }

  const partial = text.match(/(\d{1,2})\s*월\s*(\d{1,2})\s*일/);
  if (partial) {
    const year = new Date().getFullYear();
    const [, m, d] = partial;
    return `${year}-${String(m).padStart(2, "0")}-${String(d).padStart(2, "0")}`;
  }

  return null;
}

function normalizeSpace(str = "") {
  return str.replace(/\s+/g, " ").trim();
}

function sha1(input) {
  return crypto.createHash("sha1").update(input).digest("hex");
}

// 공지 본문에서 상품 라인 추출
function extractProductsFromText(rawText = "") {
  const text = rawText
    .replace(/\r/g, "\n")
    .replace(/⠀/g, " ")
    .replace(/\t/g, " ")
    .replace(/\u00a0/g, " ");

  const results = [];
  const lines = text
    .split("\n")
    .map((line) => normalizeSpace(line))
    .filter(Boolean);

  for (const line of lines) {
    // 예시:
    // ✔ HG 구스타프 칼 00형(1인 1개 한정)
    // - MG XXX
    // • RG XXX
    const cleaned = line
      .replace(/^[✔✓•·\-\*]\s*/, "")
      .trim();

    if (!cleaned) continue;

    // 너무 긴 설명문 제외
    if (cleaned.length < 3 || cleaned.length > 120) continue;

    // 공지성 문장 제외
    const excludedKeywords = [
      "유의사항",
      "입고 예정일",
      "매장별",
      "공식 카페",
      "확인 부탁",
      "판매 방식",
      "양해",
      "책임지지",
      "한정",
      "불이익",
      "출처",
      "신상품이",
      "입고 될 예정",
      "발매 예정 상품",
    ];

    if (excludedKeywords.some((k) => cleaned.includes(k))) {
      continue;
    }

    // 제품명 보정
    const name = cleaned
      .replace(/\(.*?한정.*?\)/g, "")
      .replace(/\(.*?개 제한.*?\)/g, "")
      .replace(/\(.*?별매.*?\)/g, "")
      .trim();

    if (name.length < 2) continue;

    results.push({
      name,
      rawLine: line,
      limitText: line.includes("한정") ? line.match(/\(.*?한정.*?\)/)?.[0] || "" : "",
    });
  }

  // 중복 제거
  const seen = new Set();
  return results.filter((item) => {
    const key = item.name.toLowerCase();
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

async function fetchHtml(url) {
  const { data } = await axios.get(url, {
    timeout: 15000,
    headers: {
      "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
      "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8",
    },
  });

  return data;
}

async function parseNoticePage(url) {
  const html = await fetchHtml(url);
  const $ = cheerio.load(html);

  // 루리웹 모바일 페이지 기준 1차 파서
  const title =
    normalizeSpace($("title").first().text()) ||
    normalizeSpace($("h1").first().text()) ||
    normalizeSpace($("h2").first().text());

  const bodyText =
    normalizeSpace($("article").text()) ||
    normalizeSpace($(".view_content").text()) ||
    normalizeSpace($("#article").text()) ||
    normalizeSpace($("body").text());

  const imageUrl =
    $("meta[property='og:image']").attr("content") ||
    $("img").first().attr("src") ||
    "";

  const sourceLink =
    $("meta[property='og:url']").attr("content") || url;

  const noticeDate = extractNoticeDate(`${title}\n${bodyText}`);

  const productItems = extractProductsFromText(bodyText);

  return {
    title,
    bodyText,
    imageUrl,
    sourceLink,
    noticeDate,
    productItems,
  };
}

async function upsertNotice(parsed) {
  const noticeId = sha1(parsed.sourceLink);
  const noticeRef = db.collection("kr_notices").doc(noticeId);

  await noticeRef.set(
    {
      noticeId,
      source: "gundambase_notice",
      sourceType: "notice",
      site: "건담베이스",
      title: parsed.title,
      bodyText: parsed.bodyText,
      imageUrl: parsed.imageUrl,
      sourceUrl: parsed.sourceLink,
      noticeDate: parsed.noticeDate || null,
      country: "KR",
      region: "KR",
      status: "notice",
      itemCount: parsed.productItems.length,
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      lastSeenAt: admin.firestore.FieldValue.serverTimestamp(),
    },
    { merge: true }
  );

  return { noticeId, noticeRef };
}

async function upsertNoticeItems(noticeId, parsed) {
  const batch = db.batch();

  for (const item of parsed.productItems) {
    const itemId = sha1(`${noticeId}:${item.name.toLowerCase()}`);
    const docRef = db.collection("kr_notice_items").doc(itemId);

    batch.set(
      docRef,
      {
        itemId,
        noticeId,
        source: "gundambase_notice",
        sourceType: "notice_item",
        site: "건담베이스",
        mallName: "건담베이스",
        country: "KR",
        region: "KR",
        name: item.name,
        rawLine: item.rawLine,
        limitText: item.limitText,
        status: "입고 예정",
        stockText: "입고 예정",
        availability: "입고 예정",
        noticeDate: parsed.noticeDate || null,
        sourceUrl: parsed.sourceLink,
        imageUrl: parsed.imageUrl,
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        lastSeenAt: admin.firestore.FieldValue.serverTimestamp(),
      },
      { merge: true }
    );

    // 앱에서 바로 보이게 aggregated_items에도 저장
    const aggRef = db.collection("aggregated_items").doc(itemId);

    batch.set(
      aggRef,
      {
        itemId,
        source: "gundambase_notice",
        sourceType: "notice_item",
        site: "건담베이스",
        mallName: "건담베이스",
        country: "KR",
        region: "KR",
        name: item.name,
        title: item.name,
        price: "",
        stock: null,
        status: "입고 예정",
        stockText: "입고 예정",
        availability: "입고 예정",
        noticeDate: parsed.noticeDate || null,
        productUrl: parsed.sourceLink,
        url: parsed.sourceLink,
        link: parsed.sourceLink,
        imageUrl: parsed.imageUrl,
        lastChangedAt: admin.firestore.FieldValue.serverTimestamp(),
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        lastSeenAt: admin.firestore.FieldValue.serverTimestamp(),
      },
      { merge: true }
    );
  }

  await batch.commit();
}

async function writeStockEventsForChanges(parsed) {
  for (const item of parsed.productItems) {
    const itemId = sha1(`${sha1(parsed.sourceLink)}:${item.name.toLowerCase()}`);
    const aggRef = db.collection("aggregated_items").doc(itemId);
    const snap = await aggRef.get();

    const nextStatus = "입고 예정";
    const prevStatus = snap.exists ? snap.data().status || "" : "";

    if (!snap.exists || prevStatus !== nextStatus) {
      await db.collection("stock_events").add({
        itemId,
        name: item.name,
        site: "건담베이스",
        country: "KR",
        previousStatus: prevStatus || null,
        nextStatus,
        source: "gundambase_notice",
        sourceUrl: parsed.sourceLink,
        createdAt: admin.firestore.FieldValue.serverTimestamp(),
      });
    }
  }
}

async function collectOneUrl(url) {
  const parsed = await parseNoticePage(url);

  if (!parsed.title || !isRelevantTitle(parsed.title)) {
    return {
      url,
      ok: false,
      reason: "건담베이스 관련 공지로 판단되지 않음",
    };
  }

  const { noticeId } = await upsertNotice(parsed);
  await writeStockEventsForChanges(parsed);
  await upsertNoticeItems(noticeId, parsed);

  return {
    url,
    ok: true,
    title: parsed.title,
    noticeDate: parsed.noticeDate,
    itemCount: parsed.productItems.length,
    items: parsed.productItems.map((x) => x.name),
  };
}

// 수동 실행용 HTTPS 함수
exports.fetchKRGundamBaseNotices = functions
  .region(REGION)
  .https.onRequest(async (req, res) => {
    try {
      const results = [];

      for (const url of SOURCE_URLS) {
        try {
          const result = await collectOneUrl(url);
          results.push(result);
        } catch (err) {
          results.push({
            url,
            ok: false,
            reason: err.message || String(err),
          });
        }
      }

      res.status(200).json({
        ok: true,
        source: "gundambase_notice",
        count: results.length,
        results,
      });
    } catch (err) {
      console.error(err);
      res.status(500).json({
        ok: false,
        error: err.message || String(err),
      });
    }
  });

// 나중에 자동 수집용 스케줄 함수
// 결제/플랜 문제로 지금 바로 못 쓰면 배포만 해두고 나중에 켜면 됨.
exports.scheduledFetchKRGundamBaseNotices = functions
  .region(REGION)
  .pubsub.schedule("every 15 minutes")
  .timeZone("Asia/Seoul")
  .onRun(async () => {
    const results = [];

    for (const url of SOURCE_URLS) {
      try {
        const result = await collectOneUrl(url);
        results.push(result);
      } catch (err) {
        results.push({
          url,
          ok: false,
          reason: err.message || String(err),
        });
      }
    }

    console.log("scheduledFetchKRGundamBaseNotices results:", results);
    return null;
  });