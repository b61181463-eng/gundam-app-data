const express = require("express");
const cors = require("cors");
const puppeteer = require("puppeteer");
const admin = require("firebase-admin");
const serviceAccount = require("./serviceAccountKey.json");

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
});

const db = admin.firestore();

const app = express();
app.use(cors());

const PORT = 3000;
const COUNTRY = "KR";
const SYNC_INTERVAL_MS = 60 * 1000;
const MAX_DETAIL_ITEMS = 20;
const MAX_RETRIES = 3;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function retry(fn, retries = MAX_RETRIES, delayMs = 1500) {
  let lastError;

  for (let i = 0; i < retries; i++) {
    try {
      return await fn();
    } catch (e) {
      lastError = e;
      console.log(`재시도 ${i + 1}/${retries} 실패: ${e.message}`);
      if (i < retries - 1) {
        await sleep(delayMs);
      }
    }
  }

  throw lastError;
}

function normalizeName(name) {
  return (name || "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .replace(/[^a-z0-9가-힣/\-\s().[\]]/g, "")
    .trim();
}

function normalizeStockStatus(rawText) {
  const text = (rawText || "").toLowerCase().trim();

  if (!text) return "unknown";

  const outKeywords = [
    "품절",
    "일시품절",
    "재고 없음",
    "out of stock",
    "sold out",
    "currently unavailable",
    "notify me",
    "입고예정",
    "판매 종료",
  ];

  const inKeywords = [
    "장바구니",
    "바로구매",
    "구매하기",
    "주문하기",
    "in stock",
    "available",
    "판매중",
    "구매 가능",
    "재고 있음",
  ];

  if (outKeywords.some((keyword) => text.includes(keyword))) {
    return "out_of_stock";
  }

  if (inKeywords.some((keyword) => text.includes(keyword))) {
    return "in_stock";
  }

  return "unknown";
}

function makeProductId(site, country, productUrl) {
  const raw = `${country}_${site}_${productUrl}`;
  return Buffer.from(raw).toString("base64").replace(/[/+=]/g, "").slice(0, 40);
}

function compareStockStatus(oldStatus, newStatus) {
  if (oldStatus === newStatus) {
    return {
      changed: false,
      oldStatus,
      newStatus,
      eventType: "none",
    };
  }

  let eventType = "none";

  if (oldStatus === "out_of_stock" && newStatus === "in_stock") {
    eventType = "restock";
  } else if (oldStatus === "in_stock" && newStatus === "out_of_stock") {
    eventType = "soldout";
  } else if (oldStatus === "unknown" && newStatus === "in_stock") {
    eventType = "new_stock";
  }

  return {
    changed: true,
    oldStatus,
    newStatus,
    eventType,
  };
}

function isGunplaProductName(name) {
  const text = (name || "").toLowerCase();

  const includeKeywords = [
    "건담",
    "건프라",
    "gundam",
    "gunpla",
    "hg ",
    "hguc",
    "rg ",
    "mg ",
    "mgex",
    "pg ",
    "sd ",
    "sdcs",
    "entry grade",
    "full mechanics",
    "유니콘",
    "바르바토스",
    "에어리얼",
    "자쿠",
    "사자비",
    "뉴 건담",
    "하이뉴",
    "시난주",
    "밴시",
    "캘리번",
    "루브리스",
    "프리덤",
    "저스티스",
    "스트라이크",
    "윙 건담",
    "데스사이즈",
    "엑시아",
    "다이너메스",
    "버체",
    "큐리오스",
    "하로",
  ];

  const excludeKeywords = [
    "포켓몬",
    "원피스",
    "드래곤볼",
    "나루토",
    "디지몬",
    "에반게리온",
    "마크로스",
    "transformers",
    "스타워즈",
    "hello kitty",
    "마블",
    "zoids",
    "30ms",
    "30mm",
    "가면라이더",
    "울트라맨",
    "다마고치",
  ];

  const hasInclude = includeKeywords.some((keyword) => text.includes(keyword));
  const hasExclude = excludeKeywords.some((keyword) => text.includes(keyword));

  return hasInclude && !hasExclude;
}

async function createBrowser() {
  return puppeteer.launch({
    headless: true,
    args: ["--no-sandbox", "--disable-setuid-sandbox"],
    defaultViewport: {
      width: 1400,
      height: 1200,
    },
  });
}

async function preparePage(page) {
  await page.setUserAgent(
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
  );
  await page.setExtraHTTPHeaders({
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
  });
}

async function safeGoto(page, url) {
  await retry(async () => {
    await page.goto(url, {
      waitUntil: "domcontentloaded",
      timeout: 30000,
    });
  }, 3, 1500);

  await sleep(2500);
}

async function extractCandidateLinks(page) {
  const rawItems = await page.evaluate(() => {
    const results = [];
    const seen = new Set();

    const anchors = Array.from(document.querySelectorAll("a[href]"));

    for (const el of anchors) {
      const name = el.innerText?.trim();
      const href = el.href;

      if (!name || !href) continue;
      if (name.length < 4) continue;
      if (!/^https?:\/\//i.test(href)) continue;

      const isDetailUrl =
        href.includes("bnkrmall.co.kr/goods/detail.do") ||
        href.includes("bnkrmall.co.kr/mw/goods/detail.do");

      if (!isDetailUrl) continue;
      if (seen.has(href)) continue;

      seen.add(href);

      results.push({
        name,
        productUrl: href,
      });
    }

    return results;
  });

  const filtered = rawItems.filter((item) => isGunplaProductName(item.name));

  const uniqueMap = new Map();
  for (const item of filtered) {
    if (!uniqueMap.has(item.productUrl)) {
      uniqueMap.set(item.productUrl, item);
    }
  }

  return Array.from(uniqueMap.values());
}

async function extractDetailInfo(browser, item) {
  const page = await browser.newPage();

  try {
    await preparePage(page);
    await safeGoto(page, item.productUrl);

    const detail = await page.evaluate(() => {
      const pickText = (selectors) => {
        for (const selector of selectors) {
          const el = document.querySelector(selector);
          if (el && el.innerText?.trim()) {
            return el.innerText.trim();
          }
        }
        return "";
      };

      const title = pickText([
        "h1",
        ".goods-name",
        ".product-name",
        ".item-name",
        ".prd_name",
      ]) || document.title?.trim() || "";

      const bodyText = document.body?.innerText?.toLowerCase() || "";

      const stockArea = [
        ".btn-area",
        ".purchase-area",
        ".goods-btn",
        ".goods-buy",
        ".btnWrap",
        ".order-btn",
      ]
        .map((selector) => document.querySelector(selector)?.innerText || "")
        .join(" ")
        .toLowerCase();

      const combinedText = `${bodyText}\n${stockArea}`.toLowerCase();

      let rawStockText = "unknown";

      if (
        combinedText.includes("품절") ||
        combinedText.includes("일시품절") ||
        combinedText.includes("재고 없음") ||
        combinedText.includes("out of stock") ||
        combinedText.includes("sold out")
      ) {
        rawStockText = "Out of Stock";
      } else if (
        combinedText.includes("장바구니") ||
        combinedText.includes("바로구매") ||
        combinedText.includes("구매하기") ||
        combinedText.includes("주문하기")
      ) {
        rawStockText = "In Stock";
      }

      return {
        title,
        rawStockText,
      };
    });

    const finalName = detail.title || item.name;

    if (!isGunplaProductName(finalName)) {
      return null;
    }

    return {
      site: "BNKR Mall",
      country: COUNTRY,
      name: finalName,
      productUrl: item.productUrl,
      imageUrl: "https://picsum.photos/200",
      rawStockText: detail.rawStockText,
    };
  } catch (e) {
    console.log("[KR] 상세 페이지 실패:", item.productUrl, e.message);
    return null;
  } finally {
    await page.close();
  }
}

async function crawlBnkrMall() {
  const browser = await createBrowser();

  try {
    const page = await browser.newPage();
    await preparePage(page);

    const urls = [
      "https://m.bnkrmall.co.kr/mw/goods/category.do?brandIdx=181&cate=1576&cateName=%EA%B1%B4%ED%94%84%EB%9D%BC&endGoods=Y&page=1&soldout=Y",
      "https://m.bnkrmall.co.kr/mw/goods/new.do?endGoods=Y",
    ];

    const mergedMap = new Map();

    for (const url of urls) {
      try {
        await safeGoto(page, url);
        const candidates = await extractCandidateLinks(page);

        for (const item of candidates) {
          if (!mergedMap.has(item.productUrl)) {
            mergedMap.set(item.productUrl, item);
          }
        }
      } catch (e) {
        console.log("[KR] 목록 페이지 실패:", url, e.message);
      }
    }

    const mergedCandidates = Array.from(mergedMap.values()).slice(0, MAX_DETAIL_ITEMS);
    const finalItems = [];

    for (const item of mergedCandidates) {
      const detail = await extractDetailInfo(browser, item);
      if (detail) {
        finalItems.push(detail);
      }
      await sleep(400);
    }

    return finalItems;
  } finally {
    await browser.close();
  }
}

async function sendNotificationsToUsers(userIds, title, body, data = {}) {
  if (!userIds || userIds.length === 0) return;

  const uniqueUserIds = [...new Set(userIds)];
  const tokens = [];

  for (const userId of uniqueUserIds) {
    const userSnap = await db.collection("users").doc(userId).get();
    if (!userSnap.exists) continue;

    const userData = userSnap.data();
    const token = userData.fcmToken;

    if (token && typeof token === "string" && token.trim().length > 0) {
      tokens.push(token);
    }
  }

  if (tokens.length === 0) {
    console.log("알림 대상 토큰 없음");
    return;
  }

  const message = {
    notification: {
      title,
      body,
    },
    data: Object.fromEntries(
      Object.entries(data).map(([k, v]) => [String(k), String(v)])
    ),
    tokens,
  };

  try {
    const response = await admin.messaging().sendEachForMulticast(message);
    console.log(`푸시 전송 완료: 성공 ${response.successCount}, 실패 ${response.failureCount}`);

    response.responses.forEach((r, index) => {
      if (!r.success) {
        console.log(`토큰 실패 [${index}]:`, r.error?.message);
      }
    });
  } catch (e) {
    console.error("푸시 전송 실패:", e.message);
  }
}

async function processStockItem(item) {
  const productRef = db.collection("products").doc(item.productId);
  const productSnap = await productRef.get();

  let oldStatus = "unknown";

  if (productSnap.exists) {
    const data = productSnap.data();
    oldStatus = data.latestStockStatus || "unknown";
  }

  const result = compareStockStatus(oldStatus, item.stockStatus);

  await productRef.set(
    {
      productId: item.productId,
      site: item.site,
      country: item.country,
      name: item.name,
      normalizedName: item.normalizedName,
      productUrl: item.productUrl,
      imageUrl: item.imageUrl,
      latestStockStatus: item.stockStatus,
      latestRawStockText: item.rawStockText,
      lastCheckedAt: admin.firestore.FieldValue.serverTimestamp(),
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    },
    { merge: true }
  );

  await db.collection("stock_history").add({
    productId: item.productId,
    site: item.site,
    country: item.country,
    name: item.name,
    checkedAt: admin.firestore.FieldValue.serverTimestamp(),
    stockStatus: item.stockStatus,
    rawStockText: item.rawStockText,
  });

  if (result.changed && result.eventType !== "none") {
    const watchSnapshot = await db
      .collection("watchlists")
      .where("productId", "==", item.productId)
      .get();

    const targetUserIds = watchSnapshot.docs.map((doc) => doc.data().userId);

    const duplicateCheckFrom = new Date(Date.now() - 30 * 60 * 1000);

    const duplicateSnapshot = await db
      .collection("restock_events")
      .where("productId", "==", item.productId)
      .where("type", "==", result.eventType)
      .where("fromStatus", "==", result.oldStatus)
      .where("toStatus", "==", result.newStatus)
      .where("detectedAt", ">=", duplicateCheckFrom)
      .limit(1)
      .get();

    const isDuplicate = !duplicateSnapshot.empty;

    if (!isDuplicate) {
      await db.collection("restock_events").add({
        productId: item.productId,
        site: item.site,
        country: item.country,
        name: item.name,
        fromStatus: result.oldStatus,
        toStatus: result.newStatus,
        type: result.eventType,
        targetUserIds,
        detectedAt: admin.firestore.FieldValue.serverTimestamp(),
        isRead: false,
      });

      if (result.eventType === "restock" && targetUserIds.length > 0) {
        await sendNotificationsToUsers(
          targetUserIds,
          "재입고 알림",
          `[KR] ${item.name} 재입고됨`,
          {
            type: "restock",
            country: item.country,
            productId: item.productId,
            productName: item.name,
            site: item.site,
          }
        );
      }
    }
  }
}

async function runSync() {
  console.log("🌐 KR 크롤링 시작");

  try {
    const crawledItems = await retry(() => crawlBnkrMall(), 2, 2000);
    console.log("📦 KR 크롤링 개수:", crawledItems.length);

    let successCount = 0;
    let unknownCount = 0;

    for (const raw of crawledItems) {
      const normalizedName = normalizeName(raw.name);
      const stockStatus = normalizeStockStatus(raw.rawStockText);
      const productId = makeProductId(raw.site, raw.country, raw.productUrl);

      if (stockStatus === "unknown") {
        unknownCount++;
      }

      await processStockItem({
        productId,
        site: raw.site,
        country: raw.country,
        name: raw.name,
        normalizedName,
        productUrl: raw.productUrl,
        imageUrl: raw.imageUrl,
        rawStockText: raw.rawStockText,
        stockStatus,
      });

      successCount++;
    }

    console.log("✅ KR Firestore 동기화 완료");
    console.log(`✅ 저장 성공: ${successCount}`);
    console.log(`⚠️ 재고 unknown: ${unknownCount}`);

    return {
      ok: true,
      count: crawledItems.length,
      successCount,
      unknownCount,
    };
  } catch (e) {
    console.error("🔥 KR 동기화 실패:", e.message);
    return { ok: false, error: e.message };
  }
}

app.get("/crawl", async (req, res) => {
  try {
    const items = await crawlBnkrMall();
    res.json(items);
  } catch (e) {
    console.error("🔥 KR 크롤링 에러:", e.message);
    res.status(500).send(e.message);
  }
});

app.get("/sync", async (req, res) => {
  const result = await runSync();
  if (result.ok) {
    res.json(result);
  } else {
    res.status(500).json(result);
  }
});

app.listen(PORT, "0.0.0.0", async () => {
  console.log(`KR Crawler running on http://localhost:${PORT}`);
  console.log(`KR Sync URL: http://localhost:${PORT}/sync`);

  await runSync();

  setInterval(async () => {
    await runSync();
  }, SYNC_INTERVAL_MS);
});