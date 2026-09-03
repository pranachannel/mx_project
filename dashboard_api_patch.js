/*
 * ProjectMX dashboard encrypted-summary adapter
 *
 * 목적:
 * 1) 기존 index.html의 시간별 R2 JSON fetch를 실제 네트워크 요청 없이 가로채고,
 *    dc-data-api의 암호화 summary API 한 번으로 대체합니다.
 * 2) 기존 Parquet 직접 요청을 차단합니다.
 * 3) 아직 encrypted details API가 없으므로 최근 게시글/댓글 패널은 임시 비활성화합니다.
 *
 * 이 파일은 기존 index.html의 큰 렌더링/검색/그래프 로직을 그대로 보존하기 위한
 * 호환 레이어입니다. index.html의 기존 inline <script> 뒤, </body> 직전에 로드하세요.
 */
(() => {
  "use strict";

  const DATA_API_BASE = "https://dc-data-api.aropura.workers.dev";
  const LEGACY_R2_HOST = "pub-fca2435a027e41bf954b4bc4f46e560a.r2.dev";

  const nativeFetch = window.fetch.bind(window);
  const summaryPromiseCache = new Map();

  function inputUrl(input) {
    if (typeof input === "string") return input;
    if (input instanceof URL) return input.href;
    if (input && typeof input.url === "string") return input.url;
    return String(input || "");
  }

  function parseLegacyHourlyJson(rawUrl) {
    let url;
    try {
      url = new URL(rawUrl, window.location.href);
    } catch (_) {
      return null;
    }

    if (url.hostname !== LEGACY_R2_HOST) return null;

    // Current: /YYYY-MM-DD/13h/13h.json
    // Legacy:  /YYYY-MM-DD/13h.json
    const current = url.pathname.match(
      /^\/(\d{4}-\d{2}-\d{2})\/(\d{2})h\/\2h\.json$/
    );
    const legacy = url.pathname.match(
      /^\/(\d{4}-\d{2}-\d{2})\/(\d{2})h\.json$/
    );
    const match = current || legacy;
    if (!match) return null;

    const hour = Number(match[2]);
    if (!Number.isInteger(hour) || hour < 0 || hour > 23) return null;

    return {
      date: match[1],
      hour,
      refresh: url.searchParams.get("refresh") || "",
    };
  }

  function isLegacyParquet(rawUrl) {
    let url;
    try {
      url = new URL(rawUrl, window.location.href);
    } catch (_) {
      return false;
    }

    if (url.hostname !== LEGACY_R2_HOST) return false;
    return /\/(?:posts|comments)\.parquet$/i.test(url.pathname);
  }

  async function getSummary(dateStr, refreshToken = "") {
    // 기존 fetchDailyData()가 동시에 최대 24개 fetch를 호출해도
    // 실제 Worker 요청은 날짜/refresh 조합당 딱 1번만 발생합니다.
    const cacheKey = `${dateStr}|${refreshToken || "normal"}`;
    if (summaryPromiseCache.has(cacheKey)) {
      return summaryPromiseCache.get(cacheKey);
    }

    const promise = (async () => {
      const apiUrl = new URL("/api/day", DATA_API_BASE);
      apiUrl.searchParams.set("date", dateStr);
      if (refreshToken) apiUrl.searchParams.set("refresh", refreshToken);

      const response = await nativeFetch(apiUrl.href, {
        method: "GET",
        cache: refreshToken ? "no-store" : "default",
        credentials: "omit",
      });

      if (response.status === 404) return null;
      if (!response.ok) {
        let reason = "";
        try {
          const body = await response.json();
          reason = body?.code || body?.message || "";
        } catch (_) {}
        throw new Error(
          `대시보드 API 요청 실패 (${response.status})${reason ? `: ${reason}` : ""}`
        );
      }

      const summary = await response.json();
      if (!summary || Number(summary.version) !== 1 || !Array.isArray(summary.hours)) {
        throw new Error("대시보드 API summary 형식이 올바르지 않습니다.");
      }
      return summary;
    })();

    summaryPromiseCache.set(cacheKey, promise);
    try {
      return await promise;
    } catch (error) {
      // 일시 오류는 다음 시도에서 다시 요청할 수 있도록 제거합니다.
      summaryPromiseCache.delete(cacheKey);
      throw error;
    }
  }

  function syntheticJsonResponse(value, status = 200) {
    return new Response(value === null ? "" : JSON.stringify(value), {
      status,
      headers: {
        "Content-Type": "application/json; charset=utf-8",
        "Cache-Control": "no-store",
        "X-Dashboard-Adapter": "encrypted-summary",
      },
    });
  }

  window.fetch = async function dashboardFetchAdapter(input, init) {
    const rawUrl = inputUrl(input);

    const hourly = parseLegacyHourlyJson(rawUrl);
    if (hourly) {
      try {
        const summary = await getSummary(hourly.date, hourly.refresh);
        if (!summary) return syntheticJsonResponse(null, 404);

        const hourData = summary.hours.find(
          (item) => Number(item?.hour) === hourly.hour
        );
        if (!hourData) return syntheticJsonResponse(null, 404);

        // 기존 index.html은 시간별 JSON의 최상위 배열(UserData[])을 기대하므로,
        // summary.hours[n].users만 같은 형식의 가상 Response로 돌려줍니다.
        return syntheticJsonResponse(
          Array.isArray(hourData.users) ? hourData.users : [],
          200
        );
      } catch (error) {
        console.error("encrypted summary adapter failed:", error);
        return syntheticJsonResponse(null, 503);
      }
    }

    // 원본 Parquet가 브라우저 Network에 노출되지 않도록 직접 fetch를 차단합니다.
    if (isLegacyParquet(rawUrl)) {
      console.warn("직접 Parquet 요청이 차단되었습니다:", rawUrl);
      return new Response("", {
        status: 410,
        statusText: "Direct Parquet Access Disabled",
        headers: {
          "Cache-Control": "no-store",
          "X-Dashboard-Data-Policy": "direct-parquet-disabled",
        },
      });
    }

    return nativeFetch(input, init);
  };

  // 기존 사용자 모달의 시간별 게시글/댓글 그래프는 summary만으로 그대로 동작합니다.
  // 최근 본문 5/10개는 Parquet를 직접 읽지 않도록 임시 비활성화합니다.
  // 다음 단계의 encrypted details API가 준비되면 이 함수만 API 호출로 교체하면 됩니다.
  if (typeof window.loadRecentUserActivity === "function") {
    window.loadRecentUserActivity = async function encryptedDetailsPending(
      user,
      requestSequence
    ) {
      try {
        if (
          typeof modalLoadSequence !== "undefined" &&
          requestSequence !== modalLoadSequence
        ) {
          return;
        }
      } catch (_) {}

      if (typeof setActivityState === "function") {
        setActivityState(
          "recent-posts",
          "최근 게시글은 암호화 details API 전환 후 다시 표시됩니다."
        );
        setActivityState(
          "recent-comments",
          "최근 댓글은 암호화 details API 전환 후 다시 표시됩니다."
        );
      }
    };
  }

  // 새로고침 버튼은 같은 날짜의 이전 Promise를 재사용하지 않도록
  // 기존 fetchDailyData가 부여하는 refresh token으로 자동 분리됩니다.
  console.info(
    "[ProjectMX] encrypted summary adapter enabled:",
    DATA_API_BASE
  );
})();
