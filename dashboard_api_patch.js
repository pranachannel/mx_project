/*
 * ProjectMX encrypted dashboard adapter v2.1
 * - 시간별 JSON -> encrypted summary API
 * - Parquet 직접 요청 차단
 * - 최근 게시글/댓글 -> 선택 날짜/시간대의 encrypted details API
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
    try { url = new URL(rawUrl, window.location.href); } catch (_) { return null; }
    if (url.hostname !== LEGACY_R2_HOST) return null;

    const current = url.pathname.match(/^\/(\d{4}-\d{2}-\d{2})\/(\d{2})h\/\2h\.json$/);
    const legacy = url.pathname.match(/^\/(\d{4}-\d{2}-\d{2})\/(\d{2})h\.json$/);
    const match = current || legacy;
    if (!match) return null;

    const hour = Number(match[2]);
    if (!Number.isInteger(hour) || hour < 0 || hour > 23) return null;
    return { date: match[1], hour, refresh: url.searchParams.get("refresh") || "" };
  }

  function isLegacyParquet(rawUrl) {
    let url;
    try { url = new URL(rawUrl, window.location.href); } catch (_) { return false; }
    return url.hostname === LEGACY_R2_HOST &&
      /\/(?:posts|comments)\.parquet$/i.test(url.pathname);
  }

  async function getSummary(dateStr, refreshToken = "") {
    const cacheKey = `${dateStr}|${refreshToken || "normal"}`;
    if (summaryPromiseCache.has(cacheKey)) return summaryPromiseCache.get(cacheKey);

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
        throw new Error(`대시보드 API 요청 실패 (${response.status})${reason ? `: ${reason}` : ""}`);
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
        const hourData = summary.hours.find((item) => Number(item?.hour) === hourly.hour);
        if (!hourData) return syntheticJsonResponse(null, 404);
        return syntheticJsonResponse(Array.isArray(hourData.users) ? hourData.users : [], 200);
      } catch (error) {
        console.error("encrypted summary adapter failed:", error);
        return syntheticJsonResponse(null, 503);
      }
    }

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

  function activityDate() {
    // 집계를 완료한 날짜를 우선 사용합니다. date-picker는 이전 GUI용입니다.
    return String(
      (typeof selectedDate !== "undefined" && selectedDate) ||
      document.getElementById("archive-date-input")?.value ||
      document.getElementById("date-picker")?.value ||
      (typeof kstToday === "function" ? kstToday() :
        (typeof todayKst !== "undefined" ? todayKst : ""))
    );
  }

  function activityHours() {
    const hours = typeof includedHours === "function"
      ? includedHours() : Array.from({ length: 24 }, (_, hour) => hour);
    return [...new Set(hours)].sort((a, b) => a - b);
  }

  function detailsErrorMessage(status, code, dateStr) {
    if (status === 404 && code === "DETAILS_SHARD_NOT_FOUND") {
      return `${dateStr}의 이 유저가 속한 상세 데이터 파일을 찾지 못했습니다. 시간대 집계와 상세 데이터는 별도로 저장됩니다. 해당 날짜의 상세 데이터 생성 상태를 확인해 주세요. (${code})`;
    }
    if (status === 404 && code === "DETAILS_NOT_FOUND") {
      return `${dateStr}의 상세 데이터 파일을 찾지 못했습니다. 해당 날짜의 상세 데이터 생성 상태를 확인해 주세요. (${code})`;
    }
    if (status === 404) {
      return `상세 조회 API를 찾지 못했습니다. 서버의 /api/user 경로와 배포 버전을 확인해 주세요. (404${code ? ` · ${code}` : ""})`;
    }
    return `최근 활동 데이터를 불러오지 못했습니다 (${status})${code ? `: ${code}` : ""}`;
  }

  async function loadEncryptedRecentUserActivity(user, requestSequence) {
    const dateStr = activityDate();
    const hours = activityHours();
    const isCurrent = () =>
      (typeof modalLoadSequence === "undefined" || requestSequence === modalLoadSequence) &&
      activityDate() === dateStr && activityHours().join(",") === hours.join(",");
    const showState = (message, isError = false) => {
      if (!isCurrent() || typeof setActivityState !== "function") return;
      setActivityState("recent-posts", message, isError);
      setActivityState("recent-comments", message, isError);
    };

    if (!isCurrent()) return;
    if (!/^\d{4}-\d{2}-\d{2}$/.test(dateStr)) {
      showState("조회할 날짜를 선택해 주세요.", true);
      return;
    }
    if (!hours.length || hours.some(hour => !Number.isInteger(hour) || hour < 0 || hour > 23)) {
      showState("조회할 시간대를 선택해 주세요.", true);
      return;
    }

    const userKey = typeof resolveUserKey === "function"
      ? String(resolveUserKey(user) || "").trim()
      : String(user?.user_key || "").trim();

    if (!userKey) {
      if (typeof setActivityState === "function") {
        setActivityState("recent-posts", "이 데이터에는 user_key가 없어 게시글 내용을 연결할 수 없습니다.");
        setActivityState("recent-comments", "이 데이터에는 user_key가 없어 댓글 내용을 연결할 수 없습니다.");
      }
      return;
    }

    if (typeof setActivityState === "function") {
      setActivityState("recent-posts", "최근 게시글을 불러오는 중...");
      setActivityState("recent-comments", "최근 댓글을 불러오는 중...");
    }

    const apiUrl = new URL("/api/user", DATA_API_BASE);
    apiUrl.searchParams.set("date", dateStr);
    apiUrl.searchParams.set("user_key", userKey);
    if (hours.length < 24) apiUrl.searchParams.set("hours", hours.join(","));
    apiUrl.searchParams.set("refresh", String(Date.now()));

    let response;
    try {
      response = await nativeFetch(apiUrl.href, {
        method: "GET",
        cache: "no-store",
        credentials: "omit",
      });
    } catch (error) {
      if (!isCurrent()) return;
      showState("최근 활동 API 연결에 실패했습니다. 연결을 확인한 뒤 유저를 다시 선택해 주세요.", true);
      console.error("encrypted details API network error:", error);
      return;
    }

    if (!isCurrent()) return;

    if (!response.ok) {
      let detail = "";
      try {
        const body = await response.json();
        detail = String(body?.code || "").slice(0, 100);
      } catch (_) {}
      showState(detailsErrorMessage(response.status, detail, dateStr), true);
      return;
    }

    let body;
    try {
      body = await response.json();
    } catch (error) {
      if (!isCurrent()) return;
      showState("최근 활동 응답 형식이 잘못되었습니다.", true);
      console.error("encrypted details JSON parse error:", error);
      return;
    }

    if (!isCurrent()) return;
    if (body?.date !== dateStr || body?.user_key !== userKey ||
        !Array.isArray(body?.posts) || !Array.isArray(body?.comments)) {
      showState("요청한 날짜·유저와 상세 응답이 일치하지 않거나 응답 형식이 잘못되었습니다.", true);
      return;
    }
    // 구버전 서버는 hours를 무시합니다. 하루 전체 결과를 시간대 결과로 표시하지 않습니다.
    if (hours.length < 24 && (!Array.isArray(body.hours) ||
        body.hours.slice().sort((a, b) => a - b).join(",") !== hours.join(","))) {
      showState("선택 시간대의 상세 조회를 지원하도록 서버 코드를 업데이트해 주세요. 전체 시간 보기에서는 날짜별 최근 활동을 확인할 수 있습니다.", true);
      return;
    }

    const posts = Array.isArray(body?.posts) ? body.posts : [];
    const comments = Array.isArray(body?.comments) ? body.comments : [];

    if (typeof renderPostItems === "function") renderPostItems(posts.slice(0, 5));
    else if (typeof setActivityState === "function") {
      setActivityState("recent-posts", "게시글 렌더링 함수를 찾지 못했습니다.", true);
    }

    if (typeof renderCommentItems === "function") renderCommentItems(comments.slice(0, 10));
    else if (typeof setActivityState === "function") {
      setActivityState("recent-comments", "댓글 렌더링 함수를 찾지 못했습니다.", true);
    }
  }

  if (typeof window.loadRecentUserActivity === "function") {
    window.loadRecentUserActivity = loadEncryptedRecentUserActivity;
  } else {
    console.error("[ProjectMX] loadRecentUserActivity 함수를 찾지 못했습니다.");
  }

  console.info("[ProjectMX] encrypted summary/details adapter v2.1 enabled:", DATA_API_BASE);
})();
