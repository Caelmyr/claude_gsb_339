/* Real-time push client: per-page SSE subscriptions with replay & coalescing.
 *
 * Semantics (matching backend/realtime.py):
 *  - Every page tab opens its OWN EventSource with its own run-id filter, so a
 *    page looking at run A never receives run B's frames.
 *  - Every server frame carries a monotonic `id`; the browser resends the last
 *    one as Last-Event-ID after a network blip, and the server replays missed
 *    frames from its ring buffer. We additionally pass ?since= and de-dupe by
 *    id client-side -> no gaps, no double application.
 *  - If the gap is older than the server buffer (or the server restarted), the
 *    server sends `hello` {reason:"resync"}; the page's onResync re-fetches
 *    authoritative REST state. A fresh page load gets reason:"ready" after its
 *    own initial REST load.
 *  - Progress frames may arrive faster than a page renders. Handlers are
 *    wrapped with frameThrottle (leading + trailing); the trailing call always
 *    carries the NEWEST frame, so nothing visually stalls and order is kept.
 *  - Progress with a step <= the newest already seen for that run is dropped
 *    (can only happen across a reconnect race).
 *
 * Usage:
 *   const sub = subscribeRealtime({
 *     runIds: () => [runId],            // or global: true
 *     onProgress: (data) => ...,        // coalescable step/stats frame
 *     onStatus:   (data) => ...,        // run lifecycle
 *     onEvent:    (data) => ...,        // intervention applied
 *     onExperiment: (data) => ...,
 *     onResync:   () => reloadAll(),    // full REST refetch
 *   });
 *   sub.setRunIds([id]);                // switch runs: reconnects atomically
 *   sub.close();
 */

function throttleLeadingTrailing(fn, wait) {
  let last = 0;
  let timer = null;
  let lastArgs = null;
  return function throttled(...args) {
    const now = Date.now();
    lastArgs = args;
    const remain = wait - (now - last);
    if (remain <= 0) {
      if (timer) { clearTimeout(timer); timer = null; }
      last = now;
      fn.apply(this, args);
    } else if (!timer) {
      timer = setTimeout(() => {
        last = Date.now();
        timer = null;
        const a = lastArgs;
        lastArgs = null;
        fn.apply(this, a);
      }, remain);
    }
  };
}

/* Exposed for pages that want their own render-rate limiting (canvas etc.). */
function frameThrottle(fn, wait = 150) {
  return throttleLeadingTrailing(fn, wait);
}

function subscribeRealtime(opts) {
  let es = null;
  let runIds = (opts.runIds ? opts.runIds() : null) || [];
  let closed = false;
  let watermark = 0;                 // last server sequence applied
  let reconnecting = false;
  const seenSteps = {};             // run_id -> newest applied progress step

  function isGlobal() { return !!(opts.global && opts.global()); }

  function buildUrl() {
    const params = new URLSearchParams();
    if (isGlobal()) {
      params.set("global", "1");
    } else {
      runIds.filter(Boolean).forEach((id) => params.append("run", id));
    }
    if (watermark > 0) params.set("since", String(watermark));
    return `/api/stream?${params.toString()}`;
  }

  function setPill(state) {
    let elPill = document.getElementById("rtPill");
    if (!elPill) {
      elPill = document.createElement("div");
      elPill.id = "rtPill";
      elPill.className = "rt-pill";
      document.body.appendChild(elPill);
    }
    if (state === "live") {
      elPill.className = "rt-pill live";
      elPill.textContent = "● 实时连接";
    } else if (state === "reconnecting") {
      elPill.className = "rt-pill reconnecting";
      elPill.textContent = "○ 重连中…";
    } else {
      elPill.className = "rt-pill";
      elPill.textContent = "○ 未连接";
    }
  }

  function dispatch(evType, raw, seq) {
    // Global ordering guard: frames can be redelivered across a reconnect.
    if (seq != null && seq <= watermark) return;

    let data;
    try { data = JSON.parse(raw); } catch (e) { return; }

    if (evType === "hello") {
      // The hello carries the server's current sequence as its id; advance the
      // watermark with it so the subsequent REST refetch is the new baseline.
      if (seq != null) watermark = Math.max(watermark, seq);
      if (data.reason === "resync") {
        Object.keys(seenSteps).forEach((k) => delete seenSteps[k]);
        if (opts.onResync) opts.onResync();
      }
      return;
    }

    if (seq != null) watermark = Math.max(watermark, seq);

    switch (evType) {
      case "progress": {
        const step = data.step | 0;
        const prev = seenSteps[data.run_id];
        if (prev != null && step <= prev) return;  // stale / reordered frame
        seenSteps[data.run_id] = step;
        if (opts.onProgress) opts.onProgress(data);
        break;
      }
      case "run.status":
        if (opts.onStatus) opts.onStatus(data);
        break;
      case "run.event":
        if (opts.onEvent) opts.onEvent(data);
        break;
      case "run.created":
        if (opts.onRunCreated) opts.onRunCreated(data);
        break;
      case "run.deleted":
        if (opts.onRunDeleted) opts.onRunDeleted(data);
        break;
      case "experiment.status":
        if (opts.onExperiment) opts.onExperiment(data);
        break;
      default:
        break;
    }
  }

  function connect() {
    if (closed) return;
    if (!isGlobal() && !runIds.length) { setPill("idle"); return; }
    es = new EventSource(buildUrl());
    reconnecting = false;
    setPill("reconnecting");

    const TYPES = ["hello", "progress", "run.status", "run.event",
                   "run.created", "run.deleted", "experiment.status"];
    TYPES.forEach((t) => {
      es.addEventListener(t, (ev) => {
        setPill("live");
        const seq = ev.lastEventId ? parseInt(ev.lastEventId, 10) : null;
        dispatch(t, ev.data, Number.isFinite(seq) ? seq : null);
      });
    });

    es.onopen = () => setPill("live");
    es.onerror = () => {
      // EventSource reconnects automatically; just reflect the transient gap.
      reconnecting = true;
      setPill("reconnecting");
    };
  }

  function reconnect() {
    if (es) { es.close(); es = null; }
    connect();
  }

  // Re-subscribe atomically when the page switches runs: the old filter is
  // torn down before the new one goes up, so frames can never cross between
  // runs. The watermark is kept (server replays missed events > since); the
  // page performs its own initial REST load alongside the switch.
  function setRunIds(ids) {
    runIds = (ids || []).filter(Boolean);
    Object.keys(seenSteps).forEach((k) => delete seenSteps[k]);
    reconnect();
  }

  function setGlobal() {
    Object.keys(seenSteps).forEach((k) => delete seenSteps[k]);
    reconnect();
  }

  function close() {
    closed = true;
    if (es) es.close();
    const elPill = document.getElementById("rtPill");
    if (elPill) elPill.remove();
  }

  connect();
  return { close, setRunIds, setGlobal, throttle: frameThrottle };
}
