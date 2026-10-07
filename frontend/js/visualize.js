/* Real-time visualization: live push + canvas rendering.
 *
 * Background batch runs arrive over the push channel: progress frames are
 * coalesced server-side (~10 fps) and throttled again client-side to the
 * canvas paint rate; snapshots (the heavy payload) stay on REST and are only
 * pulled when we actually repaint.  Manual stepping keeps using the POST
 * response (it IS the authoritative frame), while push still notifies other
 * pages looking at the same run.
 */

let runId = null;
let meta = null;
let labels = {};
let running = false;
let timer = null;
let sub = null;
let paintQueued = false;

function statusLine(t) { el("statusLine").textContent = t; }

async function refreshRuns() {
  const runs = await fillRunSelect(el("runSelect"));
  if (runId && runs.some((r) => r.id === runId)) {
    el("runSelect").value = runId;
  } else if (runId) {
    runId = null;
  }
}

/* ---- push handling ----------------------------------------------------- */
const paintFromPush = frameThrottle(async () => {
  if (!runId || paintQueued) return;
  paintQueued = true;
  try {
    const snap = await get(`/api/runs/${runId}/snapshot`);
    if (snap && runId) drawSnapshot(snap);
  } catch (e) { /* a transient fetch failure just skips one frame */ }
  paintQueued = false;
}, 250);

function onProgress(d) {
  if (!runId || d.run_id !== runId) return;
  renderStats(d.stats || {});
  statusLine(`第 ${d.step} 步${d.fraction != null ? ` / ${d.total_steps}（${Math.round(d.fraction * 100)}%）` : ""} · 运行中`);
  paintFromPush();
}

async function onStatus(d) {
  if (!runId || d.run_id !== runId) return;
  const terminal = ["finished", "stopped", "error", "paused", "ready"].includes(d.status);
  if (terminal) {
    // Terminal frame: always show the final state, bypassing the throttle.
    try {
      const snap = await get(`/api/runs/${runId}/snapshot`);
      if (snap && runId) drawSnapshot(snap);
    } catch (e) { /* ignore */ }
  }
  if (meta) meta.status = d.status;
  const label = { running: "运行中", paused: "已暂停", finished: "已完成",
                  stopped: "已停止", error: "错误", ready: "就绪" }[d.status] || d.status;
  statusLine(`第 ${d.current_step ?? ""} 步 · ${label}`);
}

async function resyncView() {
  if (runId) await loadRun(runId);
}

async function loadRun(id) {
  runId = id;
  meta = await get(`/api/runs/${id}`);
  await loadSnapshot();
  if (sub) sub.setRunIds([runId]);
}

async function loadSnapshot() {
  if (!runId) return;
  const snap = await get(`/api/runs/${runId}/snapshot`);
  drawSnapshot(snap);
  const cur = await get(`/api/runs/${runId}`);
  const label = { running: "运行中", paused: "已暂停", finished: "已完成",
                  stopped: "已停止", error: "错误", ready: "就绪" }[cur.status] || cur.status;
  statusLine(`第 ${cur.current_step} 步 · ${label}`);
}

function drawSnapshot(snap) {
  renderSnapshot(el("canvas"), snap, meta.domain, meta.model);
  renderLegend(el("legend"), snap.palette);
  renderStats(snap.stats);
}

function renderStats(stats) {
  el("statTiles").innerHTML = Object.entries(stats).map(([k, v]) => `
    <div class="stat"><div class="k">${esc(labels[k] || k)}</div>
    <div class="v">${fmt(v)}</div></div>`).join("");
}

async function tick() {
  if (!running || !runId) return;
  try {
    const n = parseInt(el("stepsPerTick").value, 10) || 1;
    const r = await post(`/api/runs/${runId}/step`, { n });
    drawSnapshot(r.snapshot);
    statusLine(`第 ${r.step} 步 · 运行中`);
  } catch (e) {
    running = false;
    statusLine("已停止：" + e.message);
    return;
  }
  const delay = parseInt(el("speedSel").value, 10) || 160;
  timer = setTimeout(tick, delay);
}

function start() { if (!runId) { alert("请先选择运行"); return; } running = true; tick(); }
function pause() { running = false; if (timer) clearTimeout(timer); }

async function startBatch() {
  if (!runId) { alert("请先选择运行"); return; }
  const steps = parseInt(el("batchSteps").value, 10) || 200;
  pause();
  try {
    // Fire-and-forget on purpose: progress/finish arrive via the push channel,
    // so a 10k-step request never holds an HTTP response or gets polled.
    await post(`/api/runs/${runId}/batch`, { steps, background: true });
    statusLine(`批量运行已开始（共 ${steps} 步）…`);
  } catch (e) { statusLine("批量运行启动失败：" + e.message); }
}

async function stopBatch() {
  if (!runId) return;
  try { await post(`/api/runs/${runId}/stop`, {}); } catch (e) { /* best effort */ }
}

async function reset() {
  pause();
  if (!runId) return;
  await post(`/api/runs/${runId}/reset`);
  await loadSnapshot();
  statusLine("已重置到第 0 步");
}

async function init() {
  const { domains } = await get("/api/catalog");
  for (const d of Object.values(domains)) {
    for (const m of d.metrics) labels[m.key] = m.label;
  }
  el("runSelect").onchange = (e) => { if (e.target.value) loadRun(e.target.value); };
  el("refreshRuns").onclick = refreshRuns;
  el("btnStart").onclick = start;
  el("btnPause").onclick = pause;
  el("btnStep").onclick = async () => { if (!runId) return; const r = await post(`/api/runs/${runId}/step`, { n: 1 }); drawSnapshot(r.snapshot); statusLine(`第 ${r.step} 步`); };
  el("btnBatch").onclick = startBatch;
  el("btnStopBatch").onclick = stopBatch;
  el("btnReset").onclick = reset;

  // Subscribe before the first run is chosen; once loadRun() knows the id it
  // re-scopes this exact connection, so no frames from other runs arrive.
  sub = subscribeRealtime({
    runIds: () => (runId ? [runId] : []),
    onProgress,
    onStatus,
    onResync: resyncView,
  });

  await refreshRuns();
  const q = new URLSearchParams(window.location.search).get("run");
  if (q && el("runSelect").querySelector(`option[value="${q}"]`)) {
    el("runSelect").value = q;
    await loadRun(q);
  }
}

window.addEventListener("beforeunload", () => sub && sub.close());

init().catch((e) => statusLine("初始化失败：" + e.message));
