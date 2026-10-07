/* Real-time visualization: streamed progress + coalesced canvas rendering. */

let runId = null;
let meta = null;
let labels = {};
let running = false;
let timer = null;
let stream = null;
let snapshotToken = 0;
let snapshotTimer = null;
let pendingSnapshotStep = 0;
let snapshotInflight = false;
let lastSnapshotStep = -1;

function statusLine(t) { el("statusLine").textContent = t; }

async function refreshRuns() {
  const runs = await fillRunSelect(el("runSelect"));
  if (runId && runs.some((r) => r.id === runId)) {
    el("runSelect").value = runId;
  } else if (runId) {
    runId = null;
    closeStream();
  }
}

function closeStream() {
  if (stream) stream.close();
  stream = null;
}

async function resyncRun() {
  const id = runId;
  meta = await get(`/api/runs/${id}`);
  if (runId !== id) return;
  const snap = await get(`/api/runs/${id}/snapshot`);
  if (runId !== id) return;
  lastSnapshotStep = snap.step || meta.current_step;
  drawSnapshot(snap);
  updateStatus(meta, "已同步");
}

async function loadRun(id) {
  pause();
  closeStream();
  if (snapshotTimer) { clearTimeout(snapshotTimer); snapshotTimer = null; }
  snapshotInflight = false;
  pendingSnapshotStep = 0;
  runId = id;
  snapshotToken += 1;
  lastSnapshotStep = -1;
  meta = await get(`/api/runs/${id}`);
  await loadSnapshot(false);
  stream = subscribeRun(id, ["status", "progress", "events"], {
    resync: resyncRun,
    "run.status": handleStatus,
    "run.progress": handleProgress,
    "run.event": handleEvent,
    connection: (s) => {
      if (s === "reconnecting") statusLine("连接断开，正在自动重连…");
      if (s === "resync") statusLine("已重连，正在补齐进度…");
    },
  });
}

async function loadSnapshot(mark = true) {
  if (!runId) return;
  const snap = await get(`/api/runs/${runId}/snapshot`);
  drawSnapshot(snap);
  if (mark) lastSnapshotStep = snap.step || 0;
  const cur = await get(`/api/runs/${runId}`);
  updateStatus(cur, "");
}

function updateStatus(cur, suffix = "") {
  if (!cur) return;
  meta = { ...(meta || {}), ...cur };
  statusLine(`第 ${cur.current_step} 步 · ${cur.status}${suffix ? ` · ${suffix}` : ""}`);
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

async function fetchSnapshot(step) {
  if (!runId || step <= lastSnapshotStep) return;
  const token = ++snapshotToken;
  try {
    const url = runId !== id
      ? `/api/runs/${runId}/snapshot?step=${step}`
      : `/api/runs/${runId}/snapshot`;
    const snap = await get(url);
    if (token !== snapshotToken || !runId) return;
    if ((snap.step || 0) < lastSnapshotStep) return;
    lastSnapshotStep = snap.step || step;
    drawSnapshot(snap);
    if (meta) statusLine(`第 ${snap.step} 步 · ${meta.status}`);
  } catch (e) {
    if (token === snapshotToken) statusLine("快照暂时不可用，等待下一帧…");
  }
}

function scheduleSnapshot(step, immediate = false) {
  if (!step || step <= lastSnapshotStep) return;
  pendingSnapshotStep = Math.max(pendingSnapshotStep, step);
  if (snapshotTimer || snapshotInflight) return;
  const delay = immediate ? 0 : 120;
  snapshotTimer = setTimeout(async () => {
    snapshotTimer = null;
    const target = pendingSnapshotStep;
    snapshotInflight = true;
    try {
      await fetchSnapshot(target);
    } finally {
      snapshotInflight = false;
      if (pendingSnapshotStep > lastSnapshotStep) {
        scheduleSnapshot(pendingSnapshotStep);
      }
    }
  }, delay);
}

function handleStatus(status) {
  const oldRevision = meta ? (meta.status_revision || 0) : 0;
  if ((status.status_revision || 0) < oldRevision) return;
  updateStatus(status, status.reason === "finished" ? "已完成" : "");
  if (["reset", "error"].includes(status.reason) ||
      ["ready", "finished", "stopped", "error"].includes(status.status)) {
    scheduleSnapshot(status.current_step, true);
  }
}

function handleProgress(p) {
  if (!meta) return;
  if (p.step < (meta.current_step || 0)) return;
  meta.current_step = p.step;
  meta.target_step = p.target_step;
  if (p.stats) meta.stats = p.stats;
  renderStats(p.stats);
  const pct = p.target_step ? ` · ${Math.min(100, Math.round((p.step / p.target_step) * 100))}%` : "";
  statusLine(`第 ${p.step} 步 · 运行中${pct}`);
  if (p.reset || p.snapshot_available) scheduleSnapshot(p.step);
}

function handleEvent(event) {
  if (event.scheduled) statusLine(`第 ${event.step} 步 · 已触发 ${event.type}`);
}

async function tick() {
  if (!running || !runId) return;
  try {
    const n = parseInt(el("stepsPerTick").value, 10) || 1;
    const r = await post(`/api/runs/${runId}/step`, { n });
    lastSnapshotStep = r.step;
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

async function reset() {
  pause();
  if (!runId) return;
  await post(`/api/runs/${runId}/reset`);
  lastSnapshotStep = 0;
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
  el("btnStep").onclick = async () => {
    if (!runId) return;
    const r = await post(`/api/runs/${runId}/step`, { n: 1 });
    lastSnapshotStep = r.step;
    drawSnapshot(r.snapshot);
    statusLine(`第 ${r.step} 步`);
  };
  el("btnReset").onclick = reset;
  window.addEventListener("beforeunload", closeStream);

  await refreshRuns();
  const q = new URLSearchParams(window.location.search).get("run");
  if (q && el("runSelect").querySelector(`option[value="${q}"]`)) {
    el("runSelect").value = q;
    await loadRun(q);
  }
}

init().catch((e) => statusLine("初始化失败：" + e.message));
