/* Replay & timeline: scrub through sharded per-step snapshots. */

let runId = null;
let meta = null;
let savedSteps = [];
let maxIdx = 0;
let playing = false;
let playTimer = null;
let stream = null;
let followLatest = true;

function renderStats(stats) {
  el("statTiles").innerHTML = Object.entries(stats || {}).map(([k, v]) => `
    <div class="stat"><div class="k">${esc(k)}</div><div class="v">${fmt(v)}</div></div>`).join("");
}

async function loadStep(idx) {
  const step = savedSteps[idx];
  const snap = await get(`/api/runs/${runId}/snapshot?step=${step}`);
  renderSnapshot(el("canvas"), snap, meta.domain, meta.model);
  renderLegend(el("legend"), snap.palette);
  renderStats(snap.stats);
  el("stepLabel").textContent = `第 ${step} 步`;
  el("timeline").value = idx;
}

async function reloadSteps(moveToLatest = followLatest) {
  const { steps } = await get(`/api/runs/${runId}/steps`);
  const previousMax = savedSteps.length ? savedSteps[maxIdx] : -1;
  savedSteps = steps.length ? steps : [0];
  maxIdx = savedSteps.length - 1;
  el("timeline").max = maxIdx;
  el("t0").textContent = String(savedSteps[0]);
  el("t1").textContent = String(savedSteps[maxIdx]);
  if (moveToLatest || followLatest) {
    followLatest = true;
    await loadStep(maxIdx);
  } else {
    const current = previousMax >= 0 ? previousMax : savedSteps[0];
    const idx = savedSteps.indexOf(current);
    if (idx >= 0) el("timeline").value = idx;
  }
}

async function loadRun(id) {
  stopPlay();
  runId = id;
  meta = await get(`/api/runs/${id}`);
  await reloadSteps(true);
  if (stream) stream.close();
  stream = subscribeRun(id, ["status", "progress"], {
    resync: async () => {
      meta = await get(`/api/runs/${id}`);
      await reloadSteps(followLatest);
    },
    "run.status": async (m) => {
      meta = { ...(meta || {}), ...m };
      if (["finished", "stopped", "reset", "error"].includes(m.status) ||
          m.reason === "reset") {
        await reloadSteps(followLatest);
      }
    },
    "run.progress": async (p) => {
      if (!p.snapshot_available) return;
      const before = savedSteps[maxIdx];
      await reloadSteps(followLatest);
      if (p.step > before && !followLatest) {
        // A newly persisted snapshot became available, but leave a scrubbing
        // user on the frame they selected.
        followLatest = false;
      }
    },
  });
}

function play() {
  if (!runId) return;
  if (playing) { stopPlay(); return; }
  let s = parseInt(el("timeline").value, 10);
  if (s >= maxIdx) s = 0;
  playing = true;
  el("playBtn").textContent = "⏸ 暂停";
  const loop = () => {
    if (!playing) return;
    loadStep(s);
    s = s + 1 > maxIdx ? 0 : s + 1;
    playTimer = setTimeout(loop, 120);
  };
  loop();
}

function stopPlay() {
  playing = false;
  if (playTimer) clearTimeout(playTimer);
  const btn = el("playBtn");
  if (btn) btn.textContent = "▶ 播放";
}

async function init() {
  el("runSelect").onchange = (e) => { if (e.target.value) loadRun(e.target.value); };
  el("refreshBtn").onclick = async () => { await fillRunSelect(el("runSelect")); };
  el("playBtn").onclick = play;
  el("timeline").oninput = debounce((e) => {
    followLatest = parseInt(e.target.value, 10) < maxIdx;
    loadStep(parseInt(e.target.value, 10));
  }, 120);
  window.addEventListener("beforeunload", () => stream && stream.close());

  await fillRunSelect(el("runSelect"));
  const q = new URLSearchParams(window.location.search).get("run");
  if (q && el("runSelect").querySelector(`option[value="${q}"]`)) {
    el("runSelect").value = q;
    await loadRun(q);
  } else if (el("runSelect").options.length > 1) {
    el("runSelect").selectedIndex = 1;
    await loadRun(el("runSelect").value);
  }
}

init().catch((e) => console.error(e));
