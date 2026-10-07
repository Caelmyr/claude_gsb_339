/* Replay & timeline: scrub through sharded per-step snapshots. */

let runId = null;
let meta = null;
let savedSteps = [];
let maxIdx = 0;
let playing = false;
let playTimer = null;
let sub = null;
let followingTail = true;   // keep the scrubber pinned to the newest frame

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

async function loadRun(id) {
  runId = id;
  meta = await get(`/api/runs/${id}`);
  await refreshSteps({ follow: true });
  if (sub) sub.setRunIds([runId]);
}

/* Re-read the sharded step index and extend the timeline without losing the
 * user's scrub position (unless they are following the live tail). */
async function refreshSteps({ follow = false } = {}) {
  const { steps } = await get(`/api/runs/${runId}/steps`);
  const had = savedSteps.length;
  savedSteps = steps.length ? steps : [0];
  maxIdx = savedSteps.length - 1;
  el("timeline").max = maxIdx;
  el("t0").textContent = String(savedSteps[0]);
  el("t1").textContent = String(savedSteps[maxIdx]);
  if (follow || followingTail || !had) {
    el("timeline").value = maxIdx;
    await loadStep(maxIdx);
  }
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
  el("playBtn").textContent = "▶ 播放";
}

async function init() {
  el("runSelect").onchange = async (e) => { if (e.target.value) await loadRun(e.target.value); };
  el("refreshBtn").onclick = async () => { await fillRunSelect(el("runSelect")); };
  el("playBtn").onclick = play;
  el("timeline").oninput = debounce((e) => {
    // Manual scrub detaches from the live tail; a new run selection re-pins.
    followingTail = parseInt(el("timeline").value, 10) >= maxIdx;
    loadStep(parseInt(e.target.value, 10));
  }, 120);

  // New snapshots are sharded at snapshot_interval; refresh the index at most
  // every 1.5 s while a batch runs and once more when it finishes.
  sub = subscribeRealtime({
    runIds: () => (runId ? [runId] : []),
    onProgress: frameThrottle(() => { if (runId) refreshSteps(); }, 1500),
    onStatus: (d) => {
      if (!runId || d.run_id !== runId) return;
      if (["finished", "stopped", "error"].includes(d.status)) refreshSteps();
    },
    onResync: () => refreshSteps({ follow: true }),
  });

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

window.addEventListener("beforeunload", () => sub && sub.close());

init().catch((e) => console.error(e));
