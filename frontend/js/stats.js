/* Statistics charts: streamed compact series rows replace polling. */

let chart = null;
let series = [];
let labels = {};
let selected = new Set();
let runId = null;
let stream = null;
let renderQueued = false;
let lastStep = -1;

async function init() {
  const { domains } = await get("/api/catalog");
  for (const d of Object.values(domains)) {
    for (const m of d.metrics) labels[m.key] = m.label;
  }
  el("loadBtn").onclick = load;
  el("runSelect").onchange = load;
  window.addEventListener("beforeunload", () => stream && stream.close());
  await fillRunSelect(el("runSelect"));
  const q = new URLSearchParams(window.location.search).get("run");
  if (q && el("runSelect").querySelector(`option[value="${q}"]`)) {
    el("runSelect").value = q;
    await load();
  } else if (el("runSelect").options.length > 1) { el("runSelect").selectedIndex = 1; await load(); }
}

async function resyncRun() {
  const { series: s } = await get(`/api/runs/${runId}/series`);
  series = s;
  lastStep = series.length ? series[series.length - 1].step : -1;
  rebuildMetricKeys(true);
  queueRender();
}

async function subscribe() {
  if (stream) stream.close();
  stream = subscribeRun(runId, ["status", "progress"], {
    resync: resyncRun,
    "run.status": (m) => {
      if (m.status === "finished" || m.status === "stopped" || m.status === "error") resyncRun();
    },
    "run.progress": mergeProgress,
  });
}

async function load() {
  runId = el("runSelect").value;
  if (!runId) return;
  await resyncRun();
  await subscribe();
}

function rebuildMetricKeys(resetChecks) {
  const keys = series.length ? Object.keys(series[series.length - 1]).filter((k) => k !== "step") : [];
  if (resetChecks) selected = new Set(keys);

  el("metricChecks").innerHTML = keys.map((k) => `
    <label class="check"><input type="checkbox" class="mchk" value="${esc(k)}" ${selected.has(k) ? "checked" : ""}> ${esc(labels[k] || k)}</label>`).join("");
  el("metricChecks").querySelectorAll(".mchk").forEach((c) => {
    c.onchange = () => { c.checked ? selected.add(c.value) : selected.delete(c.value); render(); };
  });
}

function mergeProgress(p) {
  const byStep = new Map(series.map((r) => [r.step, r]));
  (p.rows || [])
    .filter((r) => r.step > lastStep)
    .forEach((r) => { byStep.set(r.step, r); lastStep = Math.max(lastStep, r.step); });
  series = [...byStep.values()].sort((a, b) => a.step - b.step);
  const oldKeys = [...selected];
  rebuildMetricKeys(false);
  oldKeys.forEach((k) => selected.add(k));
  queueRender();
}

function queueRender() {
  if (renderQueued) return;
  renderQueued = true;
  requestAnimationFrame(() => { renderQueued = false; render(); });
}

function render() {
  if (!chart) chart = echarts.init(el("chart"), "dark");
  const keys = [...selected];
  const option = {
    backgroundColor: "transparent",
    tooltip: { trigger: "axis" },
    legend: { textStyle: { color: "#8b98a5" }, top: 0 },
    grid: { left: 60, right: 24, top: 40, bottom: 40 },
    xAxis: {
      type: "category",
      data: series.map((r) => r.step),
      name: "时间步",
      axisLine: { lineStyle: { color: "#26313f" } },
    },
    yAxis: {
      type: "value",
      axisLine: { lineStyle: { color: "#26313f" } },
      splitLine: { lineStyle: { color: "#1b2430" } },
    },
    series: keys.map((k) => ({
      name: labels[k] || k,
      type: "line",
      showSymbol: false,
      smooth: true,
      data: series.map((r) => r[k] != null ? r[k] : null),
    })),
  };
  chart.setOption(option, true);
  chart.resize();
}

window.addEventListener("resize", () => chart && chart.resize());

init().catch((e) => console.error(e));
