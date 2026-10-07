/* Statistics charts: ECharts line chart, kept live by push while runs progress. */

let chart = null;
let series = [];
let labels = {};
let selected = new Set();
let runId = null;
let sub = null;
let lastRowStep = -1;

async function init() {
  const { domains } = await get("/api/catalog");
  for (const d of Object.values(domains)) {
    for (const m of d.metrics) labels[m.key] = m.label;
  }
  el("loadBtn").onclick = load;
  el("runSelect").onchange = onSelectChange;

  sub = subscribeRealtime({
    runIds: () => (runId ? [runId] : []),
    // Series is a heavier fetch; refresh at ~1 Hz while running and once more
    // on completion. The push frame itself only signals "newer data exists".
    onProgress: frameThrottle((d) => {
      if (!runId || d.run_id !== runId) return;
      refreshSeries();
    }, 1000),
    onStatus: (d) => {
      if (!runId || d.run_id !== runId) return;
      if (["finished", "stopped", "error"].includes(d.status)) refreshSeries();
    },
    onResync: () => { if (runId) load(); },
  });

  await fillRunSelect(el("runSelect"));
  const q = new URLSearchParams(window.location.search).get("run");
  if (q && el("runSelect").querySelector(`option[value="${q}"]`)) {
    el("runSelect").value = q;
    await load();
  } else if (el("runSelect").options.length > 1) { el("runSelect").selectedIndex = 1; await load(); }
}

async function onSelectChange() {
  await load();
  if (sub) sub.setRunIds(runId ? [runId] : []);
}

async function load() {
  runId = el("runSelect").value;
  if (!runId) return;
  await refreshSeries();
  // Metric keys from the newest row (minus step).
  const keys = series.length ? Object.keys(series[series.length - 1]).filter((k) => k !== "step") : [];
  const prev = selected;
  selected = new Set(keys.filter((k) => prev.has(k)).length ? [...prev].filter((k) => keys.includes(k)) : keys);

  el("metricChecks").innerHTML = keys.map((k) => `
    <label class="check"><input type="checkbox" class="mchk" value="${esc(k)}" ${selected.has(k) ? "checked" : ""}> ${esc(labels[k] || k)}</label>`).join("");
  el("metricChecks").querySelectorAll(".mchk").forEach((c) => {
    c.onchange = () => { c.checked ? selected.add(c.value) : selected.delete(c.value); render(); };
  });

  render();
}

/* Pull the series and only repaint when rows actually advanced. */
async function refreshSeries() {
  if (!runId) return;
  try {
    const { series: s } = await get(`/api/runs/${runId}/series`);
    const newest = s.length ? s[s.length - 1].step : -1;
    series = s;
    if (newest !== lastRowStep) {
      lastRowStep = newest;
      render();
    }
  } catch (e) { /* transient: next frame retries */ }
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
window.addEventListener("beforeunload", () => sub && sub.close());

init().catch((e) => console.error(e));
