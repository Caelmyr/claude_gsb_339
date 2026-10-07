/* Interventions: apply policy/drug/cull actions and watch the live event log.
 *
 * The event log updates itself over the push channel: a background batch that
 * crosses a scheduled intervention's at_step emits run.event immediately, and
 * manual applies from this page arrive the same way (the POST response is only
 * used for the alert). Scheduled-intervention badges and the status line are
 * driven by run.status / run.event too, so no polling is needed.
 */

let CATALOG = null;
let runId = null;
let runMeta = null;
let sub = null;

function paramField(p) {
  if (p.type === "bool") {
    return `<label class="check"><input type="checkbox" data-key="${esc(p.key)}" ${p.default ? "checked" : ""}> ${esc(p.label)}</label>`;
  }
  return `<div class="range-row">
    <label class="muted small" style="min-width:70px">${esc(p.label)}</label>
    <input type="range" data-key="${esc(p.key)}" min="${p.min}" max="${p.max}" step="${p.step || 0.05}" value="${p.default}">
    <span class="range-val">${fmt(p.default)}</span>
  </div>`;
}

function renderForms() {
  const dom = CATALOG[runMeta.domain];
  el("itvForms").innerHTML = dom.interventions.map((itv) => `
    <div class="card" style="padding:12px; margin-bottom:10px" data-type="${esc(itv.type)}">
      <div class="row between" style="margin-bottom:6px">
        <strong>${esc(itv.label)}</strong>
        <button class="btn primary small apply">施加</button>
      </div>
      <div class="itv-params">${itv.params.length ? itv.params.map(paramField).join("") : '<span class="muted small">无参数</span>'}</div>
    </div>`).join("");

  el("itvForms").querySelectorAll(".card").forEach((card) => {
    card.querySelectorAll('input[type="range"]').forEach((r) => {
      const lab = r.parentElement.querySelector(".range-val");
      r.oninput = () => lab.textContent = fmt(parseFloat(r.value));
    });
    card.querySelector(".apply").onclick = async () => {
      const params = {};
      card.querySelectorAll("[data-key]").forEach((inp) => {
        params[inp.dataset.key] = inp.type === "checkbox" ? inp.checked : parseFloat(inp.value);
      });
      try {
        const res = await post(`/api/runs/${runId}/interventions`, { type: card.dataset.type, params });
        alert(res.applied ? ("已施加：" + res.reason) : ("未施加：" + res.reason));
        // The authoritative event comes back over run.event; refresh scheduled
        // badges directly as well so the click feels instant.
        runMeta = await get(`/api/runs/${runId}`);
        renderScheduled();
      } catch (e) { alert("施加失败：" + e.message); }
    };
  });
}

function renderScheduled() {
  const list = runMeta.interventions || [];
  el("scheduled").innerHTML = list.map((i) => `
    <div class="row between" style="padding:6px 0;border-bottom:1px solid var(--border)">
      <span>${esc(i.label || i.type)} · 触发步 ${i.at_step ?? 0}</span>
      ${i.applied ? '<span class="badge finished">已施加</span>' : '<span class="badge ready">待触发</span>'}
    </div>`).join("") || '<p class="muted small">该场景没有定时干预。</p>';
}

async function loadEvents() {
  const { events } = await get(`/api/runs/${runId}/events`);
  el("events").innerHTML = `<thead><tr><th>步</th><th>类型</th><th>来源</th><th>结果</th></tr></thead><tbody>` +
    events.slice().reverse().map((e) => `
      <tr><td>${e.step}</td><td>${esc(e.type)}</td>
      <td>${e.scheduled ? '<span class="badge">定时</span>' : '<span class="badge ready">手动</span>'}</td>
      <td class="muted small">${esc((e.result && e.result.reason) || "")}</td></tr>`).join("") + "</tbody>";
}

function renderInfo() {
  el("runInfo").textContent = `${runMeta.name} · ${DOMAIN_LABEL[runMeta.domain]} · ${MODEL_LABEL[runMeta.model]} · 第 ${runMeta.current_step} 步 · ${runMeta.status}`;
}

async function loadAll() {
  runMeta = await get(`/api/runs/${runId}`);
  renderInfo();
  renderForms();
  renderScheduled();
  await loadEvents();
}

async function init() {
  const { domains } = await get("/api/catalog");
  CATALOG = domains;
  el("runSelect").onchange = (e) => {
    if (e.target.value) { runId = e.target.value; loadAll(); sub.setRunIds([runId]); }
  };
  el("refreshBtn").onclick = async () => { await fillRunSelect(el("runSelect")); };

  sub = subscribeRealtime({
    runIds: () => (runId ? [runId] : []),
    // An intervention (manual here, or scheduled by a running batch) happened.
    // The log is rebuilt from the authoritative REST list, so a frame
    // redelivered across reconnect can never duplicate a row.
    onEvent: (d) => {
      if (!runId || d.run_id !== runId) return;
      loadEvents();
      // Scheduled applies also flip "待触发" badges and advance the step.
      get(`/api/runs/${runId}`).then((m) => { runMeta = m; renderInfo(); renderScheduled(); });
    },
    onStatus: (d) => {
      if (!runId || d.run_id !== runId || !runMeta) return;
      runMeta.status = d.status;
      runMeta.current_step = d.current_step;
      renderInfo();
      if (["finished", "stopped", "error"].includes(d.status)) { loadEvents(); renderScheduled(); }
    },
    onResync: () => { if (runId) loadAll(); },
  });

  await fillRunSelect(el("runSelect"));
  if (el("runSelect").options.length > 1) {
    el("runSelect").selectedIndex = 1;
    runId = el("runSelect").value;
    await loadAll();
    sub.setRunIds([runId]);
  }
}

window.addEventListener("beforeunload", () => sub && sub.close());

init().catch((e) => console.error(e));
