/* Interventions: apply policy/drug/cull actions and subscribe to the log. */

let CATALOG = null;
let runId = null;
let runMeta = null;
let stream = null;
let eventSeqs = new Set();

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

function eventRow(e) {
  return `<tr><td>${e.step}</td><td>${esc(e.type)}</td>
      <td>${e.scheduled ? '<span class="badge">定时</span>' : '<span class="badge ready">手动</span>'}</td>
      <td class="muted small">${esc((e.result && e.result.reason) || "")}</td></tr>`;
}

function renderEvents(events) {
  eventSeqs = new Set(events.map((e) => e.seq));
  el("events").innerHTML = `<thead><tr><th>步</th><th>类型</th><th>来源</th><th>结果</th></tr></thead><tbody>` +
    events.slice().reverse().map(eventRow).join("") + "</tbody>";
}

function appendEvent(event) {
  if (!event || eventSeqs.has(event.seq)) return;
  eventSeqs.add(event.seq);
  const body = el("events").querySelector("tbody");
  if (body) body.insertAdjacentHTML("afterbegin", eventRow(event));
}

function updateInfo() {
  el("runInfo").textContent = `${runMeta.name} · ${DOMAIN_LABEL[runMeta.domain]} · ${MODEL_LABEL[runMeta.model]} · 第 ${runMeta.current_step} 步 · ${runMeta.status}`;
}

async function resyncRun() {
  runMeta = await get(`/api/runs/${runId}`);
  updateInfo();
  renderForms();
  renderScheduled();
  const { events } = await get(`/api/runs/${runId}/events`);
  renderEvents(events);
}

async function subscribe(id) {
  if (stream) stream.close();
  stream = subscribeRun(id, ["status", "events"], {
    resync: resyncRun,
    "run.status": (m) => {
      if (runMeta && (m.status_revision || 0) < (runMeta.status_revision || 0)) return;
      runMeta = { ...(runMeta || {}), ...m }; updateInfo(); renderScheduled();
    },
    "run.event": appendEvent,
  });
}

async function loadAll(id = runId) {
  runId = id;
  await resyncRun();
  await subscribe(runId);
}

async function init() {
  const { domains } = await get("/api/catalog");
  CATALOG = domains;
  el("runSelect").onchange = (e) => { if (e.target.value) loadAll(e.target.value); };
  el("refreshBtn").onclick = async () => { await fillRunSelect(el("runSelect")); };
  window.addEventListener("beforeunload", () => stream && stream.close());
  await fillRunSelect(el("runSelect"));
  if (el("runSelect").options.length > 1) {
    el("runSelect").selectedIndex = 1;
    runId = el("runSelect").value;
    await loadAll(runId);
  }
}

init().catch((e) => console.error(e));
