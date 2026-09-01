const state = { queryId: null, offset: 0, limit: 100, nextOffset: null, polling: null };

const byId = (id) => document.getElementById(id);

async function api(path, options = {}) {
  const response = await fetch(path, {
    headers: { "Content-Type": "application/json", ...(options.headers || {}) },
    ...options,
  });
  if (response.status === 204) return null;
  const payload = await response.json();
  if (!response.ok) throw new Error(payload.error?.message || `HTTP ${response.status}`);
  return payload;
}

function iconButton(icon, title, action) {
  const button = document.createElement("button");
  button.className = "icon-button";
  button.title = title;
  button.setAttribute("aria-label", title);
  button.innerHTML = `<svg><use href="#i-${icon}"></use></svg>`;
  button.addEventListener("click", action);
  return button;
}

async function refreshCatalog() {
  const root = byId("catalog-tree");
  try {
    const data = await api("/api/v1/catalog/namespaces");
    root.replaceChildren();
    for (const namespace of data.namespaces) {
      const wrapper = document.createElement("div");
      wrapper.className = "namespace";
      const line = document.createElement("div");
      line.className = "namespace-line";
      const name = document.createElement("span");
      name.textContent = namespace.name;
      line.append(name);
      line.append(iconButton("plus", `在 ${namespace.name} 注册表`, () => {
        openCatalogDialog("table", namespace.name);
      }));
      line.append(iconButton("trash", `删除 namespace ${namespace.name}`, async () => {
        if (!confirm(`删除空 namespace ${namespace.name}?`)) return;
        await api(`/api/v1/catalog/namespaces/${encodeURIComponent(namespace.name)}`, {
          method: "DELETE",
        });
        await refreshCatalog();
      }));
      wrapper.append(line);
      const tables = await api(
        `/api/v1/catalog/namespaces/${encodeURIComponent(namespace.name)}/tables`
      );
      for (const table of tables.tables) {
        const tableLine = document.createElement("div");
        tableLine.className = "table-line";
        const label = document.createElement("span");
        label.textContent = `${table.name} · ${table.format}`;
        label.title = table.location;
        tableLine.append(label);
        tableLine.append(iconButton("refresh", `导入 ${table.name}`, () => {
          openCatalogDialog("import", namespace.name, table.name);
        }));
        tableLine.append(iconButton("trash", `删除表 ${table.name}`, async () => {
          if (!confirm(`删除表 ${namespace.name}.${table.name}?`)) return;
          await api(
            `/api/v1/catalog/namespaces/${encodeURIComponent(namespace.name)}/tables/${encodeURIComponent(table.name)}`,
            { method: "DELETE" }
          );
          await refreshCatalog();
        }));
        wrapper.append(tableLine);
      }
      root.append(wrapper);
    }
    if (!data.namespaces.length) root.textContent = "暂无 namespace";
  } catch (error) {
    root.textContent = error.message;
    root.className = "tree error";
  }
}

async function refreshNodes() {
  const root = byId("nodes");
  try {
    const data = await api("/api/v1/nodes");
    byId("node-count").textContent = data.workers.length;
    root.replaceChildren();
    for (const worker of data.workers) {
      const line = document.createElement("div");
      line.className = "node";
      const identity = document.createElement("span");
      identity.textContent = `${worker.worker_id} · ${worker.available_slots}/${worker.slots}`;
      identity.title = worker.endpoint;
      const status = document.createElement("span");
      status.className = `node-state ${worker.state === "lost" ? "error" : ""}`;
      status.textContent = worker.state;
      line.append(identity, status);
      root.append(line);
    }
    if (!data.workers.length) root.textContent = "暂无已注册节点";
    byId("connection").textContent = "已连接";
  } catch (error) {
    byId("connection").textContent = "连接失败";
    root.textContent = error.message;
  }
}

function setQueryStatus(query) {
  const status = byId("query-status");
  status.textContent = query.error ? `${query.state}: ${query.error.message}` : query.state;
  status.className = `status ${query.state === "failed" ? "error" : ""}`;
  byId("cancel").disabled = !["queued", "planning", "running"].includes(query.state);
}

async function runQuery() {
  clearInterval(state.polling);
  state.offset = 0;
  try {
    const query = await api("/api/v1/queries", {
      method: "POST",
      body: JSON.stringify({ sql: byId("sql").value }),
    });
    state.queryId = query.query_id;
    setQueryStatus(query);
    state.polling = setInterval(pollQuery, 250);
    await pollQuery();
  } catch (error) {
    showError(error);
  }
}

async function pollQuery() {
  if (!state.queryId) return;
  try {
    const query = await api(`/api/v1/queries/${state.queryId}`);
    setQueryStatus(query);
    if (["succeeded", "failed", "canceled"].includes(query.state)) {
      clearInterval(state.polling);
      state.polling = null;
      if (query.state === "succeeded") await loadQueryDetails();
    }
  } catch (error) {
    clearInterval(state.polling);
    showError(error);
  }
}

async function cancelQuery() {
  if (!state.queryId) return;
  const query = await api(`/api/v1/queries/${state.queryId}`, { method: "DELETE" });
  setQueryStatus(query);
}

async function explainQuery() {
  try {
    const plan = await api("/api/v1/queries/explain", {
      method: "POST",
      body: JSON.stringify({ sql: byId("sql").value }),
    });
    renderPlan(plan);
    activateTab("plan");
  } catch (error) {
    showError(error);
  }
}

async function loadQueryDetails() {
  await Promise.all([loadResults(), loadPlan(), loadMetrics(), loadAdvisor()]);
}

async function loadResults() {
  const page = await api(
    `/api/v1/queries/${state.queryId}/results?offset=${state.offset}&limit=${state.limit}`
  );
  state.nextOffset = page.next_offset;
  byId("result-meta").textContent =
    `${page.total_rows} 行 · 当前 ${page.offset + 1}-${page.offset + page.returned}`;
  const table = byId("result-table");
  table.replaceChildren();
  const head = document.createElement("thead");
  const headerRow = document.createElement("tr");
  for (const column of page.columns) {
    const cell = document.createElement("th");
    cell.textContent = column;
    headerRow.append(cell);
  }
  head.append(headerRow);
  const body = document.createElement("tbody");
  for (const row of page.rows) {
    const tableRow = document.createElement("tr");
    for (const column of page.columns) {
      const cell = document.createElement("td");
      cell.textContent = row[column] === null ? "NULL" : String(row[column]);
      tableRow.append(cell);
    }
    body.append(tableRow);
  }
  table.append(head, body);
  byId("previous-page").disabled = state.offset === 0;
  byId("next-page").disabled = page.next_offset === null;
}

async function loadPlan() {
  renderPlan(await api(`/api/v1/queries/${state.queryId}/plan`));
}

function renderPlan(plan) {
  byId("plan-output").textContent =
    `${plan.explain}\n\n== Physical Plan JSON ==\n${JSON.stringify(plan.physical_plan, null, 2)}`;
}

async function loadMetrics() {
  const metrics = await api(`/api/v1/queries/${state.queryId}/metrics`);
  byId("metrics-output").textContent = JSON.stringify(metrics.diagnostics, null, 2);
}

async function loadAdvisor() {
  const report = await api(`/api/v1/queries/${state.queryId}/advisor`);
  const root = byId("advisor-output");
  root.replaceChildren();
  const summary = document.createElement("p");
  summary.textContent = report.message;
  root.append(summary);
  for (const item of report.recommendations) {
    const row = document.createElement("div");
    row.className = "recommendation";
    const title = document.createElement("strong");
    title.textContent = `[${item.severity}] ${item.title}`;
    const detail = document.createElement("div");
    detail.textContent = `${item.cause} ${item.action} ${item.expected_impact}`;
    row.append(title, detail);
    root.append(row);
  }
}

function activateTab(name) {
  document.querySelectorAll(".tab").forEach((tab) => {
    tab.classList.toggle("active", tab.dataset.tab === name);
  });
  document.querySelectorAll(".panel").forEach((panel) => {
    panel.classList.toggle("active", panel.id === name);
  });
}

function showError(error) {
  byId("query-status").textContent = error.message;
  byId("query-status").className = "status error";
}

function openCatalogDialog(action, namespace = "default", table = "") {
  byId("catalog-action").value = action;
  byId("namespace-name").value = namespace;
  byId("table-name").value = table;
  updateCatalogForm();
  byId("catalog-dialog").showModal();
}

function updateCatalogForm() {
  const action = byId("catalog-action").value;
  byId("table-name-row").hidden = action === "namespace";
  byId("payload-row").hidden = action === "namespace";
  if (action === "table") {
    byId("catalog-payload").value =
      '{"schema":{"fields":[{"name":"id","data_type":"int64","nullable":false}]},"format":"parquet","location":"data/warehouse/orders"}';
  } else if (action === "import") {
    byId("catalog-payload").value =
      '{"source_location":"data/input/orders.csv","source_format":"csv","partition_count":2}';
  }
}

async function submitCatalog(event) {
  event.preventDefault();
  const action = byId("catalog-action").value;
  const namespace = encodeURIComponent(byId("namespace-name").value);
  const table = encodeURIComponent(byId("table-name").value);
  try {
    if (action === "namespace") {
      await api("/api/v1/catalog/namespaces", {
        method: "POST",
        body: JSON.stringify({ name: byId("namespace-name").value }),
      });
    } else {
      const payload = JSON.parse(byId("catalog-payload").value);
      if (action === "table") payload.name = byId("table-name").value;
      const suffix = action === "import" ? "/imports" : "";
      await api(`/api/v1/catalog/namespaces/${namespace}/tables${action === "table" ? "" : `/${table}${suffix}`}`, {
        method: "POST",
        body: JSON.stringify(payload),
      });
    }
    byId("catalog-message").textContent = "操作成功";
    await refreshCatalog();
  } catch (error) {
    byId("catalog-message").textContent = error.message;
    byId("catalog-message").className = "error";
  }
}

document.querySelectorAll(".tab").forEach((tab) => {
  tab.addEventListener("click", () => activateTab(tab.dataset.tab));
});
byId("run").addEventListener("click", runQuery);
byId("cancel").addEventListener("click", cancelQuery);
byId("explain").addEventListener("click", explainQuery);
byId("refresh-all").addEventListener("click", () => Promise.all([refreshCatalog(), refreshNodes()]));
byId("add-namespace").addEventListener("click", () => openCatalogDialog("namespace", ""));
byId("close-dialog").addEventListener("click", () => byId("catalog-dialog").close());
byId("catalog-action").addEventListener("change", updateCatalogForm);
byId("catalog-form").addEventListener("submit", submitCatalog);
byId("previous-page").addEventListener("click", async () => {
  state.offset = Math.max(0, state.offset - state.limit);
  await loadResults();
});
byId("next-page").addEventListener("click", async () => {
  if (state.nextOffset !== null) {
    state.offset = state.nextOffset;
    await loadResults();
  }
});
byId("sql").addEventListener("keydown", (event) => {
  if (event.ctrlKey && event.key === "Enter") {
    event.preventDefault();
    runQuery();
  }
});

Promise.all([refreshCatalog(), refreshNodes()]);
setInterval(refreshNodes, 5000);
