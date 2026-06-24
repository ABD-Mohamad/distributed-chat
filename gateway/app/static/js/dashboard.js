let dashboardInterval = null;

async function loadDashboard() {
  try {
    const [health, strategies, metrics, cb] = await Promise.all([
      fetch("/health").then(r => r.json()),
      fetch("/strategies").then(r => r.json()),
      fetch("/metrics").then(r => r.json()),
      fetch("/circuit-breakers").then(r => r.json()),
    ]);
    renderStats(health, metrics, cb);
    renderBackends(metrics, cb);
    renderStrategies(strategies);
  } catch (err) {
    console.error("Dashboard load error:", err);
  }
}

function renderStats(health, metrics, cb) {
  const backends = Object.keys(metrics);
  const openCbs = Object.values(cb).filter(v => v === "open").length;
  const totalLatency = Object.values(metrics).reduce((sum, m) => sum + m.avg_latency, 0);
  const avgLatency = backends.length > 0 ? (totalLatency / backends.length) : 0;

  document.getElementById("statBackends").textContent = backends.length;
  const cbStatus = document.getElementById("statCircuitBreakers");
  cbStatus.textContent = openCbs === 0 ? "All Closed" : `${openCbs} Open`;
  cbStatus.className = "stat-value " + (openCbs === 0 ? "green" : openCbs < 2 ? "yellow" : "red");

  document.getElementById("statLatency").textContent = (avgLatency * 1000).toFixed(1) + " ms";
  document.getElementById("statLatency").className = "stat-value " + (avgLatency < 0.1 ? "green" : avgLatency < 0.5 ? "yellow" : "red");

  const totalReqs = Object.values(metrics).reduce((sum, m) => sum + m.request_count, 0);
  document.getElementById("statRequests").textContent = totalReqs;
}

function renderBackends(metrics, cb) {
  const tbody = document.getElementById("backendsBody");
  const backends = Object.keys(metrics);
  if (backends.length === 0) {
    tbody.innerHTML = "<tr><td colspan='6' style='text-align:center;padding:24px;color:var(--gray-400)'>No backends available</td></tr>";
    return;
  }
  tbody.innerHTML = backends.map(b => {
    const m = metrics[b];
    const breaker = cb[b] || "closed";
    const cbClass = breaker === "open" ? "red" : breaker === "half" ? "yellow" : "green";
    const cbLabel = breaker.charAt(0).toUpperCase() + breaker.slice(1);
    const errorRate = (m.error_rate * 100).toFixed(1);
    return `<tr>
      <td><span class="cb-indicator ${breaker}"></span> ${escapeHtml(b)}</td>
      <td>${m.active_connections}</td>
      <td>${(m.avg_latency * 1000).toFixed(1)} ms</td>
      <td>${errorRate}%</td>
      <td>${m.request_count}</td>
      <td><span class="status-badge ${cbClass}">${cbLabel}</span></td>
    </tr>`;
  }).join("");
}

function renderStrategies(strategies) {
  const sel = document.getElementById("strategySelect");
  const current = sel.value;
  sel.innerHTML = Object.entries(strategies.available).map(([key, desc]) =>
    `<option value="${key}" ${key === strategies.active ? "selected" : ""}>${desc}</option>`
  ).join("");
}

async function switchStrategy() {
  const name = document.getElementById("strategySelect").value;
  const r = await fetch(`/switch-strategy/${name}`, { method: "GET" });
  if (r.ok) {
    const data = await r.json();
    document.getElementById("activeStrategy").textContent = data.active_strategy;
  }
}

function escapeHtml(str) {
  const d = document.createElement("div");
  d.textContent = str;
  return d.innerHTML;
}

// Init
document.addEventListener("DOMContentLoaded", () => {
  loadDashboard();
  dashboardInterval = setInterval(loadDashboard, 5000);
});
