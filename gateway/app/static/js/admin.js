let adminInterval = null;

async function loadEvents() {
  try {
    const r = await fetch("http://localhost:8001/events?limit=50");
    if (!r.ok) {
      document.getElementById("eventsBody").innerHTML = `<tr><td colspan="5" style="text-align:center;padding:24px;color:var(--gray-400)">Event service not reachable (port 8001)</td></tr>`;
      return;
    }
    const events = await r.json();
    const tbody = document.getElementById("eventsBody");
    if (events.length === 0) {
      tbody.innerHTML = `<tr><td colspan="5" style="text-align:center;padding:24px;color:var(--gray-400)">No events yet</td></tr>`;
      return;
    }
    tbody.innerHTML = events.map(e => {
      const meta = e._meta || {};
      return `<tr>
        <td>${escapeHtml(e.event_type || "-")}</td>
        <td>${escapeHtml(JSON.stringify(e).slice(0, 80))}${JSON.stringify(e).length > 80 ? "…" : ""}</td>
        <td>${escapeHtml(meta.topic || "-")}</td>
        <td>${meta.offset !== undefined ? meta.offset : "-"}</td>
        <td>${meta.consumed_at ? new Date(meta.consumed_at + "Z").toLocaleTimeString() : "-"}</td>
      </tr>`;
    }).join("");
  } catch (err) {
    document.getElementById("eventsBody").innerHTML = `<tr><td colspan="5" style="text-align:center;padding:24px;color:var(--gray-400)">Error loading events</td></tr>`;
  }
}

async function loadEventCount() {
  try {
    const r = await fetch("http://localhost:8001/events/count");
    if (r.ok) {
      const data = await r.json();
      document.getElementById("eventCount").textContent = data.total || 0;
    }
  } catch {}
}

function escapeHtml(str) {
  if (str === null || str === undefined) return "";
  const d = document.createElement("div");
  d.textContent = String(str);
  return d.innerHTML;
}

document.addEventListener("DOMContentLoaded", () => {
  loadEvents();
  loadEventCount();
  adminInterval = setInterval(() => { loadEvents(); loadEventCount(); }, 5000);
});
