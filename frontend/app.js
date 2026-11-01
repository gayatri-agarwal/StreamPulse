"use strict";

// Keep the backend origin in one place. Use the same API port as the README command.
const API_BASE_URL = "http://127.0.0.1:8000";
const REFRESH_INTERVAL_MS = 10_000;

const apiStatus = document.getElementById("api-status");
const rabbitStatus = document.getElementById("rabbit-status");
const redisStatus = document.getElementById("redis-status");
const metricsMessage = document.getElementById("metrics-message");
const metricsUpdated = document.getElementById("metrics-updated");
const eventsChartMessage = document.getElementById("events-chart-message");
const latencyChartMessage = document.getElementById("latency-chart-message");
const eventsMessage = document.getElementById("events-message");
const eventsBody = document.getElementById("events-body");
const eventsCount = document.getElementById("events-count");
const actionForm = document.getElementById("action-form");
const actionMessage = document.getElementById("action-message");

let activeRefresh = null;
let refreshTimer = null;
let eventsChart = null;
let latencyChart = null;
let latestMetrics = { tumbling: null, sliding: null };

function formatNumber(value) {
  if (value === null || value === undefined || !Number.isFinite(Number(value))) return "-";
  return new Intl.NumberFormat(undefined, { maximumSignificantDigits: 6 }).format(Number(value));
}

function setApiStatus(available, label) {
  apiStatus.className = `status-pill ${available ? "status-online" : "status-offline"}`;
  apiStatus.querySelector("span:last-child").textContent = label;
}

function setServiceStatus(element, name, value) {
  const known = ["available", "unavailable", "not_configured", "not_verified"];
  const normalized = known.includes(value) ? value : "not_verified";
  const readable = {
    available: "Connected",
    unavailable: "Unavailable",
    not_configured: "Not configured",
    not_verified: "Not verified",
  }[normalized];
  element.innerHTML = `${name} <b></b>`;
  element.querySelector("b").textContent = readable;
  element.classList.toggle("service-unavailable", normalized === "unavailable");
}

async function getJson(path) {
  const response = await fetch(`${API_BASE_URL}${path}`, {
    headers: { Accept: "application/json" },
    cache: "no-store",
  });
  let body;
  try {
    body = await response.json();
  } catch {
    body = null;
  }
  if (!response.ok) {
    const detail = typeof body?.detail === "string" ? body.detail : `Request failed (${response.status})`;
    throw new Error(detail);
  }
  return body;
}

async function loadHealth() {
  try {
    const health = await getJson("/api/health");
    const online = health?.status === "available";
    setApiStatus(online, online ? "API online" : "API unavailable");
    setServiceStatus(rabbitStatus, "RabbitMQ", health?.services?.rabbitmq);
    setServiceStatus(redisStatus, "Redis", health?.services?.redis);
    return online;
  } catch {
    setApiStatus(false, "API unavailable");
    setServiceStatus(rabbitStatus, "RabbitMQ", "unavailable");
    setServiceStatus(redisStatus, "Redis", "unavailable");
    return false;
  }
}

function showMetricsUnavailable(message) {
  latestMetrics = { tumbling: null, sliding: null };
  document.querySelectorAll(".metric-card").forEach((card) => {
    card.querySelectorAll("[data-metric]").forEach((field) => { field.textContent = "-"; });
    card.querySelector("[data-window-state]").textContent = "Metrics unavailable";
  });
  metricsMessage.textContent = message;
  metricsMessage.className = "inline-message error";
  metricsUpdated.textContent = "Not live";
  updateCharts();
}

function renderMetricCard(windowType, metrics) {
  const card = document.querySelector(`[data-window="${windowType}"]`);
  const isTumbling = windowType === "tumbling";
  const fields = {
    count: metrics.count,
    sum: metrics.sum,
    average: metrics.average,
    minimum: metrics.minimum,
    maximum: metrics.maximum,
  };
  Object.entries(fields).forEach(([name, value]) => {
    card.querySelector(`[data-metric="${name}"]`).textContent = formatNumber(value);
  });
  card.querySelector("[data-window-state]").textContent = isTumbling
    ? "Latest completed window"
    : "Latest rolling window";
}

async function loadMetrics() {
  try {
    const data = await getJson("/api/metrics");
    if (!data || !("tumbling" in data) || !("sliding" in data)) {
      throw new Error("The metrics response did not contain both windows.");
    }
    latestMetrics = { tumbling: data.tumbling, sliding: data.sliding };
    for (const windowType of ["tumbling", "sliding"]) {
      const metrics = latestMetrics[windowType];
      if (metrics) renderMetricCard(windowType, metrics);
      else {
        const card = document.querySelector(`[data-window="${windowType}"]`);
        card.querySelectorAll("[data-metric]").forEach((field) => { field.textContent = "-"; });
        card.querySelector("[data-window-state]").textContent = windowType === "tumbling"
          ? "Waiting for first complete window"
          : "Waiting for first event";
      }
    }
    const hasMetrics = Boolean(data.tumbling || data.sliding);
    metricsMessage.textContent = hasMetrics ? "Metrics reflect the latest stored windows." : "No window metrics have been stored yet.";
    metricsMessage.className = "inline-message";
    metricsUpdated.textContent = `Updated ${new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" })}`;
    updateCharts();
  } catch (error) {
    showMetricsUnavailable(`Live metrics unavailable: ${error.message}`);
    setServiceStatus(redisStatus, "Redis", "unavailable");
  }
}

function numericMetric(value) {
  if (value === null || value === undefined || value === "") return null;
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : null;
}

function updateOneChart(existingChart, canvasId, messageElement, labels, metricNames, axisTitle, formatter) {
  const windowValues = (windowType) => {
    const metrics = latestMetrics[windowType];
    return metricNames.map((name) => numericMetric(metrics?.[name]));
  };
  const values = [windowValues("tumbling"), windowValues("sliding")];
  const hasData = values.some((series) => series.some((value) => value !== null));
  messageElement.textContent = hasData
    ? "Showing values returned by the metrics API. Missing window values are left blank."
    : "No values are available for this chart yet.";
  messageElement.className = "chart-message";
  const data = {
    labels,
    datasets: [
      { label: "Tumbling", data: metricNames.length === 1 && labels.length === 2 ? [values[0][0], null] : values[0], backgroundColor: "#7057eb", borderRadius: 5, maxBarThickness: 34 },
      { label: "Sliding", data: metricNames.length === 1 && labels.length === 2 ? [null, values[1][0]] : values[1], backgroundColor: "#22b7b2", borderRadius: 5, maxBarThickness: 34 },
    ],
  };
  const options = {
    responsive: true,
    maintainAspectRatio: false,
    animation: { duration: 250 },
    plugins: {
      legend: { display: false },
      tooltip: {
        mode: "index",
        intersect: false,
        callbacks: { label: (context) => `${context.dataset.label}: ${context.parsed.y === null ? "No data" : formatter(context.parsed.y)}` },
      },
    },
    scales: {
      x: {
        grid: { display: false },
        title: { display: true, text: "Window", color: "#738098", font: { family: "DM Sans", size: 12, weight: "600" } },
        ticks: { color: "#59647c", font: { family: "DM Sans", size: 11 } },
      },
      y: {
        beginAtZero: true,
        grid: { color: "#edf0f5" },
        title: { display: true, text: axisTitle, color: "#738098", font: { family: "DM Sans", size: 12, weight: "600" } },
        ticks: { color: "#59647c", font: { family: "DM Sans", size: 11 }, callback: formatter },
      },
    },
  };
  if (!existingChart) return new Chart(document.getElementById(canvasId), { type: "bar", data, options });
  existingChart.data = data;
  existingChart.options = options;
  existingChart.update();
  return existingChart;
}

function updateCharts() {
  if (typeof Chart === "undefined") {
    for (const message of [eventsChartMessage, latencyChartMessage]) {
      message.textContent = "Chart.js could not load. Metric cards above still show API data.";
      message.className = "chart-message error";
    }
    return;
  }
  const formatCount = (value) => new Intl.NumberFormat(undefined, { maximumFractionDigits: 0 }).format(value);
  const formatMilliseconds = (value) => `${new Intl.NumberFormat(undefined, { maximumSignificantDigits: 6 }).format(value)} ms`;
  eventsChart = updateOneChart(eventsChart, "events-chart", eventsChartMessage,
    ["Tumbling", "Sliding"], ["count"], "Events", formatCount);
  latencyChart = updateOneChart(latencyChart, "latency-chart", latencyChartMessage,
    ["Latency sum", "Average", "Minimum", "Maximum"], ["sum", "average", "minimum", "maximum"],
    "Latency (ms)", formatMilliseconds);
}

function formatTimestamp(value) {
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? "Unknown time" : date.toLocaleString();
}

function renderEvents(events) {
  eventsBody.replaceChildren();
  if (events.length === 0) {
    const row = document.createElement("tr");
    row.className = "placeholder-row";
    const cell = document.createElement("td");
    cell.colSpan = 4;
    cell.textContent = "No recent library events yet. Submit an action to get started.";
    row.append(cell);
    eventsBody.append(row);
    return;
  }
  for (const event of events) {
    const row = document.createElement("tr");
    const typeCell = document.createElement("td");
    const typeTag = document.createElement("span");
    typeTag.className = "event-type";
    typeTag.textContent = String(event.event_type ?? "Unknown event").replaceAll("_", " ");
    typeCell.append(typeTag);
    row.append(typeCell);
    for (const value of [event.student_id, event.book_id, formatTimestamp(event.timestamp)]) {
      const cell = document.createElement("td");
      cell.textContent = value === null || value === undefined ? "-" : String(value);
      row.append(cell);
    }
    eventsBody.append(row);
  }
}

async function loadEvents() {
  try {
    const data = await getJson("/api/events");
    if (!data || !Array.isArray(data.events)) throw new Error("The recent events response was invalid.");
    renderEvents(data.events);
    eventsCount.textContent = `${data.count ?? data.events.length} events`;
    eventsMessage.textContent = data.events.length ? `Showing ${data.events.length} recent event${data.events.length === 1 ? "" : "s"}.` : "No events recorded yet.";
    eventsMessage.className = "inline-message";
  } catch (error) {
    renderEvents([]);
    eventsCount.textContent = "Unavailable";
    eventsMessage.textContent = `Recent events unavailable: ${error.message}`;
    eventsMessage.className = "inline-message error";
  }
}

async function refreshLiveData() {
  if (activeRefresh) return activeRefresh;
  activeRefresh = Promise.allSettled([loadHealth(), loadMetrics(), loadEvents()]);
  try {
    await activeRefresh;
  } finally {
    activeRefresh = null;
  }
}

function scheduleRefresh() {
  window.clearTimeout(refreshTimer);
  if (document.hidden) return;
  refreshTimer = window.setTimeout(async () => {
    await refreshLiveData();
    scheduleRefresh();
  }, REFRESH_INTERVAL_MS);
}

function showActionMessage(message, kind) {
  actionMessage.textContent = message;
  actionMessage.className = `message-box ${kind}`;
  actionMessage.hidden = false;
}

actionForm.addEventListener("click", (event) => {
  const button = event.target.closest("button[data-event-type]");
  if (button) actionForm.dataset.selectedEventType = button.dataset.eventType;
});

actionForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  if (actionForm.dataset.submitting === "true") return;
  const studentId = document.getElementById("student-id").value.trim();
  const bookId = document.getElementById("book-id").value.trim();
  const eventType = actionForm.dataset.selectedEventType;
  if (!studentId || !bookId) {
    showActionMessage("Enter both a student ID and a book ID.", "error");
    return;
  }
  if (!eventType) {
    showActionMessage("Choose an action to continue.", "error");
    return;
  }

  actionForm.dataset.submitting = "true";
  const buttons = [...actionForm.querySelectorAll("button[data-event-type]")];
  buttons.forEach((button) => { button.disabled = true; });
  showActionMessage("Sending event to the API...", "loading");
  try {
    const submission = { event_type: eventType, student_id: studentId, book_id: bookId };
    const response = await fetch(`${API_BASE_URL}/api/events`, {
      method: "POST",
      headers: { "Content-Type": "application/json", Accept: "application/json" },
      body: JSON.stringify(submission),
    });
    let body;
    try { body = await response.json(); } catch { body = null; }
    if (!response.ok) {
      throw new Error(typeof body?.detail === "string" ? body.detail : `Publishing failed (${response.status}).`);
    }
    if (body?.status !== "published" || !body?.event?.event_id) {
      throw new Error("The API did not confirm that the event was published.");
    }
    showActionMessage(`${eventType.replaceAll("_", " ")} published for ${studentId} - ${bookId}.`, "success");
    await refreshLiveData();
  } catch (error) {
    showActionMessage(`Could not publish the event: ${error.message}`, "error");
  } finally {
    actionForm.dataset.submitting = "false";
    buttons.forEach((button) => { button.disabled = false; });
  }
});

document.addEventListener("visibilitychange", () => {
  if (document.hidden) window.clearTimeout(refreshTimer);
  else {
    void refreshLiveData();
    scheduleRefresh();
  }
});
window.addEventListener("pagehide", () => window.clearTimeout(refreshTimer));

void refreshLiveData().finally(scheduleRefresh);
