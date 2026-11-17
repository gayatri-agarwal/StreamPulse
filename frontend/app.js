"use strict";

const API_BASE_URL = "http://127.0.0.1:8000";
const REFRESH_INTERVAL_MS = 5_000;
const MONITORING_INTERVAL_TICKS = 3;
const MODE_COPY = {
  steady: {
    label: "Steady Flow",
    tag: "STEADY FLOW",
    explanation: "A consistent interval makes the input rate easier to compare with the consumer's processing rate.",
  },
  bursty: {
    label: "Bursty Flow",
    tag: "BURSTY FLOW",
    explanation: "A rapid batch can temporarily outpace the consumer. Watch ready messages rise, then fall as queued work is processed.",
  },
  late_arriving: {
    label: "Late-Arriving Events",
    tag: "LATE-ARRIVING EVENTS",
    explanation: "Events carry older event timestamps and can arrive out of event-time order. The consumer still processes them in RabbitMQ arrival order.",
  },
};

const elements = {
  labPage: document.getElementById("lab-page"),
  failurePage: document.getElementById("failure-page"),
  navLab: document.getElementById("nav-lab"),
  navRecovery: document.getElementById("nav-recovery"),
  apiStatus: document.getElementById("api-status"),
  rabbitStatus: document.getElementById("rabbit-status"),
  redisStatus: document.getElementById("redis-status"),
  modeCards: [...document.querySelectorAll(".mode-card")],
  modeExplainerTag: document.getElementById("mode-explainer-tag"),
  modeExplanation: document.getElementById("mode-explanation"),
  simulationForm: document.getElementById("simulation-form"),
  startButton: document.getElementById("start-simulation"),
  stopButton: document.getElementById("stop-simulation"),
  simulationMessage: document.getElementById("simulation-message"),
  activeState: document.getElementById("active-state"),
  simulationModeLabel: document.getElementById("simulation-mode-label"),
  generatedCount: document.getElementById("generated-count"),
  publishedCount: document.getElementById("published-count"),
  failedCount: document.getElementById("failed-count"),
  startedAt: document.getElementById("started-at"),
  lastUpdated: document.getElementById("last-updated"),
  architectureUpdated: document.getElementById("architecture-updated"),
  activityPanel: document.querySelector(".activity-panel"),
  activityBody: document.getElementById("activity-body"),
  activityMessage: document.getElementById("activity-message"),
  activityCount: document.getElementById("activity-count"),
  lateLimitation: document.getElementById("late-limitation"),
  monitoringUpdated: document.getElementById("monitoring-updated"),
  queueReady: document.getElementById("queue-ready"),
  queueName: document.getElementById("queue-name"),
  consumerCount: document.getElementById("consumer-count"),
  consumerState: document.getElementById("consumer-state"),
  processedCount: document.getElementById("processed-count"),
  processingFailures: document.getElementById("processing-failures"),
  throughput: document.getElementById("throughput"),
  publishLatency: document.getElementById("publish-latency"),
  processingLatency: document.getElementById("processing-latency"),
  aggregationMessage: document.getElementById("aggregation-message"),
};

let selectedMode = "steady";
let simulation = null;
let simulationStatus = null;
let monitoring = null;
let health = null;
let pollTimer = null;
let pollInFlight = false;
let pollCount = 0;
let actionInFlight = false;
let lastActivityError = null;

function currentPage() {
  const failure = window.location.pathname.replace(/\/+$/, "") === "/failure-recovery";
  elements.labPage.hidden = failure;
  elements.failurePage.hidden = !failure;
  elements.navLab.removeAttribute("aria-current");
  elements.navRecovery.removeAttribute("aria-current");
  (failure ? elements.navRecovery : elements.navLab).setAttribute("aria-current", "page");
  return failure ? "recovery" : "lab";
}

function formatValue(value, suffix = "") {
  if (value === null || value === undefined || value === "") return "Unavailable";
  if (typeof value === "number" && Number.isFinite(value)) {
    return `${new Intl.NumberFormat(undefined, { maximumFractionDigits: 2 }).format(value)}${suffix}`;
  }
  return `${String(value)}${suffix}`;
}

function formatTimestamp(value) {
  if (!value) return "Unavailable";
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? "Invalid timestamp" : date.toLocaleString();
}

function setServiceChip(element, name, status) {
  const normalized = String(status || "not_verified").toLowerCase();
  let className = "is-checking";
  let label = "Checking";
  if (["available", "running", "connected", "active"].includes(normalized)) {
    className = "is-healthy";
    label = normalized === "running" || normalized === "active" ? "Running" : "Available";
  } else if (["unavailable", "disconnected", "not_configured", "error"].includes(normalized)) {
    className = "is-error";
    label = normalized === "not_configured" ? "Not configured" : "Unavailable";
  } else if (["stale", "stopped"].includes(normalized)) {
    className = "is-unverified";
    label = normalized === "stale" ? "Stale" : "Stopped";
  } else if (normalized === "not_verified") {
    label = "Not verified";
  }
  element.className = `service-chip ${className}`;
  element.querySelector("span").textContent = `${name} ${label}`;
}

function componentState(element, label, state) {
  element.querySelector("[data-node-status]").textContent = label;
  element.dataset.state = state;
}

function monitoringStatusLabel(value) {
  const labels = {
    available: "Available",
    unavailable: "Unavailable",
    not_configured: "Not configured",
    not_verified: "Not verified",
    running: "Running",
    stale: "Stale heartbeat",
    stopped: "Stopped",
  };
  return labels[value] || "Not reported";
}

function updateHeaderStatus() {
  setServiceChip(elements.apiStatus, "API", health?.status || "not_verified");
  setServiceChip(elements.rabbitStatus, "RabbitMQ", monitoring?.rabbitmq?.status);
  setServiceChip(elements.redisStatus, "Redis", monitoring?.api?.redis_status);
}

function updateArchitecture() {
  const nodes = Object.fromEntries([...document.querySelectorAll(".architecture-node")].map((node) => [node.dataset.node, node]));
  const sessionState = simulation?.state || "idle";
  const modeLabel = MODE_COPY[simulation?.flow_mode]?.label || MODE_COPY[selectedMode].label;
  const sessionActive = sessionState === "running" || sessionState === "stopping";
  componentState(nodes.generator,
    simulation ? `${modeLabel} · ${sessionState}` : "Idle · no active simulation",
    sessionState === "running" ? "active" : sessionState === "failed" ? "error" : "waiting");

  const apiState = health?.status === "available" ? "healthy" : health?.status === "unavailable" ? "error" : "waiting";
  componentState(nodes.api,
    health?.status === "available" ? "API available" : health?.status ? "API unavailable" : "Checking API",
    apiState);

  const queue = simulationStatus?.queue;
  const queueReady = queue?.messages_ready;
  const queueConsumers = queue?.consumer_count;
  const rabbitStatus = monitoring?.rabbitmq?.status;
  const queueLabel = queueReady !== null && queueReady !== undefined
    ? `${formatValue(queueReady)} ready · ${formatValue(queueConsumers)} consumers`
    : monitoringStatusLabel(rabbitStatus);
  componentState(nodes.rabbitmq, queueLabel,
    rabbitStatus === "available" ? (Number(queueReady) > 0 ? "backlog" : "healthy")
      : rabbitStatus === "unavailable" || rabbitStatus === "not_configured" ? "error" : "waiting");

  const consumerStatus = monitoring?.consumer?.status || simulationStatus?.consumer?.status;
  componentState(nodes.consumer,
    consumerStatus === "running" ? "Running · heartbeat fresh" : monitoringStatusLabel(consumerStatus),
    consumerStatus === "running" ? "healthy"
      : consumerStatus === "stale" ? "error" : "waiting");

  const windows = simulationStatus?.aggregations;
  const hasWindows = windows && (windows.tumbling || windows.sliding);
  const processingFailures = simulationStatus?.measurements?.processing_failures || 0;
  const hasSimulation = Boolean(simulationStatus?.simulation);
  componentState(nodes.windows,
    hasWindows ? "Simulation summaries available"
      : !hasSimulation ? "No simulation session"
        : processingFailures > 0 ? `${formatValue(processingFailures)} processing failures`
          : windows === null ? "Summary storage unavailable" : "Waiting for arrivals",
    hasWindows ? "healthy" : !hasSimulation ? "waiting"
      : processingFailures > 0 || windows === null ? "error" : "waiting");

  const redisStatus = monitoring?.api?.redis_status;
  componentState(nodes.redis, monitoringStatusLabel(redisStatus),
    redisStatus === "available" ? "healthy"
      : redisStatus === "unavailable" || redisStatus === "not_configured" ? "error" : "waiting");
  componentState(nodes.dashboard, health ? "Connected to API" : "Checking API", health ? "healthy" : "waiting");
  elements.architectureUpdated.textContent = monitoring ? `Monitoring observed ${new Date().toLocaleTimeString()}` : "Live status unavailable";
  updateHeaderStatus();
  document.body.classList.toggle("simulation-active", sessionActive);
}

function renderMode(mode) {
  if (!MODE_COPY[mode]) return;
  selectedMode = mode;
  elements.modeCards.forEach((card) => {
    const selected = card.dataset.mode === mode;
    card.classList.toggle("is-selected", selected);
    card.setAttribute("aria-pressed", String(selected));
    card.disabled = Boolean(simulation && ["running", "stopping"].includes(simulation.state));
  });
  document.querySelectorAll(".mode-fields").forEach((panel) => {
    panel.hidden = panel.dataset.fields !== mode;
  });
  elements.modeExplainerTag.textContent = MODE_COPY[mode].tag;
  elements.modeExplanation.textContent = MODE_COPY[mode].explanation;
  elements.lateLimitation.hidden = mode !== "late_arriving";
  document.body.classList.toggle("late-mode", mode === "late_arriving");
  elements.simulationModeLabel.textContent = simulation
    ? `${MODE_COPY[simulation.flow_mode]?.label || simulation.flow_mode} · ${["running", "stopping"].includes(simulation.state) ? "active session" : "last session"}`
    : `${MODE_COPY[selectedMode].label} · ready`;
  renderActivity();
}

function updateSimulationControls() {
  const state = simulation?.state || "idle";
  const active = state === "running" || state === "stopping";
  elements.activeState.textContent = state === "idle" ? "Idle" : state;
  elements.activeState.className = `state-badge is-${state}`;
  elements.startButton.disabled = actionInFlight || active;
  elements.stopButton.disabled = actionInFlight || state !== "running";
  elements.modeCards.forEach((card) => { card.disabled = actionInFlight || active; });
  document.querySelectorAll(".mode-fields input").forEach((input) => { input.disabled = actionInFlight || active; });
  elements.simulationModeLabel.textContent = simulation
    ? `${MODE_COPY[simulation.flow_mode]?.label || simulation.flow_mode} · ${active ? "active session" : "last session"}`
    : `${MODE_COPY[selectedMode].label} · ready`;
  elements.generatedCount.textContent = simulation ? formatValue(simulation.generated_count) : "—";
  elements.publishedCount.textContent = simulation ? formatValue(simulation.publish_confirmed_count) : "—";
  elements.failedCount.textContent = simulation ? formatValue(simulation.publish_failed_count) : "—";
  elements.startedAt.textContent = simulation ? formatTimestamp(simulation.started_at) : "—";
  elements.lastUpdated.textContent = simulation
    ? `Last updated ${formatTimestamp(simulation.last_updated_at)}${simulation.stopped_at ? ` · stopped ${formatTimestamp(simulation.stopped_at)}` : ""}`
    : "Status has not been loaded.";
  if (simulation && ["running", "stopping"].includes(simulation.state)
      && MODE_COPY[simulation.flow_mode]) renderMode(simulation.flow_mode);
}

function clearSimulationMessage() {
  elements.simulationMessage.hidden = true;
  elements.simulationMessage.textContent = "";
  elements.simulationMessage.className = "feedback";
}

function showSimulationMessage(message, isError = false) {
  elements.simulationMessage.hidden = false;
  elements.simulationMessage.textContent = message;
  elements.simulationMessage.className = `feedback ${isError ? "is-error" : "is-success"}`;
}

async function requestJson(path, options = {}) {
  const response = await fetch(`${API_BASE_URL}${path}`, {
    cache: "no-store",
    headers: { Accept: "application/json", ...(options.body ? { "Content-Type": "application/json" } : {}) },
    ...options,
  });
  let data = null;
  try { data = await response.json(); } catch { /* Keep response details unavailable. */ }
  if (!response.ok) {
    throw new Error(typeof data?.detail === "string" ? data.detail : `Request failed (${response.status}).`);
  }
  return data;
}

function setText(element, value, suffix = "") {
  element.textContent = value === null || value === undefined ? "Unavailable" : formatValue(value, suffix);
}

function renderMonitoring() {
  const queue = simulationStatus?.queue;
  const reportedQueue = monitoring?.rabbitmq;
  const ready = queue?.messages_ready ?? reportedQueue?.messages_ready;
  const consumers = queue?.consumer_count ?? reportedQueue?.consumer_count;
  const qName = reportedQueue?.queue_name;
  setText(elements.queueReady, ready);
  elements.queueName.textContent = qName ? `Queue: ${qName}` : "Queue name unavailable";
  setText(elements.consumerCount, consumers);

  const consumerStatus = monitoring?.consumer?.status || simulationStatus?.consumer?.status;
  elements.consumerState.textContent = consumerStatus
    ? monitoringStatusLabel(consumerStatus)
    : "Consumer status unavailable";
  const metrics = simulationStatus?.measurements;
  setText(elements.processedCount, metrics?.processed);
  setText(elements.processingFailures, metrics?.processing_failures);
  setText(elements.throughput, metrics?.throughput_events_per_second, " events/s");
  const averages = metrics?.average_latency_ms;
  const p95 = metrics?.p95_latency_ms;
  const publishMean = averages?.publish_latency_ms;
  const processMean = averages?.consumer_processing_latency_ms;
  elements.publishLatency.textContent = publishMean === null || publishMean === undefined
    ? "Unavailable" : `${formatValue(publishMean, " ms")} · P95 ${formatValue(p95?.publish_latency_ms, " ms")}`;
  elements.processingLatency.textContent = processMean === null || processMean === undefined
    ? "Unavailable" : `${formatValue(processMean, " ms")} · P95 ${formatValue(p95?.consumer_processing_latency_ms, " ms")}`;
  elements.monitoringUpdated.textContent = monitoring || simulationStatus
    ? `Updated ${new Date().toLocaleTimeString()}` : "Monitoring unavailable";
}

function metricAt(aggregations, windowName, fieldName) {
  return aggregations?.[windowName]?.[fieldName];
}

function renderAggregations() {
  const aggregations = simulationStatus?.aggregations;
  for (const windowName of ["tumbling", "sliding"]) {
    for (const fieldName of ["count", "sum", "average", "minimum", "maximum"]) {
      const field = document.querySelector(`[data-window-value="${windowName}.${fieldName}"]`);
      const value = metricAt(aggregations, windowName, fieldName);
      field.textContent = value === null || value === undefined ? "Unavailable" : formatValue(value);
    }
  }
  const hasSummary = aggregations && (aggregations.tumbling || aggregations.sliding);
  if (hasSummary) {
    elements.aggregationMessage.textContent = "Showing the latest simulation-scoped summaries returned by Redis.";
    elements.aggregationMessage.className = "table-state";
  } else if (!simulationStatus?.simulation) {
    elements.aggregationMessage.textContent = "Start a simulation to view its window summaries.";
    elements.aggregationMessage.className = "table-state";
  } else if (aggregations === null) {
    elements.aggregationMessage.textContent = "Simulation summaries are unavailable from Redis.";
    elements.aggregationMessage.className = "table-state is-error";
  } else {
    elements.aggregationMessage.textContent = "No simulation summaries have been stored yet. Tumbling waits for a full group; sliding updates at its configured event interval.";
    elements.aggregationMessage.className = "table-state";
  }
}

function makeCell(text, className = "") {
  const cell = document.createElement("td");
  if (className) cell.className = className;
  cell.textContent = text;
  return cell;
}

function addTimestampCell(row, value, className = "") {
  const cell = makeCell(value ? formatTimestamp(value) : "Not recorded", `time-cell ${className}`.trim());
  if (value) cell.title = value;
  row.append(cell);
}

function eventTimeOf(event) {
  return event.event_time || event.timestamp || null;
}

function renderActivity() {
  const activity = window.lastSimulationActivity;
  elements.activityBody.replaceChildren();
  if (!activity) {
    elements.activityCount.textContent = "Activity unavailable";
    elements.activityMessage.textContent = lastActivityError || "Start a simulation to see generated events.";
    elements.activityMessage.className = `table-state${lastActivityError ? " is-error" : ""}`;
    return;
  }
  const records = Array.isArray(activity.events) ? [...activity.events] : [];
  records.sort((a, b) => new Date(a.published_at || 0) - new Date(b.published_at || 0));
  const visible = records.slice(-30);
  elements.activityCount.textContent = records.length
    ? `Showing ${visible.length} of ${activity.count ?? records.length} recent generated events`
    : "No events generated";
  elements.activityMessage.textContent = lastActivityError
    ? `Activity refresh failed: ${lastActivityError}`
    : records.length ? "Events are ordered by publish time. Processing time appears only after a successful consumer completion is recorded." : "No events generated in this simulation yet.";
  elements.activityMessage.className = `table-state${lastActivityError ? " is-error" : ""}`;
  let previousEventTimestamp = null;
  for (const event of visible) {
    const row = document.createElement("tr");
    const eventCell = makeCell("");
    const shortId = typeof event.event_id === "string" ? `${event.event_id.slice(0, 8)}…` : "Unknown ID";
    const id = document.createElement("span");
    id.className = "event-id";
    id.textContent = shortId;
    id.title = event.event_id || "";
    eventCell.textContent = "";
    eventCell.append(id);
    const eventTimestamp = eventTimeOf(event);
    const eventMs = eventTimestamp ? new Date(eventTimestamp).getTime() : NaN;
    const isOutOfOrder = Number.isFinite(eventMs) && previousEventTimestamp !== null && eventMs < previousEventTimestamp;
    if (Number.isFinite(eventMs)) previousEventTimestamp = eventMs;
    if (selectedMode === "late_arriving") {
      const lateBadge = document.createElement("span");
      lateBadge.className = isOutOfOrder ? "out-of-order-badge" : "late-badge";
      lateBadge.textContent = isOutOfOrder ? "Out of order" : "Late event time";
      eventCell.append(lateBadge);
    }
    row.append(eventCell);

    const typeCell = makeCell("");
    const type = document.createElement("span");
    type.className = "event-type-label";
    type.textContent = String(event.event_type || "Unknown event").replaceAll("_", " ");
    typeCell.append(type);
    row.append(typeCell);

    const knownProcessed = Boolean(event.processed_at);
    const effectiveStatus = knownProcessed ? "processed" : event.status || "unknown";
    const statusCell = makeCell("");
    const status = document.createElement("span");
    status.className = "event-status";
    status.dataset.status = effectiveStatus;
    status.textContent = effectiveStatus.replaceAll("_", " ");
    if (event.processing_error) status.title = `Processing error: ${event.processing_error}`;
    statusCell.append(status);
    row.append(statusCell);

    addTimestampCell(row, eventTimestamp, selectedMode === "late_arriving" ? "is-late" : "");
    addTimestampCell(row, event.published_at);
    addTimestampCell(row, event.processed_at);
    elements.activityBody.append(row);
  }
}

function setWindowSummaryError(error) {
  if (!simulationStatus) return;
  if (error) simulationStatus.aggregations = null;
}

async function refreshLiveData() {
  if (pollInFlight || currentPage() !== "lab" || document.hidden) return;
  pollInFlight = true;
  pollCount += 1;
  const shouldReadMonitoring = !monitoring || pollCount % MONITORING_INTERVAL_TICKS === 1;
  const tasks = [
    requestJson("/api/simulations/status"),
    requestJson("/api/simulations/activity"),
  ];
  if (shouldReadMonitoring) {
    tasks.push(requestJson("/api/health"), requestJson("/api/monitoring"));
  }
  try {
    const results = await Promise.allSettled(tasks);
    if (results[0].status === "fulfilled") {
      simulationStatus = results[0].value;
      simulation = simulationStatus.simulation || null;
      if (simulation && ["running", "stopping"].includes(simulation.state)
          && MODE_COPY[simulation.flow_mode]) selectedMode = simulation.flow_mode;
      setWindowSummaryError(false);
      updateSimulationControls();
      renderMonitoring();
      renderAggregations();
    } else {
      showSimulationMessage(`Simulation status unavailable: ${results[0].reason?.message || "API request failed."}`, true);
    }
    if (results[1].status === "fulfilled") {
      window.lastSimulationActivity = results[1].value;
      lastActivityError = null;
    } else {
      lastActivityError = results[1].reason?.message || "Activity API request failed.";
    }
    if (shouldReadMonitoring) {
      if (results[2]?.status === "fulfilled") health = results[2].value;
      if (results[3]?.status === "fulfilled") monitoring = results[3].value;
      if (results[2]?.status === "rejected" || results[3]?.status === "rejected") {
        if (results[2]?.status === "rejected") health = null;
        if (results[3]?.status === "rejected") monitoring = null;
      }
    }
    updateHeaderStatus();
    updateArchitecture();
    renderActivity();
    if (elements.simulationMessage.classList.contains("is-error") && results[0].status === "fulfilled") {
      clearSimulationMessage();
    }
  } finally {
    pollInFlight = false;
  }
}

function scheduleRefresh() {
  window.clearTimeout(pollTimer);
  if (document.hidden || currentPage() !== "lab") return;
  pollTimer = window.setTimeout(async () => {
    await refreshLiveData();
    scheduleRefresh();
  }, REFRESH_INTERVAL_MS);
}

function validateVisibleInputs() {
  const panel = document.querySelector(`.mode-fields[data-fields="${selectedMode}"]`);
  const inputs = [...panel.querySelectorAll("input")];
  for (const input of inputs) {
    if (!input.checkValidity()) {
      input.reportValidity();
      return false;
    }
  }
  return true;
}

function startPayload() {
  const payload = { flow_mode: selectedMode };
  const value = (id) => Number(document.getElementById(id).value);
  if (selectedMode === "steady") payload.interval_ms = value("steady-interval");
  if (selectedMode === "bursty") {
    payload.burst_size = value("burst-size");
    payload.burst_interval_ms = value("burst-interval");
  }
  if (selectedMode === "late_arriving") {
    payload.interval_ms = value("late-interval");
    payload.lateness_ms = value("lateness-ms");
    payload.out_of_order_every = value("out-of-order-every");
  }
  return payload;
}

elements.modeCards.forEach((card) => card.addEventListener("click", () => {
  if (simulation && ["running", "stopping"].includes(simulation.state)) return;
  renderMode(card.dataset.mode);
  clearSimulationMessage();
}));

elements.simulationForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  if (actionInFlight || (simulation && ["running", "stopping"].includes(simulation.state))) return;
  if (!validateVisibleInputs()) return;
  actionInFlight = true;
  clearSimulationMessage();
  updateSimulationControls();
  try {
    const response = await requestJson("/api/simulations/start", {
      method: "POST",
      body: JSON.stringify(startPayload()),
    });
    if (!response?.simulation?.simulation_id) throw new Error("The API accepted the request without returning a simulation ID.");
    simulation = response.simulation;
    simulationStatus = { ...(simulationStatus || {}), simulation };
    renderMode(simulation.flow_mode);
    showSimulationMessage(`${MODE_COPY[simulation.flow_mode]?.label || simulation.flow_mode} started. Publish confirmation and consumer processing are reported separately.`, false);
    await refreshLiveData();
  } catch (error) {
    showSimulationMessage(`Could not start simulation: ${error.message}`, true);
  } finally {
    actionInFlight = false;
    updateSimulationControls();
  }
});

elements.stopButton.addEventListener("click", async () => {
  if (actionInFlight || !simulation || simulation.state !== "running") return;
  actionInFlight = true;
  clearSimulationMessage();
  updateSimulationControls();
  try {
    const response = await requestJson("/api/simulations/stop", { method: "POST" });
    simulation = response?.simulation || simulation;
    simulationStatus = { ...(simulationStatus || {}), simulation };
    showSimulationMessage("Stop requested. The current publish will settle before the generator stops.", false);
    await refreshLiveData();
  } catch (error) {
    showSimulationMessage(`Could not stop simulation: ${error.message}`, true);
  } finally {
    actionInFlight = false;
    updateSimulationControls();
  }
});

document.addEventListener("visibilitychange", () => {
  if (document.hidden) window.clearTimeout(pollTimer);
  else if (currentPage() === "lab") {
    void refreshLiveData();
    scheduleRefresh();
  }
});

window.addEventListener("pagehide", () => window.clearTimeout(pollTimer));

function initialize() {
  const page = currentPage();
  if (page === "lab") {
    renderMode(selectedMode);
    updateSimulationControls();
    void refreshLiveData().finally(scheduleRefresh);
  } else {
    void requestJson("/api/health").then((data) => {
      health = data;
      setServiceChip(elements.apiStatus, "API", data?.status);
      setServiceChip(elements.rabbitStatus, "RabbitMQ", data?.services?.rabbitmq);
      setServiceChip(elements.redisStatus, "Redis", data?.services?.redis);
    }).catch(() => {
      setServiceChip(elements.apiStatus, "API", "unavailable");
      setServiceChip(elements.rabbitStatus, "RabbitMQ", "not_verified");
      setServiceChip(elements.redisStatus, "Redis", "not_verified");
    });
  }
}

initialize();
