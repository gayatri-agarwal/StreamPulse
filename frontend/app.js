"use strict";

// Keep the backend origin in one place. Use the same API port as the README command.
const API_BASE_URL = "http://127.0.0.1:8000";
const REFRESH_INTERVAL_MS = 10_000;
const CHART_FONT_FAMILY = getComputedStyle(document.documentElement).getPropertyValue("--font-body").trim();

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
const sampleBatchSize = document.getElementById("sample-batch-size");
const generateSampleEventsButton = document.getElementById("generate-sample-events");
const sampleBatchMessage = document.getElementById("sample-batch-message");
const architectureNodes = [...document.querySelectorAll(".architecture-node")];
const architectureDetailTitle = document.getElementById("architecture-detail-title");
const architectureDetailStatus = document.getElementById("architecture-detail-status");
const architectureDetailDescription = document.getElementById("architecture-detail-description");
const architectureDetailInformation = document.getElementById("architecture-detail-information");
const windowConfigurationSection = document.getElementById("window-configuration");
const activeWindowConfiguration = document.getElementById("active-window-configuration");
const pendingWindowConfiguration = document.getElementById("pending-window-configuration");
const windowConfigurationForm = document.getElementById("window-configuration-form");
const tumblingWindowSizeSelect = document.getElementById("tumbling-window-size");
const slidingWindowSizeSelect = document.getElementById("sliding-window-size");
const slidingWindowIntervalSelect = document.getElementById("sliding-window-interval");
const applyWindowConfigurationButton = document.getElementById("apply-window-configuration");
const windowConfigurationMessage = document.getElementById("window-configuration-message");

let activeRefresh = null;
let refreshTimer = null;
let eventsChart = null;
let latencyChart = null;
let latestMetrics = { tumbling: null, sliding: null };
let metricsDataStatus = "loading";
let architectureState = {
  api: "checking",
  dashboard: "checking",
  rabbitmq: "not_verified",
  redis: "not_verified",
  consumer: "unavailable",
  windows: "unavailable",
};
let architectureMonitoringStatus = "loading";
let architectureMonitoringData = null;
let architectureMonitoringUpdatedAt = null;
let selectedArchitectureComponent = "generation";
let windowConfigurationStatus = "loading";
let windowConfigurationData = null;
let windowConfigurationDirty = false;
let windowConfigurationSubmitting = false;
let windowConfigurationRequestId = 0;

const architectureComponents = {
  generation: {
    name: "Library Event Generation",
    description: "Library actions enter the event flow here. The manual action form publishes individual events through the existing API.",
    status: generationStatusLabel,
    state: generationStatusState,
    information: ["Manual Event Generation: available through the existing library actions form.", "On-demand Random Batch Producer: generates a finite sample batch through the same event publishing flow; it is an input method, not a separate processing service."],
  },
  rabbitmq: {
    name: "CloudAMQP / RabbitMQ",
    description: "The API publisher sends library events to the configured RabbitMQ queue. Queue counts are point-in-time observations from the existing AMQP connection.",
    status: () => monitoringLoading() ? "Loading monitoring…" : monitoringFailed() ? "Monitoring unavailable" : architectureStatusLabel(architectureState.rabbitmq),
    state: () => monitoringFailed() ? "monitoring-unavailable" : architectureState.rabbitmq,
    information: rabbitmqInformation,
  },
  consumer: {
    name: "Python Stream Processor / Consumer",
    description: "A separate Python process consumes and validates queued events, then passes them into the window processor.",
    status: consumerStatusLabel,
    state: () => consumerStatusState(architectureMonitoringData?.consumer?.status),
    information: consumerInformation,
  },
  windows: {
    name: "Streaming Window Manager",
    description: "The in-memory window managers summarize consumer events before the summaries are stored in Redis.",
    status: windowStatusLabel,
    state: windowStatusState,
    information: windowInformation,
  },
  redis: {
    name: "Redis KV Store",
    description: "Redis stores the latest window summaries and consumer monitoring snapshot. API-side monitoring reads and consumer-side aggregation writes are reported separately.",
    status: redisStatusLabel,
    state: redisStatusState,
    information: redisInformation,
  },
  api: {
    name: "FastAPI REST API",
    description: "The API validates and publishes events, serves recent API-process event history, reads metrics, and reports health.",
    status: () => architectureState.api === "connected" ? "Available" : architectureState.api === "disconnected" ? "Unavailable" : "Checking",
    state: () => architectureState.api === "connected" ? "available" : architectureState.api,
    information: ["Availability is based on the dashboard's monitoring request."],
  },
  dashboard: {
    name: "Monitoring Dashboard",
    description: "Connection to the StreamPulse API.",
    status: () => architectureState.dashboard === "connected" ? "Connected" : architectureState.dashboard === "disconnected" ? "Disconnected" : "Checking",
    state: () => architectureState.dashboard,
    information: [],
  },
};

function monitoringLoading() {
  return architectureMonitoringStatus === "loading";
}

function monitoringFailed() {
  return architectureMonitoringStatus === "error";
}

function monitoringUnavailableInformation() {
  if (monitoringLoading()) return ["Loading component monitoring…"];
  if (monitoringFailed()) return ["The monitoring request failed; component status is unconfirmed."];
  return ["This component's monitoring data was not returned."];
}

function architectureStatusLabel(value) {
  return ({
    available: "Available",
    unavailable: "Unavailable",
    not_configured: "Not configured",
    not_verified: "Not verified",
    running: "Running",
    stale: "Stale heartbeat",
    stopped: "Stopped",
    connected: "Connected",
    disconnected: "Disconnected",
  })[value] || "Not reported";
}

function displayMonitoringValue(value, suffix = "") {
  if (value === null || value === undefined || value === "") return "Unavailable";
  return `${String(value)}${suffix}`;
}

function displayMonitoringTimestamp(value) {
  if (value === null || value === undefined || value === "") return "Unavailable";
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? "Invalid timestamp" : date.toLocaleString();
}

function monitoringResponseUpdatedLine() {
  return `Monitoring response updated: ${architectureMonitoringUpdatedAt?.toLocaleTimeString() || "Unavailable"}.`;
}

function staleSnapshotNote(consumer) {
  if (consumer?.status === "stale") {
    return `Heartbeat expired. Values below are from the last snapshot at ${displayMonitoringTimestamp(consumer.last_heartbeat)}.`;
  }
  if (consumer?.status === "stopped") {
    return `The consumer reported a graceful stop. Values below are from its final snapshot at ${displayMonitoringTimestamp(consumer.last_heartbeat)}.`;
  }
  return null;
}

function rabbitmqInformation() {
  if (monitoringLoading() || monitoringFailed()) return monitoringUnavailableInformation();
  const rabbitmq = architectureMonitoringData?.rabbitmq;
  if (!rabbitmq) return monitoringUnavailableInformation();
  return [
    `Connection / operation status: ${architectureStatusLabel(rabbitmq.status)}.`,
    monitoringResponseUpdatedLine(),
    `Queue name: ${displayMonitoringValue(rabbitmq.queue_name)}.`,
    `Messages ready: ${displayMonitoringValue(rabbitmq.messages_ready)}.`,
    `Consumer count: ${displayMonitoringValue(rabbitmq.consumer_count)}.`,
    `Unacknowledged messages: ${rabbitmq.unacknowledged_messages == null ? "Unavailable" : rabbitmq.unacknowledged_messages}.`,
    `Queue counts observed: ${displayMonitoringTimestamp(rabbitmq.counts_observed_at)}.`,
  ];
}

function consumerStatusLabel() {
  if (monitoringLoading()) return "Loading monitoring…";
  if (monitoringFailed()) return "Monitoring unavailable";
  const status = architectureMonitoringData?.consumer?.status;
  return status === "unavailable" ? "Monitoring unavailable" : architectureStatusLabel(status);
}

function consumerStatusState(value) {
  if (monitoringLoading()) return "checking";
  if (monitoringFailed()) return "monitoring-unavailable";
  return ({
    running: "available",
    stale: "stale",
    stopped: "stopped",
    unavailable: "monitoring-unavailable",
  })[value] || "monitoring-unavailable";
}

function consumerInformation() {
  if (monitoringLoading() || monitoringFailed()) return monitoringUnavailableInformation();
  const consumer = architectureMonitoringData?.consumer;
  if (!consumer) return monitoringUnavailableInformation();
  const note = staleSnapshotNote(consumer);
  const info = [];
  if (note) info.push(note);
  if (consumer.status === "unavailable") {
    const reasons = {
      not_initialized: "No consumer heartbeat is recorded; current consumer state is unknown.",
      monitoring_not_configured: "Consumer monitoring is not configured.",
      redis_unavailable: "The API could not read consumer monitoring data from Redis.",
      invalid_snapshot: "The stored consumer monitoring snapshot is invalid.",
    };
    info.push(reasons[consumer.reason] || "Consumer monitoring data is unavailable; consumer state is not confirmed.");
  }
  info.push(monitoringResponseUpdatedLine());
  info.push(`Last heartbeat: ${displayMonitoringTimestamp(consumer.last_heartbeat)}.`);
  info.push(`Processed since startup: ${displayMonitoringValue(consumer.processed_since_start)}.`);
  info.push(`Processing failures since startup: ${displayMonitoringValue(consumer.processing_failures_since_start)}.`);
  info.push(`Consumer-side Redis write status: ${architectureStatusLabel(consumer.redis_status)}.`);
  info.push(`Last successful aggregation write: ${displayMonitoringTimestamp(consumer.last_successful_aggregation_write)}.`);
  return info;
}

function windowStatusLabel() {
  if (monitoringLoading()) return "Loading monitoring…";
  if (monitoringFailed()) return "Monitoring unavailable";
  const consumer = architectureMonitoringData?.consumer;
  const status = consumer?.status;
  if (status === "running" && consumer?.windows) return "Fresh snapshot";
  if (status === "stale") return "Stale snapshot";
  if (status === "stopped") return "Final stopped snapshot";
  return "Window data unavailable";
}

function windowStatusState() {
  if (monitoringLoading()) return "checking";
  if (monitoringFailed()) return "monitoring-unavailable";
  const consumer = architectureMonitoringData?.consumer;
  if (consumer?.status === "running" && consumer.windows) return "available";
  if (consumer?.status === "stale") return "stale";
  if (consumer?.status === "stopped") return "stopped";
  if (consumer?.status === "unavailable") return "monitoring-unavailable";
  return "not_verified";
}

function windowInformation() {
  if (monitoringLoading() || monitoringFailed()) return monitoringUnavailableInformation();
  const consumer = architectureMonitoringData?.consumer;
  const windows = consumer?.windows;
  if (!windows) return monitoringUnavailableInformation();
  const tumbling = windows.tumbling || {};
  const sliding = windows.sliding || {};
  const info = [];
  const note = staleSnapshotNote(consumer);
  if (note) info.push(note);
  else info.push(`Monitoring heartbeat: ${displayMonitoringTimestamp(consumer.last_heartbeat)}.`);
  info.push(monitoringResponseUpdatedLine());
  info.push(`Tumbling window size: ${displayMonitoringValue(tumbling.size, " events")} · progress: ${displayMonitoringValue(tumbling.progress)}.`);
  info.push(`Sliding window size: ${displayMonitoringValue(sliding.size, " events")} · progress: ${displayMonitoringValue(sliding.progress)}.`);
  info.push(`Sliding interval: ${sliding.interval === "per_event" ? "Per event" : displayMonitoringValue(sliding.interval)}.`);
  info.push(`Last completed tumbling aggregation: ${displayMonitoringTimestamp(tumbling.last_completed_at)}.`);
  return info;
}

function isValidWindowConfiguration(configuration) {
  if (!configuration || typeof configuration !== "object") return false;
  const sizes = [5, 8, 10];
  return sizes.includes(configuration.tumbling_size)
    && sizes.includes(configuration.sliding_size)
    && Number.isInteger(configuration.sliding_interval)
    && configuration.sliding_interval > 0
    && configuration.sliding_interval < configuration.sliding_size;
}

function validateConfigurationResponse(data) {
  if (!data || !isValidWindowConfiguration(data.active)) {
    throw new Error("The configuration response did not include a valid active configuration.");
  }
  if (data.pending !== null && !isValidWindowConfiguration(data.pending)) {
    throw new Error("The configuration response included an invalid pending configuration.");
  }
  const expectedStatus = data.pending === null ? "active" : "pending";
  if (data.status !== expectedStatus) {
    throw new Error("The configuration response status did not match its active and pending values.");
  }
  return { active: data.active, pending: data.pending, status: expectedStatus };
}

function describeWindowConfiguration(configuration) {
  const interval = configuration.sliding_interval === 1
    ? "Per event"
    : `Every ${configuration.sliding_interval} events`;
  return `Tumbling: ${configuration.tumbling_size} events · Sliding: ${configuration.sliding_size} events · Interval: ${interval}`;
}

function setWindowConfigurationMessage(message, kind = "") {
  windowConfigurationMessage.textContent = message;
  windowConfigurationMessage.className = `configuration-message${kind ? ` ${kind}` : ""}`;
}

function updateSlidingIntervalOptions(size, preferredValue = null) {
  const previousValue = Number(slidingWindowIntervalSelect.value);
  const intervalToSelect = Number(preferredValue ?? previousValue);
  const selectedInterval = Number.isInteger(intervalToSelect) && intervalToSelect > 0 && intervalToSelect < size
    ? intervalToSelect
    : 1;
  const options = [];
  for (let interval = 1; interval < size; interval += 1) {
    const option = document.createElement("option");
    option.value = String(interval);
    option.textContent = interval === 1 ? "Every event" : `Every ${interval} events`;
    options.push(option);
  }
  slidingWindowIntervalSelect.replaceChildren(...options);
  slidingWindowIntervalSelect.value = String(selectedInterval);
}

function populateWindowConfigurationForm(configuration) {
  tumblingWindowSizeSelect.value = String(configuration.tumbling_size);
  slidingWindowSizeSelect.value = String(configuration.sliding_size);
  updateSlidingIntervalOptions(configuration.sliding_size, configuration.sliding_interval);
}

function renderWindowConfiguration() {
  const isWindowComponentSelected = selectedArchitectureComponent === "windows";
  windowConfigurationSection.hidden = !isWindowComponentSelected;

  if (windowConfigurationStatus === "loading" && !windowConfigurationData) {
    activeWindowConfiguration.textContent = "Loading configuration…";
    pendingWindowConfiguration.textContent = "Loading configuration…";
  } else if (windowConfigurationStatus === "error" || !windowConfigurationData) {
    activeWindowConfiguration.textContent = "Unavailable";
    pendingWindowConfiguration.textContent = "Unavailable";
  } else {
    activeWindowConfiguration.textContent = describeWindowConfiguration(windowConfigurationData.active);
    pendingWindowConfiguration.textContent = windowConfigurationData.pending
      ? `${describeWindowConfiguration(windowConfigurationData.pending)} · Waiting for the tumbling boundary`
      : "None · The active configuration is in use";
  }

  const canEdit = windowConfigurationStatus === "available" && Boolean(windowConfigurationData);
  tumblingWindowSizeSelect.disabled = !canEdit || windowConfigurationSubmitting;
  slidingWindowSizeSelect.disabled = !canEdit || windowConfigurationSubmitting;
  slidingWindowIntervalSelect.disabled = !canEdit || windowConfigurationSubmitting;
  applyWindowConfigurationButton.disabled = !canEdit || windowConfigurationSubmitting;
  applyWindowConfigurationButton.textContent = windowConfigurationSubmitting
    ? "Saving Configuration…"
    : "Apply Configuration";
}

async function loadWindowConfiguration({ announce = false } = {}) {
  const requestId = ++windowConfigurationRequestId;
  const previousStatus = windowConfigurationStatus;
  if (!windowConfigurationData) {
    windowConfigurationStatus = "loading";
    setWindowConfigurationMessage("Loading saved configuration…", "loading");
    renderWindowConfiguration();
  }

  try {
    const configuration = validateConfigurationResponse(await getJson("/api/config"));
    if (requestId !== windowConfigurationRequestId) return false;
    windowConfigurationData = configuration;
    windowConfigurationStatus = "available";
    if (!windowConfigurationDirty && !windowConfigurationSubmitting) {
      populateWindowConfigurationForm(configuration.pending || configuration.active);
    }
    if (announce || previousStatus !== "available") {
      setWindowConfigurationMessage("Configuration loaded from the API.");
    }
    renderWindowConfiguration();
    return true;
  } catch (error) {
    if (requestId !== windowConfigurationRequestId) return false;
    windowConfigurationData = null;
    windowConfigurationStatus = "error";
    setWindowConfigurationMessage(`Configuration unavailable: ${error.message}`, "error");
    renderWindowConfiguration();
    return false;
  }
}

async function submitWindowConfiguration(event) {
  event.preventDefault();
  if (windowConfigurationSubmitting || windowConfigurationStatus !== "available") return;

  const configuration = {
    tumbling_size: Number(tumblingWindowSizeSelect.value),
    sliding_size: Number(slidingWindowSizeSelect.value),
    sliding_interval: Number(slidingWindowIntervalSelect.value),
  };
  if (!isValidWindowConfiguration(configuration)) {
    setWindowConfigurationMessage("Choose valid window sizes and an interval smaller than the sliding size.", "error");
    return;
  }

  windowConfigurationSubmitting = true;
  setWindowConfigurationMessage("Saving as pending configuration…", "loading");
  renderWindowConfiguration();
  let saved = false;
  try {
    const response = await fetch(`${API_BASE_URL}/api/config`, {
      method: "PUT",
      headers: { Accept: "application/json", "Content-Type": "application/json" },
      cache: "no-store",
      body: JSON.stringify(configuration),
    });
    let body;
    try { body = await response.json(); } catch { body = null; }
    if (!response.ok) {
      throw new Error(typeof body?.detail === "string" ? body.detail : `Request failed (${response.status}).`);
    }
    validateConfigurationResponse(body);
    saved = true;
    windowConfigurationDirty = false;
    setWindowConfigurationMessage("Configuration saved. Refreshing active and pending status…", "loading");
    const refreshed = await loadWindowConfiguration({ announce: true });
    if (!refreshed) {
      setWindowConfigurationMessage("Configuration was saved, but its latest active/pending status could not be refreshed.", "error");
    } else if (windowConfigurationData.pending) {
      setWindowConfigurationMessage("Configuration saved as pending. It will activate after the current tumbling window completes.", "success");
    } else {
      setWindowConfigurationMessage("Configuration status refreshed from the API.", "success");
    }
  } catch (error) {
    setWindowConfigurationMessage(
      saved
        ? `Configuration was saved, but its status could not be confirmed: ${error.message}`
        : `Could not save configuration: ${error.message}`,
      "error",
    );
  } finally {
    windowConfigurationSubmitting = false;
    if (windowConfigurationData && !windowConfigurationDirty) {
      populateWindowConfigurationForm(windowConfigurationData.pending || windowConfigurationData.active);
    }
    renderWindowConfiguration();
  }
}

function redisStatusLabel() {
  if (monitoringLoading()) return "Loading monitoring…";
  if (monitoringFailed()) return "Monitoring unavailable";
  const apiStatus = architectureMonitoringData?.api?.redis_status;
  const consumerStatus = architectureMonitoringData?.consumer?.redis_status;
  const consumer = architectureMonitoringData?.consumer;
  const consumerQualifier = consumer?.status === "stale"
    ? " (stale snapshot)"
    : consumer?.status === "stopped" ? " (final snapshot)" : "";
  const currentConsumerStatus = consumer?.status === "running" && consumerStatus
    ? consumerStatus
    : "not_verified";
  return `API: ${architectureStatusLabel(apiStatus)} · Consumer${consumerQualifier}: ${architectureStatusLabel(currentConsumerStatus)}`;
}

function redisStatusState() {
  if (monitoringLoading()) return "checking";
  if (monitoringFailed()) return "monitoring-unavailable";
  const apiStatus = architectureMonitoringData?.api?.redis_status;
  const consumerStatus = architectureMonitoringData?.consumer?.redis_status;
  const consumerState = architectureMonitoringData?.consumer?.status;
  if (apiStatus === "unavailable") return "unavailable";
  if (consumerState === "running" && consumerStatus === "unavailable") return "unavailable";
  if (apiStatus === "available" && consumerState === "running" && consumerStatus === "available") return "available";
  return "not_verified";
}

function redisInformation() {
  if (monitoringLoading() || monitoringFailed()) return monitoringUnavailableInformation();
  const data = architectureMonitoringData;
  if (!data) return monitoringUnavailableInformation();
  const consumer = data.consumer || {};
  const info = [
    `API-observed Redis status: ${architectureStatusLabel(data.api?.redis_status)}.`,
    monitoringResponseUpdatedLine(),
  ];
  const note = staleSnapshotNote(consumer);
  if (note) info.push(`Consumer-side status is from that snapshot. ${note}`);
  else info.push(`Consumer monitoring heartbeat: ${displayMonitoringTimestamp(consumer.last_heartbeat)}.`);
  info.push(`Consumer-side aggregation write status: ${architectureStatusLabel(consumer.redis_status)}.`);
  info.push(`Last successful consumer-side aggregation write: ${displayMonitoringTimestamp(consumer.last_successful_aggregation_write)}.`);
  return info;
}

function renderArchitectureDetail() {
  const component = architectureComponents[selectedArchitectureComponent];
  if (!component) return;
  const state = typeof component.state === "function" ? component.state() : "";
  architectureDetailTitle.textContent = component.name;
  architectureDetailStatus.textContent = component.status();
  architectureDetailStatus.dataset.state = state;
  architectureDetailDescription.textContent = component.description;
  const information = typeof component.information === "function" ? component.information() : component.information;
  architectureDetailInformation.replaceChildren();
  information.forEach((item) => {
    const entry = document.createElement("li");
    entry.textContent = item;
    architectureDetailInformation.append(entry);
  });
  renderWindowConfiguration();
}

function renderArchitectureStatuses() {
  const rabbitmqStatus = monitoringLoading()
    ? "Loading monitoring…"
    : monitoringFailed() ? "Monitoring unavailable" : architectureStatusLabel(architectureMonitoringData?.rabbitmq?.status);
  const consumerStatus = consumerStatusLabel();
  const windows = architectureMonitoringData?.consumer?.windows;
  const windowsStatus = monitoringLoading()
    ? "Loading monitoring…"
    : monitoringFailed() ? "Monitoring unavailable"
      : architectureMonitoringData?.consumer?.status === "running" && windows
        ? `Tumbling ${displayMonitoringValue(windows.tumbling?.progress)}/${displayMonitoringValue(windows.tumbling?.size)} · Sliding ${displayMonitoringValue(windows.sliding?.progress)}/${displayMonitoringValue(windows.sliding?.size)}`
        : windowStatusLabel();
  const redisStatus = redisStatusLabel();
  const labels = {
    generation: generationStatusLabel(),
    rabbitmq: rabbitmqStatus,
    consumer: consumerStatus,
    windows: windowsStatus,
    redis: redisStatus,
    api: architectureState.api === "connected" ? "Available" : architectureState.api === "disconnected" ? "Unavailable" : "Checking",
    dashboard: architectureState.dashboard === "connected" ? "Connected" : architectureState.dashboard === "disconnected" ? "Disconnected" : "Checking",
  };
  const states = {
    generation: generationStatusState(),
    rabbitmq: monitoringLoading() ? "checking" : monitoringFailed() ? "monitoring-unavailable" : architectureMonitoringData?.rabbitmq?.status || "not_verified",
    consumer: consumerStatusState(architectureMonitoringData?.consumer?.status),
    windows: windowStatusState(),
    redis: redisStatusState(),
    api: architectureState.api === "connected" ? "available" : architectureState.api,
    dashboard: architectureState.dashboard,
  };
  Object.entries(labels).forEach(([name, label]) => {
    const status = document.querySelector(`[data-node-status="${name}"]`);
    if (status) {
      status.textContent = label;
      status.dataset.nodeState = states[name];
    }
  });
  renderArchitectureDetail();
}

function generationStatusState() {
  if (architectureState.api === "disconnected" || ["unavailable", "not_configured"].includes(architectureState.rabbitmq)) {
    return "unavailable";
  }
  if (architectureState.api !== "connected") return "checking";
  if (architectureState.rabbitmq === "available") return "available";
  return architectureState.rabbitmq === "checking" ? "checking" : "not_verified";
}

function generationStatusLabel() {
  return ({
    available: "Manual + batch available",
    unavailable: "Event publishing unavailable",
    checking: "Checking",
    not_verified: "Event path not verified",
  })[generationStatusState()];
}

architectureNodes.forEach((node) => node.addEventListener("click", () => {
  selectedArchitectureComponent = node.dataset.component;
  architectureNodes.forEach((item) => item.setAttribute("aria-pressed", String(item === node)));
  renderArchitectureDetail();
}));
function markWindowConfigurationDirty() {
  windowConfigurationDirty = true;
  setWindowConfigurationMessage("Unsaved selections. Apply them to save as pending configuration.");
  renderWindowConfiguration();
}

tumblingWindowSizeSelect.addEventListener("change", markWindowConfigurationDirty);
slidingWindowSizeSelect.addEventListener("change", () => {
  updateSlidingIntervalOptions(Number(slidingWindowSizeSelect.value));
  markWindowConfigurationDirty();
});
slidingWindowIntervalSelect.addEventListener("change", markWindowConfigurationDirty);
windowConfigurationForm.addEventListener("submit", submitWindowConfiguration);
updateSlidingIntervalOptions(Number(slidingWindowSizeSelect.value), 1);
renderArchitectureStatuses();

function formatNumber(value) {
  if (value === null || value === undefined || !Number.isFinite(Number(value))) return "-";
  return new Intl.NumberFormat(undefined, { maximumSignificantDigits: 6 }).format(Number(value));
}

function setApiStatus(available, label) {
  apiStatus.className = `status-pill ${available ? "status-online" : "status-offline"}`;
  apiStatus.querySelector("span:last-child").textContent = label;
}

function setServiceStatus(element, name, value) {
  const states = {
    available: { label: "Connected", className: "service-connected", color: "#22C55E", tint: "#22C55E12" },
    unavailable: { label: "Disconnected", className: "service-unavailable", color: "#EF4444", tint: "#EF444412" },
    not_configured: { label: "Not configured", className: "service-unavailable", color: "#EF4444", tint: "#EF444412" },
    not_verified: { label: "Not verified", className: "service-unverified", color: "#F59E0B", tint: "#F59E0B12" },
    connecting: { label: "Connecting", className: "service-unverified", color: "#F59E0B", tint: "#F59E0B12" },
  };
  const state = states[value] || states.not_verified;
  element.innerHTML = `${name} <b></b>`;
  element.querySelector("b").textContent = state.label;
  element.classList.remove("service-connected", "service-unavailable", "service-unverified");
  element.classList.add(state.className);
  element.style.setProperty("--service-status-color", state.color);
  element.style.setProperty("--service-status-tint", state.tint);
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

async function loadMonitoring() {
  try {
    const data = await getJson("/api/monitoring");
    if (!data || !data.api || !data.rabbitmq || !data.consumer) {
      throw new Error("The monitoring response did not contain component status data.");
    }
    architectureMonitoringData = data;
    architectureMonitoringStatus = "available";
    architectureMonitoringUpdatedAt = new Date();
    const online = data.api.status === "available";
    architectureState.api = online ? "connected" : "disconnected";
    // A successful monitoring response confirms this dashboard can reach the API.
    architectureState.dashboard = "connected";
    architectureState.rabbitmq = ["available", "unavailable", "not_configured", "not_verified"].includes(data.rabbitmq.status)
      ? data.rabbitmq.status
      : "not_verified";
    architectureState.redis = ["available", "unavailable", "not_configured", "not_verified"].includes(data.api.redis_status)
      ? data.api.redis_status
      : "not_verified";
    setApiStatus(online, online ? "API online" : "API unavailable");
    setServiceStatus(rabbitStatus, "RabbitMQ", architectureState.rabbitmq);
    setServiceStatus(redisStatus, "Redis", architectureState.redis);
    renderArchitectureStatuses();
    return online;
  } catch {
    architectureMonitoringData = null;
    architectureMonitoringStatus = "error";
    architectureMonitoringUpdatedAt = null;
    architectureState.api = "disconnected";
    architectureState.dashboard = "disconnected";
    architectureState.rabbitmq = "not_verified";
    architectureState.redis = "not_verified";
    setApiStatus(false, "API unavailable");
    // Monitoring failures leave RabbitMQ and Redis status unconfirmed.
    setServiceStatus(rabbitStatus, "RabbitMQ", "not_verified");
    setServiceStatus(redisStatus, "Redis", "not_verified");
    renderArchitectureStatuses();
    return false;
  }
}

function showMetricsUnavailable(message) {
  metricsDataStatus = "unavailable";
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
    metricsDataStatus = "available";
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
  const canvas = document.getElementById(canvasId);
  const chartWrap = canvas?.parentElement;
  if (!hasData) {
    messageElement.textContent = metricsDataStatus === "unavailable"
      ? "Chart data unavailable because the latest metrics request failed."
      : metricsDataStatus === "loading"
        ? "Waiting for metrics data."
        : "No metric values have been stored for this chart yet.";
    messageElement.className = metricsDataStatus === "unavailable" ? "chart-message error" : "chart-message empty";
    if (chartWrap) chartWrap.hidden = true;
    if (existingChart) existingChart.destroy();
    return null;
  }
  messageElement.textContent = "Showing values returned by the metrics API. Missing values are left blank.";
  messageElement.className = "chart-message";
  if (chartWrap) chartWrap.hidden = false;
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
        title: { display: true, text: "Window", color: "#586477", font: { family: CHART_FONT_FAMILY, size: 13, weight: "600" } },
        ticks: { color: "#465166", maxRotation: 0, font: { family: CHART_FONT_FAMILY, size: 13 } },
      },
      y: {
        beginAtZero: true,
        grid: { color: "#edf0f5" },
        title: { display: true, text: axisTitle, color: "#586477", font: { family: CHART_FONT_FAMILY, size: 13, weight: "600" } },
        ticks: { color: "#465166", maxTicksLimit: 5, font: { family: CHART_FONT_FAMILY, size: 12 }, callback: formatter },
      },
    },
  };
  if (!existingChart) return new Chart(canvas, { type: "bar", data, options });
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
  if (normalizedRoute() !== "dashboard") {
    window.clearTimeout(refreshTimer);
    return;
  }
  if (activeRefresh) return activeRefresh;
  activeRefresh = Promise.allSettled([
    loadMonitoring(),
    loadMetrics(),
    loadEvents(),
    loadWindowConfiguration(),
  ]);
  try {
    await activeRefresh;
  } finally {
    activeRefresh = null;
  }
}

function scheduleRefresh() {
  window.clearTimeout(refreshTimer);
  if (document.hidden || normalizedRoute() !== "dashboard") return;
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

function showSampleBatchMessage(message, kind) {
  sampleBatchMessage.textContent = message;
  sampleBatchMessage.className = `message-box ${kind}`;
  sampleBatchMessage.hidden = false;
}

generateSampleEventsButton.addEventListener("click", async () => {
  if (generateSampleEventsButton.disabled) return;
  const requested = Number(sampleBatchSize.value);
  if (![5, 8, 10].includes(requested)) {
    showSampleBatchMessage("Choose a batch size of 5, 8, or 10 events.", "error");
    return;
  }

  generateSampleEventsButton.disabled = true;
  generateSampleEventsButton.textContent = "Generating…";
  showSampleBatchMessage(`Publishing a finite batch of ${requested} events…`, "loading");
  try {
    const response = await fetch(`${API_BASE_URL}/api/events/batch`, {
      method: "POST",
      headers: { Accept: "application/json", "Content-Type": "application/json" },
      cache: "no-store",
      body: JSON.stringify({ batch_size: requested }),
    });
    let result;
    try { result = await response.json(); } catch { result = null; }
    if (!response.ok) {
      throw new Error(typeof result?.detail === "string" ? result.detail : `Batch request failed (${response.status}).`);
    }
    if (!result
        || result.requested !== requested
        || !Number.isInteger(result.published)
        || !Number.isInteger(result.failed)
        || !Array.isArray(result.failures)
        || result.published + result.failed !== result.requested) {
      throw new Error("The API returned an invalid batch result.");
    }

    const failedEvents = result.failures.map((failure) => `#${failure.event_number}`).join(", ");
    const summary = `Published ${result.published} of ${result.requested} sample events; ${result.failed} failed.`;
    showSampleBatchMessage(
      failedEvents ? `${summary} Failed event numbers: ${failedEvents}.` : summary,
      result.failed === 0 ? "success" : result.published === 0 ? "error" : "partial",
    );
    await refreshLiveData();
  } catch (error) {
    showSampleBatchMessage(`Could not complete sample batch: ${error.message}`, "error");
  } finally {
    generateSampleEventsButton.disabled = false;
    generateSampleEventsButton.textContent = "Generate Sample Events";
  }
});

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
  else if (normalizedRoute() === "dashboard") {
    void refreshLiveData();
    scheduleRefresh();
  }
});
window.addEventListener("pagehide", () => window.clearTimeout(refreshTimer));

function normalizedRoute(pathname = window.location.pathname) {
  const path = pathname.replace(/\/+$/, "") || "/";
  if (path === "/failure-recovery") return "failureRecovery";
  if (path === "/experiments") return "experiments";
  if (path === "/experiments/new") return "create";
  if (/^\/experiments\/[^/]+\/runs\/[^/]+\/results$/.test(path)) return "results";
  if (/^\/experiments\/[^/]+\/runs\/[^/]+$/.test(path)) return "run";
  if (/^\/experiments\/[^/]+$/.test(path)) return "details";
  return "dashboard";
}

let experimentListRequest = 0;
let experimentDetailRequest = 0;
let currentExperiment = null;
let runRefreshInFlight = false;
let runRefreshQueued = false;
let runActionInFlight = false;
let runPageSnapshot = null;
let resultsRefreshInFlight = false;
let resultsRefreshQueued = false;
let resultsRefreshRouteKey = null;
let resultsPageSnapshot = null;
let comparisonRequestId = 0;
let comparisonEligibleRuns = [];
let selectedComparisonRunIds = new Set();

function experimentElement(tag, className, text) {
  const element = document.createElement(tag);
  if (className) element.className = className;
  if (text !== undefined && text !== null && text !== "") element.textContent = String(text);
  return element;
}

function formatExperimentDate(value) {
  if (!value) return "Unavailable";
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? "Unavailable" : date.toLocaleString();
}

function experimentSummary(experiment) {
  const config = experiment.default_configuration || {};
  const workload = experiment.default_workload_configuration || {};
  const windowSummary = Number.isInteger(config.tumbling_size) && Number.isInteger(config.sliding_size)
    && Number.isInteger(config.sliding_interval)
    ? `Tumbling ${config.tumbling_size} · Sliding ${config.sliding_size} / ${config.sliding_interval}`
    : "Unavailable";
  const workloadSummary = Number.isInteger(workload.target_event_count)
    && Number.isInteger(workload.publish_interval_ms)
    ? `${workload.target_event_count.toLocaleString()} events · ${workload.publish_interval_ms} ms`
    : "Unavailable";
  return { windowSummary, workloadSummary };
}

function setExperimentListState(state) {
  document.getElementById("experiments-loading").hidden = state !== "loading";
  document.getElementById("experiments-error").hidden = state !== "error";
  document.getElementById("experiments-empty").hidden = state !== "empty";
  document.getElementById("experiments-results").hidden = state !== "results";
  document.getElementById("experiments-header-create").hidden = state !== "results";
}

function renderExperiments(experiments) {
  const tbody = document.getElementById("experiments-table-body");
  const cards = document.getElementById("experiments-cards");
  tbody.replaceChildren();
  cards.replaceChildren();
  const types = new Set();

  for (const experiment of experiments) {
    if (experiment.experiment_type) types.add(experiment.experiment_type);
    const { windowSummary, workloadSummary } = experimentSummary(experiment);
    const name = experimentElement("span", "experiment-row-name", experiment.name || "Unnamed experiment");
    const description = experimentElement("span", "experiment-row-description", experiment.description || "No description");
    const nameCell = experimentElement("td");
    nameCell.append(name, description);
    const typeCell = experimentElement("td");
    typeCell.append(experimentElement("span", "experiment-type-tag", String(experiment.experiment_type || "Unavailable").replaceAll("_", " ")));
    const row = document.createElement("tr");
    row.append(nameCell, typeCell,
      experimentElement("td", "", formatExperimentDate(experiment.updated_at)),
      experimentElement("td", "", windowSummary),
      experimentElement("td", "", workloadSummary));
    const actionCell = document.createElement("td");
    const detailLink = experimentElement("a", "secondary-action-button compact-action", "View Details");
    detailLink.href = `/experiments/${encodeURIComponent(experiment.experiment_id)}`;
    detailLink.dataset.routeLink = "";
    actionCell.append(detailLink);
    row.append(actionCell);
    tbody.append(row);

    const card = experimentElement("article", "panel experiment-mobile-card");
    const cardHead = experimentElement("div", "experiment-mobile-card-heading");
    cardHead.append(experimentElement("div", "", experiment.name || "Unnamed experiment"),
      experimentElement("span", "experiment-type-tag", String(experiment.experiment_type || "Unavailable").replaceAll("_", " ")));
    card.append(cardHead, experimentElement("p", "experiment-row-description", experiment.description || "No description"));
    const details = experimentElement("dl", "experiment-mobile-details");
    for (const [label, value] of [["Last updated", formatExperimentDate(experiment.updated_at)], ["Window defaults", windowSummary], ["Workload defaults", workloadSummary]]) {
      details.append(experimentElement("dt", "", label), experimentElement("dd", "", value));
    }
    card.append(details);
    const mobileDetailLink = experimentElement("a", "secondary-action-button compact-action", "View Details");
    mobileDetailLink.href = `/experiments/${encodeURIComponent(experiment.experiment_id)}`;
    mobileDetailLink.dataset.routeLink = "";
    card.append(mobileDetailLink);
    cards.append(card);
  }

  document.getElementById("experiments-total-count").textContent = experiments.length.toLocaleString();
  document.getElementById("experiments-type-count").textContent = String(types.size);
  setExperimentListState(experiments.length ? "results" : "empty");
}

async function loadExperiments() {
  const requestId = ++experimentListRequest;
  setExperimentListState("loading");
  try {
    const response = await fetch("/api/experiments", { headers: { Accept: "application/json" } });
    let body;
    try { body = await response.json(); } catch { body = null; }
    if (!response.ok) throw new Error(typeof body?.detail === "string" ? body.detail : `Could not load experiments (${response.status}).`);
    if (!Array.isArray(body?.experiments)) throw new Error("The API returned an invalid experiment list.");
    if (requestId === experimentListRequest && normalizedRoute() === "experiments") renderExperiments(body.experiments);
  } catch (error) {
    if (requestId !== experimentListRequest || normalizedRoute() !== "experiments") return;
    document.getElementById("experiments-error-detail").textContent = error.message || "Could not load experiments.";
    setExperimentListState("error");
  }
}

function setExperimentDetailState(state) {
  document.getElementById("experiment-detail-loading").hidden = state !== "loading";
  document.getElementById("experiment-detail-error").hidden = state !== "error";
  document.getElementById("experiment-detail-missing").hidden = state !== "missing";
  document.getElementById("experiment-detail-content").hidden = state !== "ready";
}

function displayValue(value, suffix = "") {
  return Number.isInteger(value) ? `${value.toLocaleString()}${suffix}` : "Unavailable";
}

function renderExperimentDetail(experiment) {
  currentExperiment = experiment;
  document.getElementById("experiment-detail-title").textContent = experiment.name || "Unnamed experiment";
  document.getElementById("experiment-detail-description").textContent = experiment.description || "Saved experiment definition and default run configuration.";
  document.getElementById("experiment-detail-description-value").textContent = experiment.description || "No description";
  document.getElementById("experiment-detail-type").textContent = String(experiment.experiment_type || "Unavailable").replaceAll("_", " ");
  document.getElementById("experiment-detail-id").textContent = experiment.experiment_id || "Unavailable";
  document.getElementById("experiment-detail-created").textContent = formatExperimentDate(experiment.created_at);

  const config = experiment.default_configuration || {};
  const workload = experiment.default_workload_configuration || {};
  document.getElementById("detail-tumbling-size").textContent = displayValue(config.tumbling_size, " events");
  document.getElementById("detail-sliding-size").textContent = displayValue(config.sliding_size, " events");
  document.getElementById("detail-sliding-interval").textContent = displayValue(config.sliding_interval, " events");
  document.getElementById("detail-target-count").textContent = displayValue(workload.target_event_count, " events");
  document.getElementById("detail-publish-interval").textContent = displayValue(workload.publish_interval_ms, " ms");
  const distribution = workload.event_type_distribution;
  const distributionText = distribution && typeof distribution === "object" && Object.keys(distribution).length
    ? Object.entries(distribution).map(([type, weight]) => `${type.replaceAll("_", " ")} (weight ${weight})`).join(", ")
    : "Unavailable";
  document.getElementById("detail-event-distribution").textContent = distributionText;
  setExperimentDetailState("ready");
}

async function loadExperimentDetail() {
  const requestId = ++experimentDetailRequest;
  const pathId = window.location.pathname.replace(/\/+$/, "").split("/").at(-1);
  setExperimentDetailState("loading");
  try {
    const response = await fetch(`/api/experiments/${encodeURIComponent(pathId)}`, { headers: { Accept: "application/json" } });
    let body;
    try { body = await response.json(); } catch { body = null; }
    if (response.status === 404) {
      if (requestId === experimentDetailRequest && normalizedRoute() === "details") setExperimentDetailState("missing");
      return;
    }
    if (!response.ok) throw new Error(typeof body?.detail === "string" ? body.detail : `Could not load experiment (${response.status}).`);
    if (!body?.experiment_id) throw new Error("The API returned an invalid experiment definition.");
    if (requestId === experimentDetailRequest && normalizedRoute() === "details") renderExperimentDetail(body);
  } catch (error) {
    if (requestId !== experimentDetailRequest || normalizedRoute() !== "details") return;
    document.getElementById("experiment-detail-error-message").textContent = error.message || "Could not load experiment.";
    setExperimentDetailState("error");
  }
}

function showExperimentDetailMessage(message, kind = "success") {
  const box = document.getElementById("experiment-detail-message");
  box.textContent = message;
  box.className = `form-submit-message${kind === "success" ? " success" : ""}`;
  box.hidden = false;
}

function syncEditSlidingOptions() {
  const size = Number(document.getElementById("edit-sliding-size").value);
  const select = document.getElementById("edit-sliding-interval");
  const previous = Number(select.value) || 1;
  select.replaceChildren();
  for (let interval = 1; interval < size; interval += 1) {
    const option = experimentElement("option", "", `${interval} ${interval === 1 ? "event" : "events"}`);
    option.value = String(interval);
    select.append(option);
  }
  select.value = String(Math.min(previous, size - 1));
}

function populateExperimentEditForm(experiment) {
  const config = experiment.default_configuration || {};
  const workload = experiment.default_workload_configuration || {};
  document.getElementById("edit-experiment-name").value = experiment.name || "";
  document.getElementById("edit-experiment-description").value = experiment.description || "";
  document.getElementById("edit-experiment-type").value = experiment.experiment_type || "window_performance";
  document.getElementById("edit-tumbling-size").value = String(config.tumbling_size ?? "");
  document.getElementById("edit-sliding-size").value = String(config.sliding_size ?? "");
  document.getElementById("edit-target-count").value = String(workload.target_event_count ?? "");
  document.getElementById("edit-publish-interval").value = String(workload.publish_interval_ms ?? "");
  for (const checkbox of document.querySelectorAll('input[name="edit-event-type"]')) {
    const weight = workload.event_type_distribution?.[checkbox.value];
    checkbox.checked = Number.isInteger(weight);
    document.querySelector(`[data-edit-event-weight="${checkbox.value}"]`).value = Number.isInteger(weight) ? String(weight) : "1";
  }
  syncEditSlidingOptions();
  document.getElementById("edit-sliding-interval").value = String(config.sliding_interval ?? "");
  document.getElementById("experiment-edit-message").hidden = true;
}

document.getElementById("experiment-detail-retry").addEventListener("click", () => { void loadExperimentDetail(); });
document.getElementById("experiment-edit-toggle").addEventListener("click", () => {
  if (!currentExperiment) return;
  const panel = document.getElementById("experiment-edit-panel");
  panel.hidden = !panel.hidden;
  if (!panel.hidden) {
    populateExperimentEditForm(currentExperiment);
    panel.scrollIntoView({ block: "start", behavior: "smooth" });
  }
});
document.getElementById("experiment-edit-cancel").addEventListener("click", () => {
  document.getElementById("experiment-edit-panel").hidden = true;
});
document.getElementById("edit-sliding-size").addEventListener("change", syncEditSlidingOptions);

document.getElementById("experiment-edit-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  if (!currentExperiment) return;
  const message = document.getElementById("experiment-edit-message");
  const name = document.getElementById("edit-experiment-name").value.trim();
  const description = document.getElementById("edit-experiment-description").value.trim();
  const tumblingSize = Number(document.getElementById("edit-tumbling-size").value);
  const slidingSize = Number(document.getElementById("edit-sliding-size").value);
  const slidingInterval = Number(document.getElementById("edit-sliding-interval").value);
  const targetCount = Number(document.getElementById("edit-target-count").value);
  const publishInterval = Number(document.getElementById("edit-publish-interval").value);
  const selected = [...document.querySelectorAll('input[name="edit-event-type"]:checked')];
  const validWindow = [5, 8, 10].includes(tumblingSize) && [5, 8, 10].includes(slidingSize) && Number.isInteger(slidingInterval) && slidingInterval > 0 && slidingInterval < slidingSize;
  const validWorkload = Number.isInteger(targetCount) && targetCount >= 1 && targetCount <= 10000
    && Number.isInteger(publishInterval) && publishInterval >= 10 && publishInterval <= 60000 && selected.length > 0;
  const distribution = {};
  for (const checkbox of selected) {
    const weight = Number(document.querySelector(`[data-edit-event-weight="${checkbox.value}"]`).value);
    if (!Number.isInteger(weight) || weight < 1 || weight > 1000) distribution[checkbox.value] = null;
    else distribution[checkbox.value] = weight;
  }
  const invalidWeights = Object.values(distribution).some((weight) => weight === null);
  if (!name || name.length > 160 || description.length > 2000 || !validWindow || !validWorkload || invalidWeights) {
    message.textContent = "Check the name and configuration values. Select an event type and use supported whole-number ranges.";
    message.hidden = false;
    return;
  }
  message.hidden = true;
  const saveButton = document.getElementById("experiment-edit-save");
  saveButton.disabled = true;
  saveButton.textContent = "Saving…";
  try {
    const response = await fetch(`/api/experiments/${encodeURIComponent(currentExperiment.experiment_id)}`, {
      method: "PATCH", headers: { "Content-Type": "application/json", Accept: "application/json" },
      body: JSON.stringify({
        name, description: description || null,
        experiment_type: document.getElementById("edit-experiment-type").value,
        default_configuration: { tumbling_size: tumblingSize, sliding_size: slidingSize, sliding_interval: slidingInterval },
        default_workload_configuration: { target_event_count: targetCount, publish_interval_ms: publishInterval, event_type_distribution: distribution },
      }),
    });
    let body;
    try { body = await response.json(); } catch { body = null; }
    if (!response.ok) throw new Error(validationMessage(body));
    renderExperimentDetail(body);
    document.getElementById("experiment-edit-panel").hidden = true;
    showExperimentDetailMessage("Experiment definition updated.");
  } catch (error) {
    message.textContent = error.message || "Could not save changes. Your entries are still here.";
    message.hidden = false;
  } finally {
    saveButton.disabled = false;
    saveButton.textContent = "Save Changes";
  }
});

document.getElementById("experiment-create-run").addEventListener("click", async () => {
  if (!currentExperiment) return;
  const button = document.getElementById("experiment-create-run");
  button.disabled = true;
  button.textContent = "Creating run…";
  try {
    const response = await fetch(`/api/experiments/${encodeURIComponent(currentExperiment.experiment_id)}/runs`, {
      method: "POST", headers: { "Content-Type": "application/json", Accept: "application/json" }, body: "{}",
    });
    let body;
    try { body = await response.json(); } catch { body = null; }
    if (!response.ok) throw new Error(validationMessage(body));
    if (!body?.run_id) throw new Error("The API did not confirm the new run record.");
    const nextPath = `/experiments/${encodeURIComponent(currentExperiment.experiment_id)}/runs/${encodeURIComponent(body.run_id)}`;
    window.history.pushState({}, "", nextPath);
    renderRoute();
    window.scrollTo({ top: 0, behavior: "auto" });
  } catch (error) {
    showExperimentDetailMessage(error.message || "Could not create the run record.", "error");
  } finally {
    button.disabled = false;
    button.textContent = "Create New Run";
  }
});

function runRouteIds() {
  const segments = window.location.pathname.replace(/\/+$/, "").split("/");
  return { experimentId: segments[2], runId: segments[4] };
}

async function requestJson(url, options) {
  const response = await fetch(url, options);
  let body;
  try { body = await response.json(); } catch { body = null; }
  return { response, body };
}

function runUnavailable(value, suffix = "") {
  return Number.isFinite(value) ? `${value.toLocaleString()}${suffix}` : "Unavailable";
}

function runMetric(value, suffix = "") {
  return typeof value === "number" && Number.isFinite(value) ? `${value.toFixed(2)}${suffix}` : "Unavailable";
}

function runEndpointPath(experimentId, runId, suffix = "") {
  return `/api/experiments/${encodeURIComponent(experimentId)}/runs/${encodeURIComponent(runId)}${suffix}`;
}

function setRunError(message, missing = false) {
  const hasPriorData = Boolean(runPageSnapshot);
  document.getElementById("run-page-loading").hidden = true;
  document.getElementById("run-page-missing").hidden = !missing;
  const errorPanel = document.getElementById("run-page-error");
  errorPanel.hidden = missing || (hasPriorData && !message);
  document.getElementById("run-page-error-message").textContent = message || "Could not load this run.";
  document.getElementById("run-page-content").hidden = missing || !hasPriorData;
}

function setRunText(id, value) {
  document.getElementById(id).textContent = value === null || value === undefined || value === "" ? "Unavailable" : String(value);
}

function renderRunTimeline(run) {
  const entries = [];
  if (run.created_at) entries.push({ at: run.created_at, label: "Run created" });
  if (run.started_at) entries.push({ at: run.started_at, label: "Run started" });
  if (run.stop_intent?.requested_at) {
    entries.push({ at: run.stop_intent.requested_at, label: `Stop requested · ${run.stop_intent.target}`,
      detail: run.stop_intent.reason || "" });
  }
  if (run.execution?.interrupted_at) {
    entries.push({ at: run.execution.interrupted_at, label: "Execution interrupted",
      detail: run.execution.interruption_reason || "" });
  }
  const terminal = ["COMPLETED", "FAILED", "CANCELLED"].includes(run.status);
  if (terminal && run.ended_at) entries.push({ at: run.ended_at, label: `Run ${run.status.toLowerCase()}`,
    detail: run.status === "FAILED" ? (run.failure_reason || "") : "" });
  entries.sort((left, right) => new Date(left.at).getTime() - new Date(right.at).getTime());
  const timeline = document.getElementById("run-timeline");
  timeline.replaceChildren();
  for (const entry of entries) {
    const item = document.createElement("li");
    const heading = experimentElement("strong", "", entry.label);
    const date = experimentElement("time", "", formatExperimentDate(entry.at));
    date.dateTime = entry.at;
    item.append(heading, date);
    if (entry.detail) item.append(experimentElement("p", "", entry.detail));
    timeline.append(item);
  }
  document.getElementById("run-timeline-empty").hidden = entries.length > 0;
}

function renderRunControls(run) {
  const status = String(run.status || "Unavailable");
  const terminal = ["COMPLETED", "FAILED", "CANCELLED"].includes(status);
  const draining = !terminal && Boolean(run.stop_intent);
  const badge = document.getElementById("run-status-badge");
  badge.textContent = draining ? `DRAINING · ${run.stop_intent.target} requested` : status;
  badge.className = `run-status-badge status-${status.toLowerCase()}${draining ? " status-draining" : ""}`;

  const note = document.getElementById("run-lifecycle-note");
  note.hidden = true;
  if (draining) {
    note.textContent = `Stop requested for ${run.stop_intent.target}. The run is draining; its terminal status will appear after finalization.`;
    note.hidden = false;
  } else if (run.execution?.interruption_recorded) {
    note.textContent = `Execution was interrupted${run.execution.interruption_reason ? `: ${run.execution.interruption_reason}` : "."} No resume action is available here.`;
    note.hidden = false;
  } else if (run.event_ingestion_closed && !terminal) {
    note.textContent = "Event ingestion is closed. Awaiting the persisted lifecycle outcome.";
    note.hidden = false;
  } else if (status === "FAILED" && run.failure_reason) {
    note.textContent = run.failure_reason;
    note.hidden = false;
  }

  const buttons = document.getElementById("run-control-buttons");
  buttons.replaceChildren();
  const resultsLink = experimentElement("a", "secondary-action-button", "View Results");
  resultsLink.href = `/experiments/${encodeURIComponent(run.experiment_id)}/runs/${encodeURIComponent(run.run_id)}/results`;
  resultsLink.dataset.routeLink = "";
  buttons.append(resultsLink);
  if (status === "CREATED") {
    const start = experimentElement("button", "primary-action-button", "Start Run");
    start.type = "button";
    start.disabled = runActionInFlight;
    start.addEventListener("click", startExperimentRun);
    buttons.append(start);
  } else if (status === "RUNNING" && !draining) {
    const stop = experimentElement("button", "secondary-action-button", "Stop Run");
    stop.type = "button";
    stop.disabled = runActionInFlight;
    stop.addEventListener("click", stopExperimentRun);
    buttons.append(stop);
  }
};

function renderRunMeasurements(snapshot) {
  const run = snapshot.run;
  const finalResults = snapshot.results?.results || null;
  const summary = finalResults
    ? (finalResults.measurements_available ? finalResults.measurements : null)
    : snapshot.measurements;
  const hasMeasurements = Boolean(summary?.measurements_present);
  const average = hasMeasurements ? summary.average_latency_ms?.consumer_processing_latency_ms : null;
  const p95 = hasMeasurements ? summary.p95_latency_ms?.consumer_processing_latency_ms : null;
  const sampleCount = hasMeasurements ? summary.sample_count?.consumer_processing_latency_ms : null;
  const throughput = hasMeasurements ? summary.throughput_events_per_second : null;
  setRunText("run-throughput-value", runMetric(throughput, " events/s"));
  setRunText("run-average-latency", runMetric(average, " ms"));
  setRunText("run-p95-latency", runMetric(p95, " ms"));
  setRunText("run-final-throughput", runMetric(finalResults?.benchmark_throughput_events_per_second, " events/s"));

  const resultComplete = document.getElementById("run-result-completeness");
  const latencyComplete = document.getElementById("run-latency-completeness");
  if (finalResults) {
    resultComplete.textContent = `Result completeness: ${finalResults.result_complete === true ? "Complete" : finalResults.result_complete === false ? "Incomplete" : "Unavailable"}`;
    latencyComplete.textContent = `Latency completeness: ${finalResults.latency_complete === true ? "Complete" : finalResults.latency_complete === false ? "Incomplete" : "Unavailable"}`;
    resultComplete.classList.toggle("incomplete", finalResults.result_complete === false);
    latencyComplete.classList.toggle("incomplete", finalResults.latency_complete === false);
  } else {
    const terminal = ["COMPLETED", "FAILED", "CANCELLED"].includes(run.status);
    resultComplete.textContent = `Result completeness: ${terminal ? "Unavailable" : "Not finalized"}`;
    latencyComplete.textContent = `Latency completeness: ${terminal ? "Unavailable" : "Not finalized"}`;
    resultComplete.classList.remove("incomplete");
    latencyComplete.classList.remove("incomplete");
  }
  let note = hasMeasurements
    ? `Run-scoped measurement samples: ${runUnavailable(sampleCount)}. Latency completeness is reported separately from result completeness.`
    : finalResults && !finalResults.measurements_available
      ? "Finalized results report that run-scoped measurements are unavailable."
      : "Run-scoped measurements are unavailable or have not been recorded yet.";
  if (finalResults?.incomplete_reasons?.length) note += ` Result incomplete: ${finalResults.incomplete_reasons.join(" ")}`;
  if (finalResults?.latency_incomplete_reasons?.length) note += ` Latency incomplete: ${finalResults.latency_incomplete_reasons.join(" ")}`;
  document.getElementById("run-measurements-note").textContent = note;
}

function renderRunPage(snapshot) {
  runPageSnapshot = snapshot;
  const { run, experiment, execution, measurements, pending, results } = snapshot;
  const experimentName = experiment?.name || "Unavailable";
  document.getElementById("run-experiment-name").textContent = experimentName === "Unavailable" ? "" : experimentName;
  document.getElementById("run-experiment-title-value").textContent = experimentName;
  document.getElementById("experiment-run-title").textContent = `Run ${run.run_id}`;
  document.getElementById("run-experiment-id").textContent = run.experiment_id || "Unavailable";
  document.getElementById("run-run-id").textContent = run.run_id || "Unavailable";
  document.getElementById("run-created-at").textContent = formatExperimentDate(run.created_at);
  document.getElementById("run-started-at").textContent = formatExperimentDate(run.started_at);
  document.getElementById("run-back-to-experiment").href = `/experiments/${encodeURIComponent(run.experiment_id)}`;
  document.getElementById("run-missing-back-link").href = `/experiments/${encodeURIComponent(run.experiment_id)}`;

  const progress = execution?.progress || run.progress || {};
  const workload = execution?.workload || run.workload_snapshot || {};
  const target = Number.isInteger(progress.planned) ? progress.planned : workload.target_event_count;
  const generated = progress.generated;
  const accounting = run.event_accounting || {};
  setRunText("run-progress-target", runUnavailable(target));
  setRunText("run-progress-generated", runUnavailable(generated));
  setRunText("run-progress-published", runUnavailable(progress.publish_confirmed));
  setRunText("run-progress-publish-failed", runUnavailable(progress.publish_failed));
  setRunText("run-progress-processed", runUnavailable(accounting.consumer_processed_successes));
  setRunText("run-progress-processing-failed", runUnavailable(accounting.consumer_processed_failures));
  const draining = Boolean(run.stop_intent || run.event_ingestion_closed || run.execution?.interruption_recorded);
  setRunText("run-progress-pending", pending?.summary && Number.isInteger(pending.summary.pending_count)
    ? String(pending.summary.pending_count) : draining ? "Unavailable" : "Unavailable (not in a drain state)");
  const validProgress = Number.isInteger(target) && target > 0 && Number.isInteger(generated) && generated >= 0 && generated <= target;
  document.getElementById("run-progress-bar-wrap").hidden = !validProgress;
  if (validProgress) {
    const percent = generated / target * 100;
    document.getElementById("run-progress-bar").value = percent;
    document.getElementById("run-progress-fraction").textContent = `${generated} / ${target} generated (${percent.toFixed(0)}%)`;
  }
  const progressNote = document.getElementById("run-progress-note");
  progressNote.textContent = execution?.execution?.state === "EXPIRED"
    ? "Progress counters show the work recorded before the execution interruption."
    : "Generated, publish-confirmed, and consumer-processed counts are separate backend counters.";
  progressNote.hidden = false;

  const stopRequested = Boolean(run.stop_intent);
  const pendingResult = snapshot.pending?.summary;
  if (stopRequested && pendingResult) {
    setRunText("run-progress-pending", runUnavailable(pendingResult.pending_count));
  }
  renderRunControls(run);
  renderRunMeasurements(snapshot);
  renderRunTimeline(run);
  document.getElementById("run-page-loading").hidden = true;
  document.getElementById("run-page-error").hidden = true;
  document.getElementById("run-page-missing").hidden = true;
  document.getElementById("run-page-content").hidden = false;
}

async function refreshRunPage() {
  if (normalizedRoute() !== "run") return;
  if (runRefreshInFlight) {
    runRefreshQueued = true;
    return;
  }
  const routeIds = runRouteIds();
  const routeKey = `${routeIds.experimentId}/${routeIds.runId}`;
  if (runPageSnapshot && (runPageSnapshot.run.run_id !== routeIds.runId || runPageSnapshot.run.experiment_id !== routeIds.experimentId)) {
    runPageSnapshot = null;
  }
  runRefreshInFlight = true;
  const refreshButton = document.getElementById("run-refresh-button");
  const indicator = document.getElementById("run-refresh-indicator");
  const hadData = Boolean(runPageSnapshot);
  refreshButton.disabled = true;
  indicator.textContent = "Refreshing…";
  if (!hadData) {
    document.getElementById("run-page-loading").hidden = false;
    document.getElementById("run-page-error").hidden = true;
  }
  try {
    const { experimentId, runId } = routeIds;
    const runPath = runEndpointPath(experimentId, runId);
    const coreResponses = await Promise.all([
      requestJson(runPath, { headers: { Accept: "application/json" } }),
      requestJson(`/api/experiments/${encodeURIComponent(experimentId)}`, { headers: { Accept: "application/json" } }),
      requestJson(`${runPath}/execution`, { headers: { Accept: "application/json" } }),
    ]);
    const [runResponse, experimentResponse, executionResponse] = coreResponses;
    if (normalizedRoute() !== "run" || `${runRouteIds().experimentId}/${runRouteIds().runId}` !== routeKey) return;
    if (runResponse.response.status === 404 || experimentResponse.response.status === 404) {
      setRunError("Run or experiment was not found.", true);
      return;
    }
    for (const [response, body, label] of [[runResponse.response, runResponse.body, "run"], [experimentResponse.response, experimentResponse.body, "experiment"], [executionResponse.response, executionResponse.body, "execution progress"]]) {
      if (!response.ok) throw new Error(typeof body?.detail === "string" ? body.detail : `Could not load ${label} (${response.status}).`);
    }
    const run = { ...runResponse.body, ...{ event_accounting: runResponse.body.event_accounting } };
    const needsPending = Boolean(run.stop_intent || run.event_ingestion_closed || run.execution?.interruption_recorded);
    const terminal = ["COMPLETED", "FAILED", "CANCELLED"].includes(run.status);
    const optionalSpecs = [
      ["measurements", `/api/measurements?run_id=${encodeURIComponent(runId)}`],
      ...(needsPending ? [["pending", `${runPath}/pending-events`]] : []),
      ...(terminal && run.result_available ? [["results", `${runPath}/results`]] : []),
    ];
    const optional = await Promise.all(optionalSpecs.map(async ([key, url]) => {
      try {
        const result = await requestJson(url, { headers: { Accept: "application/json" } });
        if (key === "results" && result.response.status === 409) return [key, null, null];
        if (!result.response.ok) throw new Error(typeof result.body?.detail === "string" ? result.body.detail : `${key} unavailable (${result.response.status}).`);
        return [key, result.body, null];
      } catch (error) {
        return [key, null, error.message || `${key} unavailable.`];
      }
    }));
    if (normalizedRoute() !== "run" || `${runRouteIds().experimentId}/${runRouteIds().runId}` !== routeKey) return;
    const previous = runPageSnapshot || {};
    const next = { run, experiment: experimentResponse.body, execution: executionResponse.body,
      measurements: null, pending: null, results: null, optionalWarnings: [] };
    for (const [key, value, error] of optional) {
      if (error) {
        if (previous[key]) next[key] = previous[key];
        next.optionalWarnings.push(`${key} could not be refreshed; showing the last available value.`);
      } else next[key] = value;
    }
    if (!optionalSpecs.some(([key]) => key === "pending") && previous.pending && needsPending) next.pending = previous.pending;
    if (!optionalSpecs.some(([key]) => key === "results") && previous.results && terminal && run.result_available) next.results = previous.results;
    renderRunPage(next);
    document.getElementById("run-last-updated").textContent = `Last updated ${new Date().toLocaleString()}`;
    indicator.textContent = next.optionalWarnings.length ? next.optionalWarnings.join(" ") : "";
  } catch (error) {
    if (normalizedRoute() === "run" && `${runRouteIds().experimentId}/${runRouteIds().runId}` === routeKey) {
      setRunError(error.message || "Could not refresh run data.");
      indicator.textContent = "Refresh failed. Try again; previously displayed data is preserved.";
    }
  } finally {
    runRefreshInFlight = false;
    refreshButton.disabled = false;
    const routeChanged = normalizedRoute() === "run" && `${runRouteIds().experimentId}/${runRouteIds().runId}` !== routeKey;
    const refreshQueued = runRefreshQueued;
    runRefreshQueued = false;
    if (normalizedRoute() === "run" && (routeChanged || refreshQueued)) {
      void refreshRunPage();
    }
  }
}

async function startExperimentRun() {
  if (runActionInFlight || !runPageSnapshot || runPageSnapshot.run.status !== "CREATED") return;
  runActionInFlight = true;
  const message = document.getElementById("run-control-message");
  message.hidden = false;
  message.className = "form-submit-message";
  message.textContent = "Starting the run and submitting it to the controlled workload runner…";
  document.querySelectorAll("#run-control-buttons button").forEach((button) => { button.disabled = true; });
  const { experimentId, runId } = runRouteIds();
  const path = runEndpointPath(experimentId, runId);
  try {
    const started = await requestJson(`${path}/start`, { method: "POST", headers: { Accept: "application/json" } });
    if (!started.response.ok) throw new Error(validationMessage(started.body));
    const execution = await requestJson(`${path}/execute`, { method: "POST", headers: { Accept: "application/json" } });
    if (!execution.response.ok) throw new Error(validationMessage(execution.body));
    message.textContent = "The runner accepted the workload. Refresh for the latest persisted progress.";
    message.classList.add("success");
  } catch (error) {
    message.textContent = `Could not start run execution: ${error.message || "API request failed."} Refresh to verify its persisted state.`;
    message.classList.add("error");
  } finally {
    runActionInFlight = false;
    await refreshRunPage();
  }
}

async function stopExperimentRun() {
  if (runActionInFlight || !runPageSnapshot || runPageSnapshot.run.status !== "RUNNING" || runPageSnapshot.run.stop_intent) return;
  runActionInFlight = true;
  const message = document.getElementById("run-control-message");
  message.hidden = false;
  message.className = "form-submit-message";
  message.textContent = "Submitting stop request…";
  document.querySelectorAll("#run-control-buttons button").forEach((button) => { button.disabled = true; });
  const { experimentId, runId } = runRouteIds();
  try {
    const result = await requestJson(runEndpointPath(experimentId, runId, "/cancel"), {
      method: "POST", headers: { Accept: "application/json" },
    });
    if (!result.response.ok) throw new Error(validationMessage(result.body));
    message.textContent = "Stop request accepted. The run is draining; this does not confirm cancellation. Refresh to check finalization.";
    message.classList.add("success");
  } catch (error) {
    message.textContent = `Could not request stop: ${error.message || "API request failed."}`;
    message.classList.add("error");
  } finally {
    runActionInFlight = false;
    await refreshRunPage();
  }
}

document.getElementById("run-refresh-button").addEventListener("click", () => { void refreshRunPage(); });
document.getElementById("run-page-retry").addEventListener("click", () => { void refreshRunPage(); });

function setResultsError(message, missing = false) {
  const hasPriorData = Boolean(resultsPageSnapshot);
  document.getElementById("results-page-loading").hidden = true;
  document.getElementById("results-page-missing").hidden = !missing;
  const error = document.getElementById("results-page-error");
  error.hidden = missing || (hasPriorData && !message);
  document.getElementById("results-page-error-message").textContent = message || "Could not load this run.";
  document.getElementById("results-page-content").hidden = missing || !hasPriorData;
}

function setResultsValue(id, value) {
  document.getElementById(id).textContent = value === null || value === undefined || value === "" ? "Unavailable" : String(value);
}

function setCompletenessBadge(element, label, state, incomplete) {
  element.textContent = `${label}: ${state}`;
  element.classList.toggle("incomplete", incomplete);
}

function renderResultsPage(snapshot) {
  resultsPageSnapshot = snapshot;
  const { run, experiment, results, measurements: liveMeasurements, pending, optionalWarnings = [] } = snapshot;
  const terminal = ["COMPLETED", "FAILED", "CANCELLED"].includes(run.status);
  const draining = !terminal && Boolean(run.stop_intent);
  const interrupted = !terminal && Boolean(run.execution?.interruption_recorded);
  const persisted = results?.results || null;
  const finalRecordAvailable = Boolean(terminal && persisted);
  const name = experiment?.name || "Unavailable";
  document.getElementById("results-experiment-name").textContent = name === "Unavailable" ? "" : name;
  document.getElementById("experiment-results-title").textContent = `Results · ${run.run_id}`;
  const statusLabel = terminal ? run.status
    : interrupted && draining ? `INTERRUPTED · ${run.stop_intent.target} requested`
      : interrupted ? `INTERRUPTED · ${run.status}`
        : draining ? `DRAINING · ${run.stop_intent.target} requested`
          : run.event_ingestion_closed ? `INGESTION CLOSED · ${run.status}` : `PROVISIONAL · ${run.status}`;
  document.getElementById("results-status").textContent = statusLabel;
  document.getElementById("results-status").className = `run-status-badge status-${String(run.status || "unavailable").toLowerCase()}${draining || interrupted || run.event_ingestion_closed ? " status-draining" : !terminal ? " status-provisional" : ""}`;
  document.getElementById("results-experiment-name-value").textContent = name;
  setResultsValue("results-experiment-id", run.experiment_id);
  setResultsValue("results-run-id", run.run_id);
  document.getElementById("results-created-at").textContent = formatExperimentDate(run.created_at);
  document.getElementById("results-started-at").textContent = formatExperimentDate(run.started_at);
  document.getElementById("results-ended-at").textContent = run.ended_at ? formatExperimentDate(run.ended_at) : "Unavailable";
  const runUrl = `/experiments/${encodeURIComponent(run.experiment_id)}/runs/${encodeURIComponent(run.run_id)}`;
  document.getElementById("results-back-to-run").href = runUrl;
  document.getElementById("results-back-to-detail").href = `/experiments/${encodeURIComponent(run.experiment_id)}`;
  document.getElementById("results-missing-back-to-experiment").href = `/experiments/${encodeURIComponent(run.experiment_id)}`;

  const provisionalNote = document.getElementById("results-provisional-note");
  provisionalNote.hidden = true;
  if (!terminal) {
    provisionalNote.textContent = interrupted
      ? `Execution was interrupted${run.execution.interruption_reason ? `: ${run.execution.interruption_reason}` : "."} ${draining ? `Stop was requested for ${run.stop_intent.target}; the run remains in drain/finalization.` : "The persisted run outcome is not terminal yet."}`
      : draining
        ? `Stop requested for ${run.stop_intent.target}. The run is draining; values remain provisional until finalization.`
        : run.event_ingestion_closed
          ? "Event ingestion is closed. The run is awaiting its persisted lifecycle outcome; values remain provisional."
          : `These values are provisional. The run is still ${run.status}; persisted final results and completeness have not been established.`;
    provisionalNote.hidden = false;
  } else if (!finalRecordAvailable) {
    provisionalNote.textContent = "The run is terminal, but a persisted result record is unavailable.";
    provisionalNote.hidden = false;
  }

  const progress = run.progress || {};
  const accounting = run.event_accounting || {};
  const workload = run.workload_snapshot || {};
  const pendingApplicable = Boolean(run.stop_intent || run.event_ingestion_closed);
  setResultsValue("results-target-events", runUnavailable(workload.target_event_count));
  setResultsValue("results-generated-events", runUnavailable(progress.generated));
  setResultsValue("results-published-events", runUnavailable(progress.publish_confirmed));
  setResultsValue("results-processed-events", runUnavailable(accounting.consumer_processed_successes));
  setResultsValue("results-publish-failures", runUnavailable(progress.publish_failed));
  setResultsValue("results-processing-failures", runUnavailable(accounting.consumer_processed_failures));
  const pendingCount = pending?.summary?.pending_count;
  setResultsValue("results-pending-events", Number.isInteger(pendingCount)
    ? String(pendingCount) : pendingApplicable ? "Unavailable" : "Unavailable (not in a drain state)");

  const measurements = finalRecordAvailable
    ? (persisted.measurements_available ? persisted.measurements : null)
    : (!terminal ? liveMeasurements : null);
  const measurementsPresent = Boolean(measurements?.measurements_present);
  const throughput = finalRecordAvailable
    ? persisted.benchmark_throughput_events_per_second
    : (!terminal && measurementsPresent ? measurements.throughput_events_per_second : null);
  const averageLatency = measurementsPresent ? measurements.average_latency_ms?.consumer_processing_latency_ms : null;
  const p95Latency = measurementsPresent ? measurements.p95_latency_ms?.consumer_processing_latency_ms : null;
  setResultsValue("results-throughput", runMetric(throughput, " events/s"));
  setResultsValue("results-average-latency", runMetric(averageLatency, " ms"));
  setResultsValue("results-p95-latency", runMetric(p95Latency, " ms"));

  const resultBadge = document.getElementById("results-result-complete");
  const latencyBadge = document.getElementById("results-latency-complete");
  if (finalRecordAvailable) {
    setCompletenessBadge(resultBadge, "Result completeness", persisted.result_complete === true ? "Complete" : persisted.result_complete === false ? "Incomplete" : "Unavailable", persisted.result_complete === false);
    setCompletenessBadge(latencyBadge, "Latency completeness", persisted.latency_complete === true ? "Complete" : persisted.latency_complete === false ? "Incomplete" : "Unavailable", persisted.latency_complete === false);
  } else if (!terminal) {
    setCompletenessBadge(resultBadge, "Result completeness", "Provisional", false);
    setCompletenessBadge(latencyBadge, "Latency completeness", "Provisional", false);
  } else {
    setCompletenessBadge(resultBadge, "Result completeness", "Unavailable", false);
    setCompletenessBadge(latencyBadge, "Latency completeness", "Unavailable", false);
  }
  const notes = [];
  if (persisted?.incomplete_reasons?.length) notes.push(...persisted.incomplete_reasons.map((reason) => `Result: ${reason}`));
  if (persisted?.latency_incomplete_reasons?.length) notes.push(...persisted.latency_incomplete_reasons.map((reason) => `Latency: ${reason}`));
  if (!measurementsPresent) notes.push(finalRecordAvailable ? "Persisted run-scoped measurements are unavailable." : "Run-scoped measurements are unavailable or have not been recorded yet.");
  if (!terminal) notes.push("Provisional measurements may change before finalization.");
  if (optionalWarnings.length) notes.push(...optionalWarnings);
  document.getElementById("results-completeness-notes").textContent = notes.join(" ");

  document.getElementById("results-page-loading").hidden = true;
  document.getElementById("results-page-error").hidden = true;
  document.getElementById("results-page-missing").hidden = true;
  document.getElementById("results-page-content").hidden = false;
}

async function loadComparisonRuns(experimentId, currentRunId) {
  const requestId = ++comparisonRequestId;
  const routeMatchesRequest = () => requestId === comparisonRequestId
    && normalizedRoute() === "results"
    && runRouteIds().experimentId === experimentId
    && runRouteIds().runId === currentRunId;
  document.getElementById("comparison-loading").hidden = false;
  document.getElementById("comparison-error").hidden = true;
  document.getElementById("comparison-empty").hidden = true;
  document.getElementById("comparison-selection-note").hidden = true;
  document.getElementById("comparison-limitation-note").hidden = true;
  document.getElementById("comparison-run-selector").hidden = true;
  document.getElementById("comparison-table-wrap").hidden = true;
  try {
    const { response, body } = await requestJson(`/api/experiments/${encodeURIComponent(experimentId)}/runs`, { headers: { Accept: "application/json" } });
    if (!response.ok) throw new Error(typeof body?.detail === "string" ? body.detail : `Could not load experiment runs (${response.status}).`);
    if (!Array.isArray(body?.runs)) throw new Error("The API returned an invalid run list.");
    if (!routeMatchesRequest()) return;
    comparisonEligibleRuns = body.runs.filter((run) => ["COMPLETED", "FAILED", "CANCELLED"].includes(run.status) && run.result_available === true);
    const eligibleIds = new Set(comparisonEligibleRuns.map((run) => run.run_id));
    selectedComparisonRunIds = new Set([...selectedComparisonRunIds].filter((id) => eligibleIds.has(id)));
    if (!selectedComparisonRunIds.size && eligibleIds.has(currentRunId)) selectedComparisonRunIds.add(currentRunId);
    if (!comparisonEligibleRuns.length) {
      document.getElementById("comparison-empty").textContent = "No terminal runs with persisted results are available to compare for this experiment.";
      document.getElementById("comparison-empty").hidden = false;
      return;
    }
    renderComparisonOptions();
    await loadSelectedRunComparison();
  } catch (error) {
    if (!routeMatchesRequest()) return;
    document.getElementById("comparison-error-message").textContent = error.message || "Could not load runs for comparison.";
    document.getElementById("comparison-error").hidden = false;
  } finally {
    if (routeMatchesRequest()) document.getElementById("comparison-loading").hidden = true;
  }
}

function renderComparisonOptions() {
  const fieldset = document.getElementById("comparison-run-selector");
  const options = document.getElementById("comparison-run-options");
  options.replaceChildren();
  fieldset.hidden = false;
  for (const run of comparisonEligibleRuns) {
    const label = document.createElement("label");
    label.className = "comparison-run-option";
    const checkbox = document.createElement("input");
    checkbox.type = "checkbox";
    checkbox.value = run.run_id;
    checkbox.checked = selectedComparisonRunIds.has(run.run_id);
    checkbox.addEventListener("change", () => {
      if (checkbox.checked) selectedComparisonRunIds.add(run.run_id);
      else selectedComparisonRunIds.delete(run.run_id);
      void loadSelectedRunComparison();
    });
    const configuration = run.configuration_snapshot || {};
    const workload = run.workload_snapshot || {};
    const summary = `Target ${Number.isInteger(workload.target_event_count) ? workload.target_event_count : "Unavailable"} · Windows ${configuration.tumbling_size ?? "Unavailable"}/${configuration.sliding_size ?? "Unavailable"}/${configuration.sliding_interval ?? "Unavailable"}`;
    const text = experimentElement("span", "comparison-run-option-copy", `Run ${run.run_id} · ${run.status} · ${formatExperimentDate(run.created_at)} · ${summary}`);
    label.append(checkbox, text);
    options.append(label);
  }
  updateComparisonSelectionNote();
}

function updateComparisonSelectionNote() {
  const note = document.getElementById("comparison-selection-note");
  note.textContent = selectedComparisonRunIds.size < 2
    ? `Select at least two eligible runs to compare (${selectedComparisonRunIds.size} selected).`
    : `${selectedComparisonRunIds.size} runs selected. All belong to this experiment.`;
  note.hidden = false;
}

function comparisonCell(value, suffix = "", completeness = "complete") {
  const cell = document.createElement("td");
  if (typeof value !== "number" || !Number.isFinite(value)) {
    cell.textContent = "Unavailable";
    return cell;
  }
  const rendered = Number.isInteger(value) ? value.toLocaleString() : value.toFixed(2);
  cell.textContent = `${rendered}${suffix}${completeness === "incomplete" ? " · incomplete" : ""}`;
  if (completeness === "incomplete") cell.className = "comparison-incomplete-value";
  return cell;
}

function renderComparisonTable(records) {
  const head = document.getElementById("comparison-table-head");
  const body = document.getElementById("comparison-table-body");
  head.replaceChildren();
  body.replaceChildren();
  const headingRow = document.createElement("tr");
  headingRow.append(experimentElement("th", "", "Metric"));
  for (const record of records) {
    const th = document.createElement("th");
    const wrapper = experimentElement("div", "comparison-column-heading");
    wrapper.append(experimentElement("strong", "", `Run ${record.run.run_id}`),
      experimentElement("span", "", record.run.status),
      experimentElement("small", "", `Result ${record.result.result_complete === true ? "complete" : record.result.result_complete === false ? "incomplete" : "unavailable"} · Latency ${record.result.latency_complete === true ? "complete" : record.result.latency_complete === false ? "incomplete" : "unavailable"}`));
    th.append(wrapper);
    headingRow.append(th);
  }
  head.append(headingRow);
  const rows = [
    ["Generated events", (record) => comparisonCell(record.run.progress?.generated)],
    ["Publish confirmed", (record) => comparisonCell(record.run.progress?.publish_confirmed)],
    ["Processed successfully", (record) => comparisonCell(record.result.event_accounting?.consumer_processed_successes)],
    ["Publish failures", (record) => comparisonCell(record.run.progress?.publish_failed)],
    ["Processing failures", (record) => comparisonCell(record.result.event_accounting?.consumer_processed_failures)],
    ["Throughput", (record) => comparisonCell(record.result.benchmark_throughput_events_per_second, " events/s", record.result.result_complete === false ? "incomplete" : "complete")],
    ["Average processing latency", (record) => comparisonCell(record.result.measurements_available ? record.result.measurements?.average_latency_ms?.consumer_processing_latency_ms : null, " ms", record.result.latency_complete === false ? "incomplete" : "complete")],
    ["P95 processing latency", (record) => comparisonCell(record.result.measurements_available ? record.result.measurements?.p95_latency_ms?.consumer_processing_latency_ms : null, " ms", record.result.latency_complete === false ? "incomplete" : "complete")],
  ];
  for (const [label, render] of rows) {
    const row = document.createElement("tr");
    row.append(experimentElement("th", "", label));
    for (const record of records) row.append(render(record));
    body.append(row);
  }
  document.getElementById("comparison-table-wrap").hidden = false;
  document.getElementById("comparison-limitation-note").hidden = false;
}

async function loadSelectedRunComparison() {
  const route = runRouteIds();
  const routeKey = `${route.experimentId}/${route.runId}`;
  const selected = comparisonEligibleRuns.filter((run) => selectedComparisonRunIds.has(run.run_id));
  updateComparisonSelectionNote();
  document.getElementById("comparison-error").hidden = true;
  document.getElementById("comparison-limitation-note").hidden = true;
  if (selected.length < 2) {
    document.getElementById("comparison-table-wrap").hidden = true;
    return;
  }
  const requestId = ++comparisonRequestId;
  const routeMatchesRequest = () => requestId === comparisonRequestId
    && normalizedRoute() === "results"
    && `${runRouteIds().experimentId}/${runRouteIds().runId}` === routeKey;
  document.getElementById("comparison-loading").hidden = false;
  document.getElementById("comparison-table-wrap").hidden = true;
  const { experimentId } = route;
  try {
    const records = await Promise.all(selected.map(async (run) => {
      const base = runEndpointPath(experimentId, run.run_id);
      const [detail, results] = await Promise.all([
        requestJson(base, { headers: { Accept: "application/json" } }),
        requestJson(`${base}/results`, { headers: { Accept: "application/json" } }),
      ]);
      if (!detail.response.ok) throw new Error(typeof detail.body?.detail === "string" ? detail.body.detail : `Could not load run ${run.run_id}.`);
      if (!results.response.ok) throw new Error(typeof results.body?.detail === "string" ? results.body.detail : `Could not load persisted results for ${run.run_id}.`);
      return { run: detail.body, result: results.body.results };
    }));
    if (!routeMatchesRequest()) return;
    renderComparisonTable(records);
  } catch (error) {
    if (!routeMatchesRequest()) return;
    document.getElementById("comparison-error-message").textContent = error.message || "Could not load selected run results.";
    document.getElementById("comparison-error").hidden = false;
  } finally {
    if (routeMatchesRequest()) document.getElementById("comparison-loading").hidden = true;
  }
}

async function refreshResultsPage() {
  if (normalizedRoute() !== "results") return;
  const routeIds = runRouteIds();
  const routeKey = `${routeIds.experimentId}/${routeIds.runId}`;
  if (resultsRefreshInFlight) {
    if (resultsRefreshRouteKey !== routeKey) resultsRefreshQueued = true;
    return;
  }
  resultsRefreshRouteKey = routeKey;
  if (resultsPageSnapshot && (resultsPageSnapshot.run.run_id !== routeIds.runId || resultsPageSnapshot.run.experiment_id !== routeIds.experimentId)) {
    resultsPageSnapshot = null;
  }
  resultsRefreshInFlight = true;
  const button = document.getElementById("results-refresh-button");
  button.disabled = true;
  if (!resultsPageSnapshot) {
    document.getElementById("results-page-loading").hidden = false;
    document.getElementById("results-page-error").hidden = true;
  }
  try {
    const { experimentId, runId } = routeIds;
    const base = runEndpointPath(experimentId, runId);
    const [runResponse, experimentResponse] = await Promise.all([
      requestJson(base, { headers: { Accept: "application/json" } }),
      requestJson(`/api/experiments/${encodeURIComponent(experimentId)}`, { headers: { Accept: "application/json" } }),
    ]);
    if (normalizedRoute() !== "results" || `${runRouteIds().experimentId}/${runRouteIds().runId}` !== routeKey) return;
    if (runResponse.response.status === 404 || experimentResponse.response.status === 404) {
      setResultsError("Run or experiment was not found.", true);
      return;
    }
    for (const [response, body, label] of [[runResponse.response, runResponse.body, "run"], [experimentResponse.response, experimentResponse.body, "experiment"]]) {
      if (!response.ok) throw new Error(typeof body?.detail === "string" ? body.detail : `Could not load ${label} (${response.status}).`);
    }
    const run = runResponse.body;
    const terminal = ["COMPLETED", "FAILED", "CANCELLED"].includes(run.status);
    const pendingApplicable = Boolean(run.stop_intent || run.event_ingestion_closed);
    const optionalSpecs = [
      ...(terminal && run.result_available ? [["results", `${base}/results`]] : []),
      ...(!terminal ? [["measurements", `/api/measurements?run_id=${encodeURIComponent(runId)}`]] : []),
      ...(pendingApplicable ? [["pending", `${base}/pending-events`]] : []),
    ];
    const optionalData = await Promise.all(optionalSpecs.map(async ([key, url]) => {
      try {
        const result = await requestJson(url, { headers: { Accept: "application/json" } });
        if (key === "results" && result.response.status === 409) return [key, null, null];
        if (!result.response.ok) throw new Error(typeof result.body?.detail === "string" ? result.body.detail : `${key} unavailable (${result.response.status}).`);
        return [key, result.body, null];
      } catch (error) {
        return [key, null, error.message || `${key} unavailable.`];
      }
    }));
    if (normalizedRoute() !== "results" || `${runRouteIds().experimentId}/${runRouteIds().runId}` !== routeKey) return;
    const previous = resultsPageSnapshot || {};
    const snapshot = { run, experiment: experimentResponse.body, results: null, measurements: null, pending: null, optionalWarnings: [] };
    for (const [key, value, error] of optionalData) {
      if (error) {
        if (previous[key]) snapshot[key] = previous[key];
        snapshot.optionalWarnings.push(`${key} could not be refreshed; showing the last available value.`);
      } else snapshot[key] = value;
    }
    if (!optionalSpecs.some(([key]) => key === "pending") && pendingApplicable) snapshot.pending = previous.pending || null;
    if (!optionalSpecs.some(([key]) => key === "results") && terminal && run.result_available) snapshot.results = previous.results || null;
    renderResultsPage(snapshot);
    document.getElementById("results-last-updated").textContent = `Last updated ${new Date().toLocaleString()}`;
    void loadComparisonRuns(experimentId, runId);
  } catch (error) {
    if (normalizedRoute() === "results" && `${runRouteIds().experimentId}/${runRouteIds().runId}` === routeKey) {
      setResultsError(error.message || "Could not refresh results.");
    }
  } finally {
    resultsRefreshInFlight = false;
    resultsRefreshRouteKey = null;
    button.disabled = false;
    const currentRouteIds = runRouteIds();
    const routeChanged = normalizedRoute() === "results"
      && `${currentRouteIds.experimentId}/${currentRouteIds.runId}` !== routeKey;
    const refreshQueued = resultsRefreshQueued;
    resultsRefreshQueued = false;
    if (normalizedRoute() === "results" && (routeChanged || refreshQueued)) {
      void refreshResultsPage();
    }
  }
}

document.getElementById("results-refresh-button").addEventListener("click", () => { void refreshResultsPage(); });
document.getElementById("results-page-retry").addEventListener("click", () => { void refreshResultsPage(); });
document.getElementById("comparison-retry").addEventListener("click", () => {
  const { experimentId, runId } = runRouteIds();
  void loadComparisonRuns(experimentId, runId);
});

function setFieldError(id, message) {
  const field = document.getElementById(id);
  if (field) field.textContent = message || "";
}

function integerField(id, min, max, errorId, label) {
  const raw = document.getElementById(id).value.trim();
  const value = Number(raw);
  if (!/^\d+$/.test(raw) || !Number.isSafeInteger(value) || value < min || value > max) {
    setFieldError(errorId, `${label} must be a whole number from ${min.toLocaleString()} to ${max.toLocaleString()}.`);
    return null;
  }
  setFieldError(errorId, "");
  return value;
}

function syncSlidingIntervalOptions() {
  const size = Number(document.getElementById("default-sliding-size").value);
  const select = document.getElementById("default-sliding-interval");
  const previous = Number(select.value) || 1;
  select.replaceChildren();
  for (let interval = 1; interval < size; interval += 1) {
    select.append(experimentElement("option", "", `${interval} ${interval === 1 ? "event" : "events"}`));
    select.lastElementChild.value = String(interval);
  }
  select.value = String(Math.min(previous, size - 1));
}

function validationMessage(body) {
  if (typeof body?.detail === "string") return body.detail;
  if (Array.isArray(body?.detail)) return body.detail.map((item) => item?.msg).filter(Boolean).join(" ") || "The API rejected this experiment definition.";
  return "The API rejected this experiment definition.";
}

function validateExperimentForm() {
  let valid = true;
  const name = document.getElementById("experiment-name").value.trim();
  const description = document.getElementById("experiment-description").value.trim();
  setFieldError("experiment-name-error", name ? "" : "Enter an experiment name.");
  if (!name) valid = false;
  setFieldError("experiment-description-error", description.length > 2000 ? "Description must be 2,000 characters or fewer." : "");
  if (description.length > 2000) valid = false;

  const tumblingSize = Number(document.getElementById("default-tumbling-size").value);
  const slidingSize = Number(document.getElementById("default-sliding-size").value);
  const slidingInterval = Number(document.getElementById("default-sliding-interval").value);
  const supportedSizes = [5, 8, 10];
  setFieldError("tumbling-size-error", supportedSizes.includes(tumblingSize) ? "" : "Choose a supported window size.");
  setFieldError("sliding-size-error", supportedSizes.includes(slidingSize) ? "" : "Choose a supported window size.");
  setFieldError("sliding-interval-error", Number.isInteger(slidingInterval) && slidingInterval > 0 && slidingInterval < slidingSize ? "" : "Interval must be smaller than the sliding window.");
  if (!supportedSizes.includes(tumblingSize) || !supportedSizes.includes(slidingSize) || !(slidingInterval > 0 && slidingInterval < slidingSize)) valid = false;

  const target = integerField("target-event-count", 1, 10000, "target-event-count-error", "Target events");
  const publishInterval = integerField("publish-interval-ms", 10, 60000, "publish-interval-error", "Publish interval");
  if (target === null || publishInterval === null) valid = false;
  const selected = [...document.querySelectorAll('input[name="event_type"]:checked')];
  setFieldError("event-distribution-error", selected.length ? "" : "Select at least one supported event type.");
  if (!selected.length) valid = false;
  const distribution = {};
  for (const checkbox of selected) {
    const weightInput = document.querySelector(`[data-event-weight="${checkbox.value}"]`);
    const raw = weightInput.value.trim();
    const weight = Number(raw);
    if (!/^\d+$/.test(raw) || !Number.isSafeInteger(weight) || weight < 1 || weight > 1000) {
      setFieldError("event-distribution-error", "Selected event weights must be whole numbers from 1 to 1,000.");
      valid = false;
    } else distribution[checkbox.value] = weight;
  }
  return { valid, payload: {
    name, description: description || null,
    experiment_type: document.getElementById("experiment-type").value,
    default_configuration: { tumbling_size: tumblingSize, sliding_size: slidingSize, sliding_interval: slidingInterval },
    default_workload_configuration: { target_event_count: target, publish_interval_ms: publishInterval, event_type_distribution: distribution },
  } };
}

document.getElementById("experiments-retry").addEventListener("click", () => { void loadExperiments(); });
document.getElementById("default-sliding-size").addEventListener("change", syncSlidingIntervalOptions);
syncSlidingIntervalOptions();

document.getElementById("experiment-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const message = document.getElementById("experiment-form-message");
  message.hidden = true;
  message.className = "form-submit-message";
  const { valid, payload } = validateExperimentForm();
  if (!valid) {
    message.textContent = "Check the highlighted fields and try again.";
    message.hidden = false;
    document.querySelector(".field-error:not(:empty)")?.scrollIntoView({ block: "center", behavior: "smooth" });
    return;
  }

  const submit = document.getElementById("create-experiment-submit");
  submit.disabled = true;
  submit.textContent = "Saving definition…";
  try {
    const response = await fetch("/api/experiments", {
      method: "POST", headers: { "Content-Type": "application/json", Accept: "application/json" },
      body: JSON.stringify(payload),
    });
    let body;
    try { body = await response.json(); } catch { body = null; }
    if (!response.ok) throw new Error(validationMessage(body));
    if (!body?.experiment_id) throw new Error("The API did not confirm the saved experiment definition.");
    window.history.pushState({}, "", "/experiments?created=1");
    renderRoute();
    window.scrollTo({ top: 0, behavior: "auto" });
  } catch (error) {
    message.textContent = error.message || "Could not save the experiment. Your entries are still here; try again.";
    message.hidden = false;
  } finally {
    submit.disabled = false;
    submit.textContent = "Create Experiment";
  }
});

function renderRoute() {
  const activeRoute = normalizedRoute();
  document.querySelectorAll("[data-route-view]").forEach((view) => {
    view.hidden = view.dataset.routeView !== activeRoute;
  });
  document.querySelectorAll("[data-route-link]").forEach((link) => {
    const linkRoute = normalizedRoute(new URL(link.href, window.location.href).pathname);
    if (linkRoute === activeRoute || ((activeRoute === "create" || activeRoute === "details" || activeRoute === "run" || activeRoute === "results") && linkRoute === "experiments")) {
      link.setAttribute("aria-current", "page");
    } else {
      link.removeAttribute("aria-current");
    }
  });
  document.title = activeRoute === "dashboard" ? "StreamPulse — Library Analytics"
    : activeRoute === "create" ? "StreamPulse — Create Experiment"
      : activeRoute === "details" ? "StreamPulse — Experiment Details"
        : activeRoute === "run" ? "StreamPulse — Live Experiment Run"
          : activeRoute === "results" ? "StreamPulse — Run Results"
            : activeRoute === "failureRecovery" ? "StreamPulse — Failure & Recovery" : "StreamPulse — Experiments";
  document.getElementById("experiment-created-message").hidden = !(activeRoute === "experiments" && new URLSearchParams(window.location.search).has("created"));
  if (activeRoute !== "dashboard") window.clearTimeout(refreshTimer);
  else void refreshLiveData().finally(scheduleRefresh);
  if (activeRoute === "experiments") void loadExperiments();
  if (activeRoute === "details") void loadExperimentDetail();
  if (activeRoute === "run") {
    const { experimentId, runId } = runRouteIds();
    if (runPageSnapshot && (runPageSnapshot.run.run_id !== runId || runPageSnapshot.run.experiment_id !== experimentId)) {
      runPageSnapshot = null;
      document.getElementById("run-page-content").hidden = true;
      document.getElementById("run-page-loading").hidden = false;
      document.getElementById("run-last-updated").textContent = "";
    }
    void refreshRunPage();
  }
  if (activeRoute === "results") {
    const { experimentId, runId } = runRouteIds();
    if (resultsPageSnapshot && (resultsPageSnapshot.run.run_id !== runId || resultsPageSnapshot.run.experiment_id !== experimentId)) {
      resultsPageSnapshot = null;
      document.getElementById("results-page-content").hidden = true;
      document.getElementById("results-page-loading").hidden = false;
      document.getElementById("results-last-updated").textContent = "";
    }
    void refreshResultsPage();
  }
}

document.addEventListener("click", (event) => {
  const link = event.target.closest("a[data-route-link]");
  if (!link || event.defaultPrevented || event.button !== 0
      || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;

  const destination = new URL(link.href, window.location.href);
  const detailPath = /^\/experiments\/[0-9a-fA-F-]{36}$/.test(destination.pathname);
  const runPath = /^\/experiments\/[0-9a-fA-F-]{36}\/runs\/[0-9a-fA-F-]{36}$/.test(destination.pathname);
  const resultsPath = /^\/experiments\/[0-9a-fA-F-]{36}\/runs\/[0-9a-fA-F-]{36}\/results$/.test(destination.pathname);
  if (destination.origin !== window.location.origin
      || (!detailPath && !runPath && !resultsPath && !["/", "/experiments", "/experiments/new", "/failure-recovery"].includes(destination.pathname.replace(/\/+$/, "") || "/"))) return;

  event.preventDefault();
  const currentPath = window.location.pathname.replace(/\/+$/, "") || "/";
  const nextPath = `${destination.pathname.replace(/\/+$/, "") || "/"}${destination.search}${destination.hash}`;
  if (`${currentPath}${window.location.search}${window.location.hash}` !== nextPath) {
    window.history.pushState({}, "", nextPath);
  }
  renderRoute();
  window.scrollTo({ top: 0, behavior: "auto" });
});

window.addEventListener("popstate", renderRoute);
renderRoute();
