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
  if (monitoringFailed()) return ["Monitoring data is unavailable because the latest monitoring request failed. No component failure is inferred from that request failure."];
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
      not_initialized: "Consumer monitoring has not been initialized; this does not mean the consumer crashed.",
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
    // A failed API monitoring request does not prove that RabbitMQ or Redis is down.
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
  else {
    void refreshLiveData();
    scheduleRefresh();
  }
});
window.addEventListener("pagehide", () => window.clearTimeout(refreshTimer));

void refreshLiveData().finally(scheduleRefresh);
