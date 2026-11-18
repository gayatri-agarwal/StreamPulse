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
  graphPage: document.getElementById("graph-page"),
  navLab: document.getElementById("nav-lab"),
  navRecovery: document.getElementById("nav-recovery"),
  navGraph: document.getElementById("nav-graph"),
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
  queueUnacknowledged: document.getElementById("queue-unacknowledged"),
  consumerCount: document.getElementById("consumer-count"),
  consumerState: document.getElementById("consumer-state"),
  processedCount: document.getElementById("processed-count"),
  processingFailures: document.getElementById("processing-failures"),
  throughput: document.getElementById("throughput"),
  publishLatency: document.getElementById("publish-latency"),
  processingLatency: document.getElementById("processing-latency"),
  aggregationMessage: document.getElementById("aggregation-message"),
  graphDataState: document.getElementById("graph-data-state"),
  graphUpdated: document.getElementById("graph-updated"),
  graphProcessingAverage: document.getElementById("graph-processing-average"),
  graphProcessingMinimum: document.getElementById("graph-processing-minimum"),
  graphProcessingMaximum: document.getElementById("graph-processing-maximum"),
  graphProcessedTotal: document.getElementById("graph-processed-total"),
  graphProcessingEmpty: document.getElementById("graph-processing-empty"),
  graphWindowCountChart: document.getElementById("graph-window-count-chart"),
  graphWindowCountEmpty: document.getElementById("graph-window-count-empty"),
  graphWindowLatencyChart: document.getElementById("graph-window-latency-chart"),
  graphWindowLatencyEmpty: document.getElementById("graph-window-latency-empty"),
  graphComparisonBody: document.getElementById("graph-comparison-body"),
  graphTumblingTrendChart: document.getElementById("graph-tumbling-trend-chart"),
  graphTumblingTrendEmpty: document.getElementById("graph-tumbling-trend-empty"),
  graphSlidingTrendChart: document.getElementById("graph-sliding-trend-chart"),
  graphSlidingTrendEmpty: document.getElementById("graph-sliding-trend-empty"),
  overallMetricsView: document.getElementById("overall-metrics-view"),
  currentExperimentView: document.getElementById("current-experiment-view"),
  overallMetricsTab: document.getElementById("overall-metrics-tab"),
  currentExperimentTab: document.getElementById("current-experiment-tab"),
  currentExperimentLabel: document.getElementById("current-experiment-label"),
  currentExperimentEmpty: document.getElementById("current-experiment-empty"),
  currentProcessedCount: document.getElementById("current-processed-count"),
  currentFailedCount: document.getElementById("current-failed-count"),
  currentLatencyAverage: document.getElementById("current-latency-average"),
  currentLatencyMinimum: document.getElementById("current-latency-minimum"),
  currentLatencyMaximum: document.getElementById("current-latency-maximum"),
  currentWindowCountChart: document.getElementById("current-window-count-chart"),
  currentWindowCountEmpty: document.getElementById("current-window-count-empty"),
  currentWindowValueChart: document.getElementById("current-window-value-chart"),
  currentWindowValueEmpty: document.getElementById("current-window-value-empty"),
  activeWindowConfig: document.getElementById("active-window-config"),
  pendingWindowConfig: document.getElementById("pending-window-config"),
  windowConfigForm: document.getElementById("window-config-form"),
  configTumblingSize: document.getElementById("config-tumbling-size"),
  configSlidingSize: document.getElementById("config-sliding-size"),
  configSlidingInterval: document.getElementById("config-sliding-interval"),
  windowConfigFeedback: document.getElementById("window-config-feedback"),
};

let selectedMode = "steady";
let simulation = null;
let simulationStatus = null;
let graphMetrics = null;
let graphMeasurements = null;
let graphMonitoring = null;
let currentSimulationData = null;
const graphTrendHistory = { tumbling: [], sliding: [] };
const graphTrendMarkers = { tumbling: null, sliding: null };
let monitoring = null;
let windowConfiguration = null;
let health = null;
let pollTimer = null;
let pollInFlight = false;
let pollCount = 0;
let actionInFlight = false;
let lastActivityError = null;

function currentPage() {
  const pathname = window.location.pathname.replace(/\/+$/, "") || "/";
  const page = pathname === "/failure-recovery" ? "recovery"
    : window.location.hash === "#graph-analysis" ? "graph" : "lab";
  elements.labPage.hidden = page !== "lab";
  elements.failurePage.hidden = page !== "recovery";
  elements.graphPage.hidden = page !== "graph";
  for (const link of [elements.navLab, elements.navRecovery, elements.navGraph]) {
    link.removeAttribute("aria-current");
  }
  const activeLink = page === "recovery" ? elements.navRecovery
    : page === "graph" ? elements.navGraph : elements.navLab;
  activeLink.setAttribute("aria-current", "page");
  return page;
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

function configurationLabel(configuration) {
  return configuration
    ? `Tumbling ${configuration.tumbling_size} · Sliding ${configuration.sliding_size} · Interval ${configuration.sliding_interval}`
    : "None";
}

function renderWindowConfiguration() {
  elements.activeWindowConfig.textContent = configurationLabel(windowConfiguration?.active);
  elements.pendingWindowConfig.textContent = configurationLabel(windowConfiguration?.pending);
  const values = windowConfiguration?.pending || windowConfiguration?.active;
  if (values && !elements.windowConfigForm.contains(document.activeElement)) {
    elements.configTumblingSize.value = values.tumbling_size;
    elements.configSlidingSize.value = values.sliding_size;
    elements.configSlidingInterval.value = values.sliding_interval;
  }
  elements.configSlidingInterval.max = String(Number(elements.configSlidingSize.value) - 1);
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
  setText(elements.queueUnacknowledged, reportedQueue?.unacknowledged_messages);

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

function svgElement(name, attributes = {}, label = null) {
  const node = document.createElementNS("http://www.w3.org/2000/svg", name);
  for (const [key, value] of Object.entries(attributes)) node.setAttribute(key, String(value));
  if (label !== null) node.textContent = String(label);
  return node;
}

function renderGroupedBars(svg, emptyMessage, rows) {
  const usable = rows.filter((row) => Number.isFinite(row.tumbling) || Number.isFinite(row.sliding));
  svg.replaceChildren();
  const hasData = usable.length > 0;
  svg.hidden = !hasData;
  emptyMessage.hidden = hasData;
  if (!hasData) return;

  const width = 720;
  const top = 42;
  const plotLeft = 190;
  const plotWidth = 430;
  const rowHeight = Math.min(70, Math.max(54, Math.floor(190 / usable.length)));
  const height = top + usable.length * rowHeight + 16;
  svg.setAttribute("viewBox", `0 0 ${width} ${height}`);
  const maxValue = Math.max(0, ...usable.flatMap((row) => [row.tumbling, row.sliding]
    .filter((value) => Number.isFinite(value))));
  const scaleMaximum = maxValue > 0 ? maxValue : 1;
  const colors = { tumbling: "#386c93", sliding: "#19877f" };
  const legend = [
    { key: "tumbling", label: "Tumbling", x: plotLeft },
    { key: "sliding", label: "Sliding", x: plotLeft + 130 },
  ];
  for (const item of legend) {
    svg.append(
      svgElement("rect", { x: item.x, y: 12, width: 10, height: 10, rx: 2, fill: colors[item.key] }),
      svgElement("text", { x: item.x + 16, y: 21, class: "svg-legend-label" }, item.label),
    );
  }
  svg.append(svgElement("line", { x1: plotLeft, y1: top - 5, x2: plotLeft, y2: height - 10, class: "svg-axis" }));

  usable.forEach((row, index) => {
    const y = top + index * rowHeight;
    svg.append(svgElement("text", { x: plotLeft - 12, y: y + 26, "text-anchor": "end", class: "svg-category" }, row.label));
    for (const [seriesIndex, key] of ["tumbling", "sliding"].entries()) {
      const value = row[key];
      if (!Number.isFinite(value)) continue;
      const barY = y + 5 + seriesIndex * 22;
      const barWidth = Math.max(0, plotWidth * value / scaleMaximum);
      svg.append(
        svgElement("rect", { x: plotLeft, y: barY, width: barWidth, height: 15, rx: 4, fill: colors[key] }),
        svgElement("text", {
          x: Math.min(width - 12, plotLeft + barWidth + 8), y: barY + 12,
          class: "svg-value", "text-anchor": plotLeft + barWidth > width - 45 ? "end" : "start",
        }, formatValue(value)),
      );
    }
    if (index < usable.length - 1) {
      svg.append(svgElement("line", { x1: 12, y1: y + rowHeight - 3, x2: width - 12, y2: y + rowHeight - 3, class: "svg-grid" }));
    }
  });
}

function renderLineChart(svg, emptyMessage, points) {
  const usable = points.filter((point) => Number.isFinite(point.value));
  svg.replaceChildren();
  const hasData = usable.length > 0;
  svg.hidden = !hasData;
  emptyMessage.hidden = hasData;
  if (!hasData) return;

  const width = 720;
  const height = 300;
  const left = 58;
  const right = 24;
  const top = 24;
  const bottom = 48;
  const plotWidth = width - left - right;
  const plotHeight = height - top - bottom;
  const values = usable.map((point) => point.value);
  const rawMin = Math.min(...values);
  const rawMax = Math.max(...values);
  const padding = rawMax === rawMin ? Math.max(Math.abs(rawMax) * 0.1, 1) : (rawMax - rawMin) * 0.12;
  const minValue = rawMin - padding;
  const maxValue = rawMax + padding;
  const coordinates = usable.map((point, index) => ({
    ...point,
    x: usable.length === 1 ? left + plotWidth / 2 : left + (index / (usable.length - 1)) * plotWidth,
    y: top + ((maxValue - point.value) / (maxValue - minValue)) * plotHeight,
  }));

  svg.setAttribute("viewBox", `0 0 ${width} ${height}`);
  for (let tick = 0; tick <= 4; tick += 1) {
    const y = top + (tick / 4) * plotHeight;
    const value = maxValue - (tick / 4) * (maxValue - minValue);
    svg.append(
      svgElement("line", { x1: left, y1: y, x2: width - right, y2: y, class: "svg-grid" }),
      svgElement("text", { x: left - 9, y: y + 4, "text-anchor": "end", class: "svg-tick" }, formatValue(value)),
    );
  }
  svg.append(svgElement("line", { x1: left, y1: top + plotHeight, x2: width - right, y2: top + plotHeight, class: "svg-axis" }));
  if (coordinates.length > 1) {
    svg.append(svgElement("polyline", {
      points: coordinates.map((point) => `${point.x},${point.y}`).join(" "),
      class: "svg-trend-line",
    }));
  }
  for (const point of coordinates) {
    const marker = svgElement("circle", { cx: point.x, cy: point.y, r: 4.5, class: "svg-trend-point" });
    marker.append(svgElement("title", {}, `${formatValue(point.value)} ms · ${point.label}`));
    svg.append(marker);
  }
  const first = coordinates[0];
  const last = coordinates[coordinates.length - 1];
  svg.append(
    svgElement("text", { x: left, y: height - 14, class: "svg-tick" }, first.label),
    svgElement("text", { x: width - right, y: height - 14, "text-anchor": "end", class: "svg-tick" }, last.label),
  );
}

function updateGraphTrend(name, summary) {
  if (!summary || !Number.isFinite(summary.average)) return;
  const completionAt = name === "tumbling"
    ? graphMonitoring?.consumer?.windows?.tumbling?.last_completed_at : null;
  const signature = JSON.stringify(summary);
  const marker = `${completionAt || ""}:${signature}`;
  if (graphTrendMarkers[name] === marker) return;
  graphTrendMarkers[name] = marker;
  const observedAt = completionAt || new Date().toISOString();
  graphTrendHistory[name].push({ value: summary.average, label: formatTimestamp(observedAt) });
  graphTrendHistory[name] = graphTrendHistory[name].slice(-50);
}

function renderGraphAnalysis() {
  const measurements = graphMeasurements;
  const processingSamples = measurements?.sample_count?.consumer_processing_latency_ms || 0;
  const processingAverage = measurements?.average_latency_ms?.consumer_processing_latency_ms;
  const processingMinimum = measurements?.minimum_latency_ms?.consumer_processing_latency_ms;
  const processingMaximum = measurements?.maximum_latency_ms?.consumer_processing_latency_ms;
  elements.graphProcessingAverage.textContent = formatValue(processingAverage, " ms");
  elements.graphProcessingMinimum.textContent = formatValue(processingMinimum, " ms");
  elements.graphProcessingMaximum.textContent = formatValue(processingMaximum, " ms");
  elements.graphProcessedTotal.textContent = formatValue(measurements?.consumer_unique_successes);
  elements.graphProcessingEmpty.hidden = processingSamples > 0;

  const tumbling = graphMetrics?.tumbling;
  const sliding = graphMetrics?.sliding;
  renderGroupedBars(elements.graphWindowCountChart, elements.graphWindowCountEmpty, [{
    label: "Latest summary", tumbling: tumbling?.count, sliding: sliding?.count,
  }]);
  renderGroupedBars(elements.graphWindowLatencyChart, elements.graphWindowLatencyEmpty, [
    { label: "Average", tumbling: tumbling?.average, sliding: sliding?.average },
    { label: "Minimum", tumbling: tumbling?.minimum, sliding: sliding?.minimum },
    { label: "Maximum", tumbling: tumbling?.maximum, sliding: sliding?.maximum },
  ]);

  elements.graphComparisonBody.replaceChildren();
  for (const [name, summary] of [["Tumbling", tumbling], ["Sliding", sliding]]) {
    const row = document.createElement("tr");
    row.append(
      makeCell(name), makeCell(formatValue(summary?.count)),
      makeCell(formatValue(summary?.average)), makeCell(formatValue(summary?.minimum)),
      makeCell(formatValue(summary?.maximum)),
    );
    elements.graphComparisonBody.append(row);
  }

  updateGraphTrend("tumbling", tumbling);
  updateGraphTrend("sliding", sliding);
  renderLineChart(elements.graphTumblingTrendChart, elements.graphTumblingTrendEmpty, graphTrendHistory.tumbling);
  renderLineChart(elements.graphSlidingTrendChart, elements.graphSlidingTrendEmpty, graphTrendHistory.sliding);

  const hasAnyData = Boolean(measurements?.measurements_present || tumbling || sliding
    || graphTrendHistory.tumbling.length || graphTrendHistory.sliding.length);
  elements.graphDataState.textContent = hasAnyData ? "Live API data loaded" : "No metrics available yet";
  elements.graphUpdated.textContent = graphMetrics !== null || graphMeasurements !== null
    ? `Updated ${new Date().toLocaleTimeString()}` : "Metrics API unavailable.";
}

function renderCurrentSimulation() {
  const data = currentSimulationData;
  const simulationSession = data?.simulation;
  const measurements = data?.measurements;
  const aggregations = data?.aggregations;
  const processed = measurements?.processed || 0;
  const failed = measurements?.processing_failures || 0;
  const modeLabel = MODE_COPY[simulationSession?.flow_mode]?.label || simulationSession?.flow_mode;
  elements.currentExperimentLabel.textContent = simulationSession
    ? `${modeLabel} · ${simulationSession.state} · ${simulationSession.simulation_id}` : "No simulation started";
  elements.currentProcessedCount.textContent = formatValue(processed);
  elements.currentFailedCount.textContent = formatValue(failed);
  elements.currentLatencyAverage.textContent = formatValue(
    measurements?.average_latency_ms?.consumer_processing_latency_ms, " ms");
  elements.currentLatencyMinimum.textContent = formatValue(
    measurements?.minimum_latency_ms?.consumer_processing_latency_ms, " ms");
  elements.currentLatencyMaximum.textContent = formatValue(
    measurements?.maximum_latency_ms?.consumer_processing_latency_ms, " ms");
  const tumbling = aggregations?.tumbling;
  const sliding = aggregations?.sliding;
  const hasEvents = processed > 0 || failed > 0
    || Number(tumbling?.count || 0) > 0 || Number(sliding?.count || 0) > 0;
  elements.currentExperimentEmpty.hidden = hasEvents;
  elements.currentExperimentEmpty.textContent = simulationSession
    ? "No simulation data yet. Processed events and window summaries will appear here as they arrive."
    : "Start a Streaming Lab simulation to see its run-scoped metrics here.";
  const rows = [
    { label: "Latest summary", tumbling: tumbling?.count, sliding: sliding?.count },
  ];
  renderGroupedBars(elements.currentWindowCountChart, elements.currentWindowCountEmpty, rows);
  renderGroupedBars(elements.currentWindowValueChart, elements.currentWindowValueEmpty, [
    { label: "Average", tumbling: tumbling?.average, sliding: sliding?.average },
    { label: "Minimum", tumbling: tumbling?.minimum, sliding: sliding?.minimum },
    { label: "Maximum", tumbling: tumbling?.maximum, sliding: sliding?.maximum },
  ]);
}

function selectGraphView(view) {
  const current = view === "current";
  elements.overallMetricsView.hidden = current;
  elements.currentExperimentView.hidden = !current;
  elements.overallMetricsTab.setAttribute("aria-selected", String(!current));
  elements.currentExperimentTab.setAttribute("aria-selected", String(current));
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
  const shouldReadMonitoring = !monitoring || !windowConfiguration || pollCount % MONITORING_INTERVAL_TICKS === 1;
  const tasks = [
    requestJson("/api/simulations/status"),
    requestJson("/api/simulations/activity"),
  ];
  if (shouldReadMonitoring) {
    tasks.push(requestJson("/api/health"), requestJson("/api/monitoring"), requestJson("/api/config"));
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
      if (results[4]?.status === "fulfilled") {
        windowConfiguration = results[4].value;
        renderWindowConfiguration();
      } else if (results[4]?.status === "rejected" && !windowConfiguration) {
        elements.activeWindowConfig.textContent = "Unavailable";
        elements.windowConfigFeedback.className = "feedback is-error";
        elements.windowConfigFeedback.textContent = `Could not load configuration: ${results[4].reason?.message || "API request failed."}`;
      }
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
  if (document.hidden || !["lab", "graph"].includes(currentPage())) return;
  pollTimer = window.setTimeout(async () => {
    if (currentPage() === "graph") await refreshGraphData();
    else await refreshLiveData();
    scheduleRefresh();
  }, REFRESH_INTERVAL_MS);
}

async function refreshGraphData() {
  if (pollInFlight || currentPage() !== "graph" || document.hidden) return;
  pollInFlight = true;
  try {
    const [metrics, measurements, monitoringResult, healthResult, simulationResult] = await Promise.allSettled([
      requestJson("/api/metrics"),
      requestJson("/api/measurements"),
      requestJson("/api/monitoring"),
      requestJson("/api/health"),
      requestJson("/api/simulations/status"),
    ]);
    graphMetrics = metrics.status === "fulfilled" ? metrics.value : null;
    graphMeasurements = measurements.status === "fulfilled" ? measurements.value : null;
    graphMonitoring = monitoringResult.status === "fulfilled" ? monitoringResult.value : null;
    health = healthResult.status === "fulfilled" ? healthResult.value : null;
    currentSimulationData = simulationResult.status === "fulfilled" ? simulationResult.value : null;
    updateHeaderStatus();
    renderGraphAnalysis();
    renderCurrentSimulation();
  } finally {
    pollInFlight = false;
  }
}

function activatePage() {
  window.clearTimeout(pollTimer);
  const page = currentPage();
  if (page === "lab") {
    renderMode(selectedMode);
    updateSimulationControls();
    void refreshLiveData().finally(scheduleRefresh);
  } else if (page === "graph") {
    renderGraphAnalysis();
    void refreshGraphData().finally(scheduleRefresh);
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

document.addEventListener("click", (event) => {
  const link = event.target.closest('a[href="/"] , a[href="/failure-recovery"], a[href="/#graph-analysis"]');
  if (!link || event.defaultPrevented || event.button !== 0 || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;
  event.preventDefault();
  const target = new URL(link.href, window.location.href);
  if (target.pathname === "/failure-recovery") {
    history.pushState({}, "", "/failure-recovery");
  } else if (target.hash === "#graph-analysis") {
    history.pushState({}, "", "/#graph-analysis");
  } else {
    history.pushState({}, "", "/");
  }
  activatePage();
});

window.addEventListener("popstate", activatePage);

elements.overallMetricsTab.addEventListener("click", () => selectGraphView("overall"));
elements.currentExperimentTab.addEventListener("click", () => selectGraphView("current"));

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

elements.configSlidingSize.addEventListener("change", renderWindowConfiguration);
elements.windowConfigForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  const feedback = elements.windowConfigFeedback;
  feedback.className = "feedback";
  feedback.textContent = "Applying configuration…";
  try {
    const response = await requestJson("/api/config", {
      method: "PUT",
      body: JSON.stringify({
        tumbling_size: Number(elements.configTumblingSize.value),
        sliding_size: Number(elements.configSlidingSize.value),
        sliding_interval: Number(elements.configSlidingInterval.value),
      }),
    });
    windowConfiguration = response;
    renderWindowConfiguration();
    feedback.className = "feedback is-success";
    feedback.textContent = response.message || "Configuration saved and pending activation.";
  } catch (error) {
    feedback.className = "feedback is-error";
    feedback.textContent = `Could not apply configuration: ${error.message}`;
  }
});

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
  else if (["lab", "graph"].includes(currentPage())) {
    if (currentPage() === "graph") void refreshGraphData();
    else void refreshLiveData();
    scheduleRefresh();
  }
});

window.addEventListener("pagehide", () => window.clearTimeout(pollTimer));

activatePage();
