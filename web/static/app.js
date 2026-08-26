import { APIError, QueueAPI } from "./api.js";

const ui = bindUI();
const state = {
  queues: [],
  activeQueue: localStorage.getItem("qmax.activeQueue") || "",
  queueInfo: null,
  stats: null,
  messages: [],
  messageView: "live",
  nextCursor: "",
  snapshotLSN: "",
  activeDelivery: null,
  refreshGeneration: 0,
  refreshController: null,
  polling: null,
  countdown: null,
  workers: null,
  activity: [],
  serverClockOffsetMS: 0,
};

const api = new QueueAPI(recordActivity);
start();

function bindUI() {
  const byId = (id) => document.getElementById(id);
  return {
    connection: byId("connection-status"), durableLSN: byId("durable-lsn"),
    queueFilter: byId("queue-filter"), queueList: byId("queue-list"),
    emptyState: byId("empty-state"), queueWorkspace: byId("queue-workspace"),
    queueTitle: byId("queue-title"), queuePolicy: byId("queue-policy"), lastRefresh: byId("last-refresh"),
    counts: { ready: byId("count-ready"), delayed: byId("count-delayed"), in_flight: byId("count-flight"), dead: byId("count-dead"), total: byId("count-total") },
    createDialog: byId("create-queue-dialog"), createForm: byId("create-queue-form"),
    enqueueForm: byId("enqueue-form"), enqueuePayload: byId("enqueue-payload"), payloadValidation: byId("payload-validation"),
    receiveForm: byId("receive-form"), deliveryState: byId("delivery-state"), deliveryEmpty: byId("delivery-empty"), deliveryCard: byId("delivery-card"),
    deliveryMessageID: byId("delivery-message-id"), deliveryAttempt: byId("delivery-attempt"), deliveryPayload: byId("delivery-payload"), leaseCountdown: byId("lease-countdown"),
    messageTable: byId("message-table"), messagesEmpty: byId("messages-empty"), snapshotLSN: byId("snapshot-lsn"), loadMore: byId("load-more"),
    workerForm: byId("worker-form"), toggleWorkers: byId("toggle-workers"), workerStats: { claimed: byId("workers-claimed"), acked: byId("workers-acked"), failed: byId("workers-failed") },
    activityLog: byId("activity-log"), toastRegion: byId("toast-region"),
  };
}

function start() {
  bindEvents();
  renderActivity();
  refreshOverview();
  state.polling = window.setInterval(() => refreshOverview({ quiet: true }), 2500);
  document.addEventListener("visibilitychange", () => {
    if (!document.hidden) refreshOverview({ quiet: true });
  });
}

function bindEvents() {
  document.querySelectorAll("[data-open-create], #open-create-queue").forEach((button) => button.addEventListener("click", openCreateDialog));
  document.querySelectorAll("[data-close-dialog]").forEach((button) => button.addEventListener("click", () => ui.createDialog.close()));
  ui.createDialog.addEventListener("click", (event) => { if (event.target === ui.createDialog) ui.createDialog.close(); });
  ui.createDialog.addEventListener("keydown", trapDialogFocus);
  ui.createForm.addEventListener("submit", createQueue);
  ui.queueFilter.addEventListener("input", renderQueueList);
  document.getElementById("refresh-all").addEventListener("click", () => refreshOverview());
  ui.enqueueForm.addEventListener("submit", enqueueMessage);
  ui.enqueuePayload.addEventListener("input", validatePayload);
  ui.enqueuePayload.addEventListener("keydown", (event) => {
    if ((event.metaKey || event.ctrlKey) && event.key === "Enter") ui.enqueueForm.requestSubmit();
  });
  ui.receiveForm.addEventListener("submit", receiveMessage);
  document.getElementById("ack-delivery").addEventListener("click", ackDelivery);
  document.getElementById("nack-delivery").addEventListener("click", nackDelivery);
  document.getElementById("extend-delivery").addEventListener("click", extendDelivery);
  document.querySelectorAll("[data-message-view]").forEach((button) => button.addEventListener("click", () => selectMessageView(button)));
  ui.loadMore.addEventListener("click", () => refreshMessages({ append: true }));
  ui.workerForm.addEventListener("submit", toggleWorkers);
  document.getElementById("clear-activity").addEventListener("click", () => { state.activity = []; renderActivity(); });
}

async function refreshOverview(options = {}) {
  const generation = ++state.refreshGeneration;
  state.refreshController?.abort();
  const controller = new AbortController();
  state.refreshController = controller;

  try {
    const [queuesResult, statsResult] = await Promise.allSettled([
      api.listQueues({ signal: controller.signal }),
      api.stats({ signal: controller.signal }),
    ]);
    if (!isCurrentRefresh(generation, controller)) return;
    if (queuesResult.status === "rejected") throw queuesResult.reason;

    state.queues = normalizeQueues(queuesResult.value);
    if (statsResult.status === "fulfilled") state.stats = statsResult.value;
    if (state.activeQueue && !state.queues.some((queue) => queueName(queue) === state.activeQueue)) state.activeQueue = "";
    if (!state.activeQueue && state.queues.length === 1) state.activeQueue = queueName(state.queues[0]);

    renderQueueList();
    renderStats();
    setConnection(true);

    if (state.activeQueue) {
      const selectedQueue = state.activeQueue;
      const [queueResult, messageResult] = await Promise.allSettled([
        api.getQueue(selectedQueue, { signal: controller.signal }),
        listCurrentView(selectedQueue, {}, controller.signal),
      ]);
      if (!isCurrentRefresh(generation, controller) || state.activeQueue !== selectedQueue) return;
      if (queueResult.status === "rejected") throw queueResult.reason;
      state.queueInfo = normalizeQueueInfo(queueResult.value);
      if (messageResult.status === "fulfilled") applyMessagePage(messageResult.value, false);
      else if (!options.quiet) showError(messageResult.reason);
      renderWorkspace();
    } else {
      state.queueInfo = null;
      renderWorkspace();
    }
    ui.lastRefresh.textContent = `Updated ${formatTime(new Date())}`;
  } catch (error) {
    if (error.name === "AbortError") return;
    setConnection(false);
    if (!options.quiet) showError(error);
  }
}

function isCurrentRefresh(generation, controller) {
  return generation === state.refreshGeneration && !controller.signal.aborted;
}

async function refreshMessages({ append = false } = {}) {
  if (!state.activeQueue) return;
  const queue = state.activeQueue;
  const view = state.messageView;
  const cursor = append ? state.nextCursor : "";
  const generation = ++state.refreshGeneration;
  state.refreshController?.abort();
  const controller = new AbortController();
  state.refreshController = controller;
  ui.loadMore.disabled = true;
  try {
    const page = await listCurrentView(queue, cursor ? { cursor } : {}, controller.signal);
    if (!isCurrentRefresh(generation, controller) || state.activeQueue !== queue || state.messageView !== view) return;
    applyMessagePage(page, append);
    renderMessages();
  } catch (error) {
    if (error.name !== "AbortError") showError(error);
  } finally {
    ui.loadMore.disabled = false;
  }
}

function listCurrentView(queue, query, signal) {
  const options = { signal };
  const pageQuery = { limit: 50, ...query };
  return state.messageView === "dead" ? api.listDeadLetters(queue, pageQuery, options) : api.listMessages(queue, pageQuery, options);
}

function applyMessagePage(rawPage, append) {
  const page = rawPage || {};
  const messages = Array.isArray(page) ? page : page.messages || page.items || [];
  state.messages = append ? state.messages.concat(messages) : messages;
  state.nextCursor = page.next_cursor || page.nextCursor || "";
  state.snapshotLSN = page.snapshot_lsn || page.snapshotLSN || "";
}

function normalizeQueues(value) {
  if (Array.isArray(value)) return value.map(normalizeQueueInfo);
  const queues = value?.queues || value?.items || [];
  return Array.isArray(queues) ? queues.map(normalizeQueueInfo) : [];
}

function normalizeQueueInfo(value) {
  if (!value) return null;
  if (value.config) return value;
  return { config: value, counts: value.counts || {} };
}

function queueName(queue) {
  return queue?.config?.name || queue?.name || "";
}

function renderQueueList() {
  const filter = ui.queueFilter.value.trim().toLocaleLowerCase();
  const queues = state.queues.filter((queue) => queueName(queue).toLocaleLowerCase().includes(filter));
  ui.queueList.replaceChildren();
  if (!queues.length) {
    ui.queueList.append(element("p", "queue-list-empty", state.queues.length ? "No queues match this filter." : "No queues yet."));
    return;
  }
  for (const queue of queues) {
    const name = queueName(queue);
    const button = element("button", `queue-item${name === state.activeQueue ? " is-active" : ""}`);
    button.type = "button";
    button.setAttribute("aria-current", name === state.activeQueue ? "page" : "false");
    const copy = document.createElement("span");
    copy.append(element("strong", "", name));
    const config = queue.config || queue;
    copy.append(element("small", "", `${String(config.ordering || "fifo").toUpperCase()} · ${config.priority_enabled ? "priority" : "sequence"}`));
    const ready = queue.counts?.ready ?? 0;
    const count = element("output", "", String(ready));
    count.title = `${ready} ready`;
    button.append(copy, count);
    button.addEventListener("click", () => selectQueue(name));
    ui.queueList.append(button);
  }
}

function selectQueue(name) {
  if (state.activeQueue === name) return;
  state.activeQueue = name;
  localStorage.setItem("qmax.activeQueue", name);
  state.queueInfo = null;
  state.messages = [];
  clearActiveDelivery();
  renderQueueList();
  renderWorkspace();
  refreshOverview();
}

function renderWorkspace() {
  const hasQueue = Boolean(state.activeQueue);
  ui.emptyState.hidden = hasQueue;
  ui.queueWorkspace.hidden = !hasQueue;
  if (!hasQueue) return;

  const info = state.queueInfo || state.queues.find((queue) => queueName(queue) === state.activeQueue);
  const config = info?.config || info || {};
  ui.queueTitle.textContent = state.activeQueue;
  ui.queuePolicy.replaceChildren(
    policyChip("ORDER", String(config.ordering || "—").toUpperCase()),
    policyChip("PRIORITY", config.priority_enabled ? "ON" : "OFF"),
    policyChip("DELAY", formatMilliseconds(config.default_delay_ms)),
    policyChip("LEASE", formatMilliseconds(config.default_visibility_timeout_ms)),
    policyChip("MAX", config.max_deliveries ?? "—"),
  );
  renderCounts(info?.counts || {});
  renderMessages();
}

function policyChip(label, value) {
  const chip = element("span", "policy-chip");
  chip.append(document.createTextNode(`${label} `), element("strong", "", String(value)));
  return chip;
}

function renderCounts(counts) {
  for (const [name, output] of Object.entries(ui.counts)) output.textContent = formatInteger(counts?.[name] ?? 0);
}

function renderStats() {
  if (!state.stats) return;
  ui.durableLSN.textContent = state.stats.durable_lsn ?? state.stats.durableLSN ?? "—";
  if (!state.queueInfo && state.activeQueue) {
    const selected = state.queues.find((queue) => queueName(queue) === state.activeQueue);
    renderCounts(selected?.counts || {});
  }
}

function renderMessages() {
  ui.messageTable.replaceChildren();
  ui.messagesEmpty.hidden = state.messages.length > 0;
  for (const message of state.messages) ui.messageTable.append(messageRow(message));
  ui.loadMore.hidden = !state.nextCursor;
  ui.snapshotLSN.textContent = state.snapshotLSN ? `Snapshot LSN ${state.snapshotLSN}` : "Snapshot LSN —";
}

function messageRow(message) {
  const row = document.createElement("tr");
  row.append(
    cellWith(element("code", "", message.id || "—")),
    cellWith(messageState(message.state)),
    textCell(String(message.priority ?? 0)),
    textCell(String(message.delivery_count ?? 0)),
    textCell(formatDateTime(message.available_at)),
    cellWith(element("div", "payload-cell", compactPayload(message.payload))),
  );
  const actionCell = document.createElement("td");
  if (state.messageView === "dead") {
    const button = element("button", "button button-quiet", "Redrive");
    button.type = "button";
    button.addEventListener("click", () => redriveMessage(message));
    actionCell.append(button);
  }
  row.append(actionCell);
  return row;
}

function messageState(value) {
  const badge = element("span", "message-state", value || "unknown");
  badge.dataset.state = value || "unknown";
  return badge;
}
function cellWith(child) { const cell = document.createElement("td"); cell.append(child); return cell; }
function textCell(value) { const cell = document.createElement("td"); cell.textContent = value; return cell; }

function trapDialogFocus(event) {
  if (event.key !== "Tab") return;
  const focusable = [...ui.createDialog.querySelectorAll("button:not(:disabled), input:not(:disabled), select:not(:disabled), textarea:not(:disabled), [tabindex]:not([tabindex='-1'])")]
    .filter((node) => node.getClientRects().length > 0);
  if (!focusable.length) return;
  const first = focusable[0];
  const last = focusable[focusable.length - 1];
  if (event.shiftKey && document.activeElement === first) {
    event.preventDefault();
    last.focus();
  } else if (!event.shiftKey && document.activeElement === last) {
    event.preventDefault();
    first.focus();
  }
}

function openCreateDialog() {
  ui.createForm.reset();
  document.getElementById("queue-delay").value = "0";
  document.getElementById("queue-visibility").value = "5000";
  document.getElementById("queue-max-deliveries").value = "3";
  ui.createDialog.showModal();
  document.getElementById("queue-name").focus();
}

async function createQueue(event) {
  event.preventDefault();
  if (!ui.createForm.reportValidity()) return;
  const submit = ui.createForm.querySelector("button[type=submit]");
  setBusy(submit, true, "Creating…");
  const config = {
    name: document.getElementById("queue-name").value.trim(),
    ordering: new FormData(ui.createForm).get("ordering"),
    priority_enabled: document.getElementById("queue-priority").checked,
    default_delay_ms: millisecondsFromInput("queue-delay"),
    default_visibility_timeout_ms: millisecondsFromInput("queue-visibility"),
    max_deliveries: integerFromInput("queue-max-deliveries"),
  };
  try {
    const result = await api.createQueue(config, uniqueKey("create"));
    const createdQueue = normalizeQueueInfo(result?.data || result);
    state.activeQueue = queueName(createdQueue) || config.name;
    localStorage.setItem("qmax.activeQueue", state.activeQueue);
    ui.createDialog.close();
    toast(`Queue ${state.activeQueue} created.`);
    await refreshOverview();
  } catch (error) {
    showError(error);
  } finally {
    setBusy(submit, false);
  }
}

function validatePayload() {
  try {
    JSON.parse(ui.enqueuePayload.value);
    ui.payloadValidation.textContent = "Valid JSON · rendered as inert text";
    ui.payloadValidation.style.color = "var(--green)";
    return true;
  } catch (error) {
    ui.payloadValidation.textContent = error.message;
    ui.payloadValidation.style.color = "var(--red)";
    return false;
  }
}

async function enqueueMessage(event) {
  event.preventDefault();
  if (!state.activeQueue || !validatePayload()) return;
  const submit = ui.enqueueForm.querySelector("button[type=submit]");
  setBusy(submit, true, "Enqueuing…");
  const idempotencyKey = document.getElementById("enqueue-idempotency").value.trim() || uniqueKey("enqueue");
  const request = {
    payload: JSON.parse(ui.enqueuePayload.value),
    priority: integerFromInput("enqueue-priority"),
    delay_ms: millisecondsFromInput("enqueue-delay"),
  };
  try {
    const result = await api.enqueue(state.activeQueue, request, idempotencyKey);
    const message = result?.data || result;
    toast(`Message ${shortID(message?.id)} accepted${result?.replayed ? " from idempotency history" : ""}.`);
    await refreshOverview({ quiet: true });
  } catch (error) {
    showError(error);
  } finally {
    setBusy(submit, false);
  }
}

async function receiveMessage(event) {
  event.preventDefault();
  if (!state.activeQueue || state.activeDelivery) return;
  const submit = ui.receiveForm.querySelector("button[type=submit]");
  setBusy(submit, true, "Waiting…");
  try {
    const result = await api.receive(state.activeQueue, {
      visibility_timeout_ms: millisecondsFromInput("receive-visibility"),
      wait_timeout_ms: millisecondsFromInput("receive-wait"),
    });
    const delivery = result?.messages?.[0];
    calibrateServerClock(result?.polled_at);
    if (!delivery?.message) {
      toast("No eligible message was available.");
      return;
    }
    state.activeDelivery = delivery;
    renderDelivery();
    await refreshOverview({ quiet: true });
  } catch (error) {
    showError(error);
  } finally {
    setBusy(submit, false);
  }
}

function renderDelivery() {
  const delivery = state.activeDelivery;
  ui.deliveryEmpty.hidden = Boolean(delivery);
  ui.deliveryCard.hidden = !delivery;
  ui.deliveryState.textContent = delivery ? "LEASED" : "IDLE";
  ui.deliveryState.classList.toggle("is-leased", Boolean(delivery));
  window.clearInterval(state.countdown);
  if (!delivery) return;
  ui.deliveryMessageID.textContent = delivery.message.id || "—";
  ui.deliveryAttempt.textContent = String(delivery.delivery_count ?? delivery.message.delivery_count ?? "—");
  ui.deliveryPayload.textContent = prettyPayload(delivery.message.payload);
  updateLeaseCountdown();
  state.countdown = window.setInterval(updateLeaseCountdown, 100);
}

function updateLeaseCountdown() {
  if (!state.activeDelivery) return;
  const deadline = new Date(state.activeDelivery.lease_expires_at).getTime();
  const remaining = deadline - (Date.now() + state.serverClockOffsetMS);
  if (!Number.isFinite(remaining)) {
    ui.leaseCountdown.textContent = "unknown";
    return;
  }
  ui.leaseCountdown.textContent = remaining > 0 ? `${(remaining / 1000).toFixed(1)}s` : "expired";
  ui.leaseCountdown.style.color = remaining <= 1000 ? "var(--red)" : "var(--text)";
}

async function ackDelivery() {
  await transitionDelivery("Acknowledge", async (delivery) => api.ack(
    state.activeQueue, delivery.message.id, delivery.receipt_handle, uniqueKey("ack"),
  ));
}

async function nackDelivery() {
  await transitionDelivery("Nack", async (delivery) => api.nack(
    state.activeQueue, delivery.message.id, delivery.receipt_handle,
    { retry_delay_ms: millisecondsFromInput("nack-delay"), reason: "nacked from queue workbench" }, uniqueKey("nack"),
  ));
}

async function extendDelivery() {
  const delivery = state.activeDelivery;
  if (!delivery) return;
  const button = document.getElementById("extend-delivery");
  setBusy(button, true, "Extending…");
  try {
    const result = await api.extend(
      state.activeQueue, delivery.message.id, delivery.receipt_handle,
      millisecondsFromInput("receive-visibility"), uniqueKey("extend"),
    );
    state.activeDelivery = result?.data || result;
    renderDelivery();
    toast("Lease deadline replaced by the server.");
    await refreshOverview({ quiet: true });
  } catch (error) {
    showError(error);
  } finally {
    setBusy(button, false);
  }
}

async function transitionDelivery(label, mutation) {
  const delivery = state.activeDelivery;
  if (!delivery) return;
  const buttons = ui.deliveryCard.querySelectorAll("button");
  buttons.forEach((button) => { button.disabled = true; });
  try {
    await mutation(delivery);
    clearActiveDelivery();
    toast(`${label} committed.`);
    await refreshOverview({ quiet: true });
  } catch (error) {
    showError(error);
  } finally {
    buttons.forEach((button) => { button.disabled = false; });
  }
}

function clearActiveDelivery() {
  state.activeDelivery = null;
  window.clearInterval(state.countdown);
  renderDelivery();
}

function selectMessageView(button) {
  state.messageView = button.dataset.messageView;
  for (const tab of document.querySelectorAll("[data-message-view]")) {
    const selected = tab === button;
    tab.classList.toggle("is-active", selected);
    tab.setAttribute("aria-selected", String(selected));
  }
  state.messages = [];
  state.nextCursor = "";
  renderMessages();
  refreshMessages();
}

async function redriveMessage(message) {
  const idempotencyKey = uniqueKey("redrive");
  try {
    const result = await api.redrive(state.activeQueue, message.id, { delay_ms: 0 }, idempotencyKey);
    const child = result?.data?.child || result?.child || result;
    toast(`Redrive created ${shortID(child?.id)}; the source remains auditable.`);
    await refreshOverview({ quiet: true });
  } catch (error) {
    showError(error);
  }
}

async function toggleWorkers(event) {
  event.preventDefault();
  if (state.workers) {
    stopWorkers();
    return;
  }
  if (!state.activeQueue) return;
  const run = {
    queue: state.activeQueue,
    controller: new AbortController(),
    count: integerFromInput("worker-count"),
    limit: integerFromInput("worker-limit"),
    behavior: document.getElementById("worker-behavior").value,
    stats: { claimed: 0, acked: 0, failed: 0 },
  };
  state.workers = run;
  ui.toggleWorkers.textContent = "Stop workers";
  ui.toggleWorkers.classList.add("button-danger");
  renderWorkerStats();
  const workers = Array.from({ length: run.count }, (_, index) => runWorker(run, index));
  await Promise.allSettled(workers);
  if (state.workers === run) stopWorkers(false);
  await refreshOverview({ quiet: true });
}

async function runWorker(run, workerIndex) {
  for (let iteration = 0; iteration < run.limit && !run.controller.signal.aborted; iteration += 1) {
    try {
      const result = await api.receive(run.queue, { visibility_timeout_ms: 1200, wait_timeout_ms: 500 }, { signal: run.controller.signal });
      const delivery = result?.messages?.[0];
      calibrateServerClock(result?.polled_at);
      if (!delivery?.message) continue;
      run.stats.claimed += 1;
      renderWorkerStats();
      const behavior = workerBehavior(run.behavior, workerIndex, iteration);
      if (behavior === "ack") {
        await api.ack(run.queue, delivery.message.id, delivery.receipt_handle, uniqueKey(`worker-${workerIndex}-ack`), { signal: run.controller.signal });
        run.stats.acked += 1;
      } else if (behavior === "nack") {
        await api.nack(run.queue, delivery.message.id, delivery.receipt_handle, { retry_delay_ms: 150, reason: `worker ${workerIndex} deterministic nack` }, uniqueKey(`worker-${workerIndex}-nack`), { signal: run.controller.signal });
        run.stats.failed += 1;
      } else {
        await abortableDelay(Math.max(0, new Date(delivery.lease_expires_at).getTime() - (Date.now() + state.serverClockOffsetMS) + 40), run.controller.signal);
        run.stats.failed += 1;
      }
      renderWorkerStats();
    } catch (error) {
      if (error.name === "AbortError") return;
      if (error instanceof APIError && [404, 408].includes(error.status)) continue;
      run.stats.failed += 1;
      renderWorkerStats();
      recordActivity({ method: "WORKER", path: `worker-${workerIndex + 1}`, status: error.status || 0, duration: 0, error: error.message });
      await abortableDelay(150, run.controller.signal).catch(() => {});
    }
  }
}

function workerBehavior(configured, worker, iteration) {
  if (configured !== "mixed") return configured;
  return ["ack", "nack", "expire"][(worker + iteration) % 3];
}

function stopWorkers(cancel = true) {
  if (!state.workers) return;
  if (cancel) state.workers.controller.abort();
  state.workers = null;
  ui.toggleWorkers.textContent = "Start workers";
  ui.toggleWorkers.classList.remove("button-danger");
}

function renderWorkerStats() {
  const stats = state.workers?.stats || { claimed: 0, acked: 0, failed: 0 };
  for (const [name, output] of Object.entries(ui.workerStats)) output.textContent = String(stats[name]);
}

function recordActivity(entry) {
  state.activity.unshift({ ...entry, at: new Date() });
  if (state.activity.length > 100) state.activity.length = 100;
  renderActivity();
}

function renderActivity() {
  ui.activityLog.replaceChildren();
  if (!state.activity.length) {
    const item = element("li", "activity-empty", "Public API requests will appear here.");
    ui.activityLog.append(item);
    return;
  }
  for (const entry of state.activity) {
    const item = element("li", "activity-item");
    item.append(element("span", "activity-method", entry.method));
    const copy = element("div", "activity-copy");
    copy.append(element("code", "", redactPath(entry.path)), element("small", "", `${formatTime(entry.at)} · ${Math.round(entry.duration)}ms${entry.requestId ? ` · request ${shortID(entry.requestId)}` : ""}`));
    const status = element("span", `activity-status${entry.status >= 400 || entry.status === 0 ? " is-error" : ""}`, entry.status ? String(entry.status) : "ERR");
    item.append(copy, status);
    ui.activityLog.append(item);
  }
}

function redactPath(path) {
  return String(path).replace(/([?&](?:receipt|idempotency_key)=)[^&]*/gi, "$1[redacted]");
}

function setConnection(online) {
  ui.connection.classList.toggle("is-online", online);
  ui.connection.classList.toggle("is-offline", !online);
  ui.connection.classList.remove("is-connecting");
  ui.connection.lastElementChild.textContent = online ? "API online" : "API unavailable";
}

function setBusy(button, busy, label) {
  if (busy) {
    button.dataset.label = button.textContent;
    button.textContent = label;
  } else if (button.dataset.label) {
    button.textContent = button.dataset.label;
    delete button.dataset.label;
  }
  button.disabled = busy;
}

function showError(error) {
  if (error.name === "AbortError") return;
  const code = error instanceof APIError ? `${error.code}: ` : "";
  toast(`${code}${error.message || "Request failed."}`, true);
}

function toast(message, isError = false) {
  const node = element("div", `toast${isError ? " is-error" : ""}`, message);
  ui.toastRegion.append(node);
  window.setTimeout(() => node.remove(), 5000);
}

function element(tag, className = "", text = "") {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text !== "") node.textContent = text;
  return node;
}

function millisecondsFromInput(id) {
  const value = Math.max(0, Number(document.getElementById(id).value));
  return Number.isFinite(value) ? Math.trunc(value) : 0;
}
function integerFromInput(id) {
  const value = Number(document.getElementById(id).value);
  return Number.isFinite(value) ? Math.trunc(value) : 0;
}
function uniqueKey(scope) {
  return `${scope}-${crypto.randomUUID()}`;
}
function shortID(value) {
  if (!value) return "—";
  const text = String(value);
  return text.length > 12 ? `${text.slice(0, 8)}…` : text;
}
function compactPayload(payload) {
  const text = typeof payload === "string" ? payload : JSON.stringify(payload);
  return text.length > 90 ? `${text.slice(0, 87)}…` : text;
}
function prettyPayload(payload) {
  if (typeof payload === "string") {
    try { return JSON.stringify(JSON.parse(payload), null, 2); } catch { return payload; }
  }
  return JSON.stringify(payload, null, 2);
}
function formatMilliseconds(value) {
  if (value === undefined || value === null || value === "") return "—";
  const milliseconds = Number(value);
  if (!Number.isFinite(milliseconds)) return String(value);
  if (milliseconds >= 1000 && milliseconds % 1000 === 0) return `${milliseconds / 1000}s`;
  return `${milliseconds}ms`;
}
function formatInteger(value) {
  const number = Number(value);
  return Number.isFinite(number) ? new Intl.NumberFormat().format(number) : String(value);
}
function formatDateTime(value) {
  if (!value) return "—";
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? String(value) : date.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
}
function formatTime(date) {
  return date.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
}
function calibrateServerClock(serverTime) {
  if (!serverTime) return;
  const parsed = new Date(serverTime).getTime();
  if (Number.isFinite(parsed)) state.serverClockOffsetMS = parsed - Date.now();
}

function abortableDelay(milliseconds, signal) {
  return new Promise((resolve, reject) => {
    if (signal.aborted) { reject(new DOMException("Aborted", "AbortError")); return; }
    const timer = window.setTimeout(resolve, milliseconds);
    signal.addEventListener("abort", () => { window.clearTimeout(timer); reject(new DOMException("Aborted", "AbortError")); }, { once: true });
  });
}
