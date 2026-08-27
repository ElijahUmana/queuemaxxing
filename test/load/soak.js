import http from "k6/http";
import { check } from "k6";
import exec from "k6/execution";
import { Counter, Trend } from "k6/metrics";

const baseURL = (__ENV.QMAX_TEST_URL || "http://127.0.0.1:8080").replace(/\/$/, "");
const namespace = __ENV.QMAX_LOAD_NAMESPACE || `load-${Date.now()}`;
const queueCount = Number(__ENV.QMAX_QUEUE_COUNT || 100);
const duration = __ENV.QMAX_DURATION || "30m";
const producerRate = Number(__ENV.QMAX_PRODUCER_RATE || 20);
const consumerRate = Number(__ENV.QMAX_CONSUMER_RATE || 20);
const durableMutation = new Trend("durable_mutation_latency", true);
const dequeueLatency = new Trend("dequeue_latency", true);
const unexpectedStatuses = new Counter("unexpected_statuses");

export const options = {
  scenarios: {
    producers: {
      executor: "constant-arrival-rate",
      exec: "produce",
      rate: producerRate,
      timeUnit: "1s",
      duration,
      preAllocatedVUs: Number(__ENV.QMAX_PRODUCER_VUS || 32),
      maxVUs: Number(__ENV.QMAX_PRODUCER_MAX_VUS || 128),
    },
    consumers: {
      executor: "constant-arrival-rate",
      exec: "consume",
      rate: consumerRate,
      timeUnit: "1s",
      duration,
      preAllocatedVUs: Number(__ENV.QMAX_CONSUMER_VUS || 32),
      maxVUs: Number(__ENV.QMAX_CONSUMER_MAX_VUS || 128),
    },
  },
  thresholds: {
    checks: ["rate==1"],
    dropped_iterations: ["count==0"],
    unexpected_statuses: ["count==0"],
    durable_mutation_latency: ["p(99)<100"],
    dequeue_latency: ["p(99)<50"],
    http_req_failed: ["rate==0"],
  },
  discardResponseBodies: false,
};

export function setup() {
  const queues = [];
  for (let index = 0; index < queueCount; index += 1) {
    const name = `${namespace}-${index}`;
    const priorityEnabled = Math.floor(index / 2) % 2 === 1;
    const response = jsonRequest("POST", `${baseURL}/v1/queues`, {
      name,
      ordering: index % 2 === 0 ? "fifo" : "lifo",
      priority_enabled: priorityEnabled,
      default_delay_ms: 0,
      default_visibility_timeout_ms: 5000,
      max_deliveries: 5,
    }, `setup-${name}`);
    expect(response, [200, 201], "create queue");
    queues.push({ name, priorityEnabled });
  }
  return { queues };
}

export function produce(data) {
  const iteration = exec.scenario.iterationInTest;
  const queue = data.queues[iteration % data.queues.length];
  const messageKey = `${namespace}-enqueue-${iteration}`;
  const body = {
    payload: {
      iteration,
      queue: queue.name,
      bytes: "x".repeat(iteration % 97 === 0 ? 262144 : 1024),
    },
    delay_ms: iteration % 20 === 0 ? 250 : 0,
  };
  if (queue.priorityEnabled) body.priority = (iteration % 21) - 10;

  const started = Date.now();
  const response = jsonRequest(
    "POST",
    `${baseURL}/v1/queues/${encodeURIComponent(queue.name)}/messages`,
    body,
    messageKey,
  );
  durableMutation.add(Date.now() - started);
  expect(response, [200, 201], "enqueue");
}

export function consume(data) {
  const iteration = exec.scenario.iterationInTest;
  const queue = data.queues[iteration % data.queues.length];
  const started = Date.now();
  const response = jsonRequest("POST", `${baseURL}/v1/queues/${encodeURIComponent(queue.name)}/messages:receive`, {
    visibility_timeout_ms: 5000,
    wait_timeout_ms: 0,
  });
  dequeueLatency.add(Date.now() - started);
  expect(response, [200], "receive");
  if (response.status !== 200) return;

  const messages = response.json("messages") || [];
  if (messages.length === 0) return;

  const delivery = messages[0];
  const action = iteration % 10 === 0 ? "nack" : "ack";
  const body = action === "ack"
    ? { receipt_handle: delivery.receipt_handle }
    : {
        receipt_handle: delivery.receipt_handle,
        retry_delay_ms: iteration % 3 === 0 ? 100 : 0,
        reason: "load-injected",
      };
  const mutationStarted = Date.now();
  const transition = jsonRequest(
    "POST",
    `${baseURL}/v1/queues/${encodeURIComponent(queue.name)}/messages/${encodeURIComponent(delivery.message.id)}/${action}`,
    body,
    `${namespace}-${action}-${delivery.message.id}-${delivery.delivery_count}`,
  );
  durableMutation.add(Date.now() - mutationStarted);
  expect(transition, [200], action);
}

function jsonRequest(method, url, body, idempotencyKey) {
  const headers = { Accept: "application/json", "Content-Type": "application/json" };
  if (idempotencyKey) headers["Idempotency-Key"] = idempotencyKey;
  return http.request(method, url, JSON.stringify(body), { headers, tags: { name: routeName(url) } });
}

function expect(response, allowed, operation) {
  const passed = check(response, { [`${operation} status`]: (result) => allowed.includes(result.status) });
  if (!passed) unexpectedStatuses.add(1, { operation, status: String(response.status) });
}

function routeName(url) {
  return url.replace(baseURL, "").replace(/[0-9a-f]{8}-[0-9a-f-]{27,}/gi, ":id");
}
