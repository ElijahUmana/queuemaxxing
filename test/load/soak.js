import http from "k6/http";
import { check, sleep } from "k6";
import exec from "k6/execution";
import { Counter, Trend } from "k6/metrics";

const baseURL = (__ENV.QMAX_TEST_URL || "http://127.0.0.1:8080").replace(/\/$/, "");
const namespace = __ENV.QMAX_LOAD_NAMESPACE || `load-${Date.now()}`;
const queueCount = Number(__ENV.QMAX_QUEUE_COUNT || 100);
const durableMutation = new Trend("durable_mutation_latency", true);
const dequeueLatency = new Trend("dequeue_latency", true);
const unexpectedStatuses = new Counter("unexpected_statuses");

export const options = {
  scenarios: {
    producers: {
      executor: "constant-vus",
      exec: "produce",
      vus: Number(__ENV.QMAX_PRODUCERS || 64),
      duration: __ENV.QMAX_DURATION || "30m",
    },
    consumers: {
      executor: "constant-vus",
      exec: "consume",
      vus: Number(__ENV.QMAX_CONSUMERS || 32),
      duration: __ENV.QMAX_DURATION || "30m",
    },
  },
  thresholds: {
    checks: ["rate==1"],
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
    const response = jsonRequest("POST", `${baseURL}/v1/queues`, {
      name,
      ordering: index % 2 === 0 ? "fifo" : "lifo",
      priority_enabled: index % 3 !== 0,
      default_delay_ms: 0,
      default_visibility_timeout_ms: 5000,
      max_deliveries: 5,
    }, `setup-${name}`);
    expect(response, [200, 201], "create queue");
    queues.push(name);
  }
  return { queues };
}

export function produce(data) {
  const iteration = exec.scenario.iterationInTest;
  const queue = data.queues[iteration % 10 === 0 ? iteration % data.queues.length : 0];
  const messageKey = `${exec.vu.idInTest}-${iteration}`;
  const delay = iteration % 20 === 0 ? 100 : 0;
  const started = Date.now();
  const response = jsonRequest("POST", `${baseURL}/v1/queues/${encodeURIComponent(queue)}/messages`, {
    payload: { producer: exec.vu.idInTest, iteration, bytes: "x".repeat(iteration % 17 === 0 ? 262144 : 1024) },
    priority: (iteration % 21) - 10,
    delay_ms: delay,
  }, `enqueue-${messageKey}`);
  durableMutation.add(Date.now() - started);
  expect(response, [200, 201], "enqueue");
}

export function consume(data) {
  const iteration = exec.scenario.iterationInTest;
  const queue = data.queues[iteration % 10 === 0 ? iteration % data.queues.length : 0];
  const started = Date.now();
  const response = jsonRequest("POST", `${baseURL}/v1/queues/${encodeURIComponent(queue)}/messages:receive`, {
    visibility_timeout_ms: 5000,
    wait_timeout_ms: 100,
  });
  dequeueLatency.add(Date.now() - started);
  expect(response, [200], "receive");
  if (response.status !== 200) return;
  const messages = response.json("messages") || [];
  if (messages.length === 0) {
    sleep(0.01);
    return;
  }
  const delivery = messages[0];
  const action = iteration % 20 === 0 ? "nack" : "ack";
  const body = action === "ack"
    ? { receipt_handle: delivery.receipt_handle }
    : { receipt_handle: delivery.receipt_handle, retry_delay_ms: iteration % 3 === 0 ? 50 : 0, reason: "load-injected" };
  const mutationStarted = Date.now();
  const transition = jsonRequest(
    "POST",
    `${baseURL}/v1/queues/${encodeURIComponent(queue)}/messages/${encodeURIComponent(delivery.message.id)}/${action}`,
    body,
    `${action}-${delivery.message.id}-${delivery.delivery_count}`,
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
