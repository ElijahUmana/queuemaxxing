import assert from "node:assert/strict";
import test from "node:test";

import { APIError, QueueAPI, routes } from "./api.js";

function response(status, body, headers = {}) {
  return new Response(body === undefined ? null : JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json", ...headers },
  });
}

async function withFetch(handler, run) {
  const original = globalThis.fetch;
  globalThis.fetch = handler;
  try {
    await run();
  } finally {
    globalThis.fetch = original;
  }
}

test("routes encode opaque path segments", () => {
  assert.equal(routes.queue("a/b c"), "/api/v1/queues/a%2Fb%20c");
  assert.equal(routes.receive("jobs"), "/api/v1/queues/jobs/messages:receive");
  assert.equal(routes.redrive("jobs", "id/1"), "/api/v1/queues/jobs/dead-letters/id%2F1/redrive");
});

test("enqueue sends strict DTO and idempotency header", async () => {
  await withFetch(async (path, options) => {
    assert.equal(path, "/api/v1/queues/jobs/messages");
    assert.equal(options.method, "POST");
    assert.equal(options.headers.get("Idempotency-Key"), "enqueue-1");
    assert.deepEqual(JSON.parse(options.body), { payload: { task: "x" }, priority: 4, delay_ms: 20 });
    return response(201, { data: { id: "message-1" }, replayed: false }, { "X-Request-ID": "req-1" });
  }, async () => {
    const activity = [];
    const api = new QueueAPI((entry) => activity.push(entry));
    const result = await api.enqueue("jobs", { payload: { task: "x" }, priority: 4, delay_ms: 20 }, "enqueue-1");
    assert.equal(result.data.id, "message-1");
    assert.equal(activity[0].status, 201);
    assert.equal(activity[0].requestId, "req-1");
  });
});

test("receipt mutations use receipt_handle and millisecond fields", async () => {
  const requests = [];
  await withFetch(async (path, options) => {
    requests.push({ path, body: JSON.parse(options.body), key: options.headers.get("Idempotency-Key") });
    return response(200, { data: {} });
  }, async () => {
    const api = new QueueAPI();
    await api.ack("q", "m", "secret", "ack-1");
    await api.nack("q", "m", "secret", { retry_delay_ms: 75, reason: "retry" }, "nack-1");
    await api.extend("q", "m", "secret", 900, "extend-1");
  });

  assert.deepEqual(requests, [
    { path: "/api/v1/queues/q/messages/m/ack", body: { receipt_handle: "secret" }, key: "ack-1" },
    { path: "/api/v1/queues/q/messages/m/nack", body: { receipt_handle: "secret", retry_delay_ms: 75, reason: "retry" }, key: "nack-1" },
    { path: "/api/v1/queues/q/messages/m/extend", body: { receipt_handle: "secret", visibility_timeout_ms: 900 }, key: "extend-1" },
  ]);
});

test("receive omits idempotency header and preserves response envelope", async () => {
  await withFetch(async (path, options) => {
    assert.equal(path, "/api/v1/queues/q/messages:receive");
    assert.equal(options.headers.has("Idempotency-Key"), false);
    assert.deepEqual(JSON.parse(options.body), { visibility_timeout_ms: 1000, wait_timeout_ms: 250 });
    return response(200, { messages: [], polled_at: "2026-08-26T00:00:00Z" });
  }, async () => {
    const api = new QueueAPI();
    const result = await api.receive("q", { visibility_timeout_ms: 1000, wait_timeout_ms: 250 });
    assert.deepEqual(result.messages, []);
  });
});

test("pagination omits empty values and encodes cursor", async () => {
  await withFetch(async (path) => {
    assert.equal(path, "/api/v1/queues/q/messages?limit=50&cursor=a%2Fb%2B1");
    return response(200, { messages: [], snapshot_lsn: "10" });
  }, async () => {
    const api = new QueueAPI();
    await api.listMessages("q", { limit: 50, cursor: "a/b+1", state: "" });
  });
});

test("problem details become typed API errors", async () => {
  await withFetch(async () => response(409, {
    type: "urn:queuemaxxing:problem:stale_receipt",
    title: "Stale receipt",
    status: 409,
    code: "stale_receipt",
    detail: "The receipt no longer owns the lease.",
    request_id: "req-problem",
  }), async () => {
    const api = new QueueAPI();
    await assert.rejects(
      api.ack("q", "m", "old", "ack-old"),
      (error) => error instanceof APIError
        && error.status === 409
        && error.code === "stale_receipt"
        && error.requestId === "req-problem",
    );
  });
});

test("invalid JSON and network errors remain visible", async () => {
  const activity = [];
  const api = new QueueAPI((entry) => activity.push(entry));

  await withFetch(async () => new Response("not json", { status: 200 }), async () => {
    await assert.rejects(api.stats(), (error) => error instanceof APIError && error.code === "invalid_response");
  });

  await withFetch(async () => { throw new TypeError("offline"); }, async () => {
    await assert.rejects(api.stats(), /offline/);
  });
  assert.equal(activity.at(-1).status, 0);
});

test("abort signals cancel fetch without logging a network failure", async () => {
  const activity = [];
  const controller = new AbortController();
  const api = new QueueAPI((entry) => activity.push(entry));

  await withFetch(async (_path, options) => new Promise((_resolve, reject) => {
    options.signal.addEventListener("abort", () => reject(new DOMException("Aborted", "AbortError")), { once: true });
  }), async () => {
    const pending = api.listQueues({ signal: controller.signal });
    controller.abort();
    await assert.rejects(pending, (error) => error.name === "AbortError");
  });
  assert.deepEqual(activity, []);
});
