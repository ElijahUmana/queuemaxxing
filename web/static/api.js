const API_ROOT = "/api/v1";

export const routes = Object.freeze({
  queues: () => `${API_ROOT}/queues`,
  queue: (queue) => `${API_ROOT}/queues/${segment(queue)}`,
  messages: (queue) => `${API_ROOT}/queues/${segment(queue)}/messages`,
  receive: (queue) => `${API_ROOT}/queues/${segment(queue)}/messages:receive`,
  messageAction: (queue, message, action) => `${API_ROOT}/queues/${segment(queue)}/messages/${segment(message)}/${action}`,
  deadLetters: (queue) => `${API_ROOT}/queues/${segment(queue)}/dead-letters`,
  redrive: (queue, message) => `${API_ROOT}/queues/${segment(queue)}/dead-letters/${segment(message)}/redrive`,
  stats: () => `${API_ROOT}/stats`,
  ready: () => "/api/health/ready",
});

export class APIError extends Error {
  constructor(status, code, message, requestId, details) {
    super(message);
    this.name = "APIError";
    this.status = status;
    this.code = code;
    this.requestId = requestId;
    this.details = details;
  }
}

export class QueueAPI {
  constructor(onActivity = () => {}) {
    this.onActivity = onActivity;
  }

  listQueues(options) {
    return this.request("GET", routes.queues(), { signal: options?.signal });
  }

  createQueue(config, idempotencyKey, options) {
    return this.request("POST", routes.queues(), { body: config, idempotencyKey, signal: options?.signal });
  }

  getQueue(queue, options) {
    return this.request("GET", routes.queue(queue), { signal: options?.signal });
  }

  enqueue(queue, request, idempotencyKey, options) {
    return this.request("POST", routes.messages(queue), { body: request, idempotencyKey, signal: options?.signal });
  }

  receive(queue, request, options) {
    return this.request("POST", routes.receive(queue), { body: request, signal: options?.signal });
  }

  ack(queue, messageId, receipt, idempotencyKey, options) {
    return this.request("POST", routes.messageAction(queue, messageId, "ack"), {
      body: { receipt_handle: receipt }, idempotencyKey, signal: options?.signal,
    });
  }

  nack(queue, messageId, receipt, request, idempotencyKey, options) {
    return this.request("POST", routes.messageAction(queue, messageId, "nack"), {
      body: { receipt_handle: receipt, ...request }, idempotencyKey, signal: options?.signal,
    });
  }

  extend(queue, messageId, receipt, visibilityTimeoutMS, idempotencyKey, options) {
    return this.request("POST", routes.messageAction(queue, messageId, "extend"), {
      body: { receipt_handle: receipt, visibility_timeout_ms: visibilityTimeoutMS }, idempotencyKey, signal: options?.signal,
    });
  }

  listMessages(queue, query = {}, options) {
    return this.request("GET", withQuery(routes.messages(queue), query), { signal: options?.signal });
  }

  listDeadLetters(queue, query = {}, options) {
    return this.request("GET", withQuery(routes.deadLetters(queue), query), { signal: options?.signal });
  }

  redrive(queue, messageId, request, idempotencyKey, options) {
    return this.request("POST", routes.redrive(queue, messageId), {
      body: request, idempotencyKey, signal: options?.signal,
    });
  }

  stats(options) {
    return this.request("GET", routes.stats(), { signal: options?.signal });
  }

  async request(method, path, options = {}) {
    const startedAt = performance.now();
    const headers = new Headers({ Accept: "application/json" });
    if (options.body !== undefined) headers.set("Content-Type", "application/json");
    if (options.idempotencyKey) headers.set("Idempotency-Key", options.idempotencyKey);

    let response;
    try {
      response = await fetch(path, {
        method,
        headers,
        body: options.body === undefined ? undefined : JSON.stringify(options.body),
        signal: options.signal,
        cache: "no-store",
      });
    } catch (error) {
      if (error.name !== "AbortError") {
        this.onActivity({ method, path, status: 0, duration: performance.now() - startedAt, error: "Network unavailable" });
      }
      throw error;
    }

    const requestId = response.headers.get("X-Request-ID") || "";
    const raw = await response.text();
    let data = null;
    if (raw) {
      try {
        data = JSON.parse(raw);
      } catch {
        throw new APIError(response.status, "invalid_response", "The API returned invalid JSON.", requestId, null);
      }
    }
    this.onActivity({ method, path, status: response.status, duration: performance.now() - startedAt, requestId });

    if (!response.ok) {
      const problem = data || {};
      throw new APIError(
        response.status,
        problem.code || "request_failed",
        problem.message || problem.detail || `Request failed with HTTP ${response.status}.`,
        problem.request_id || requestId,
        problem.details,
      );
    }

    return data;
  }
}

function segment(value) {
  return encodeURIComponent(String(value));
}

function withQuery(path, values) {
  const query = new URLSearchParams();
  for (const [name, value] of Object.entries(values)) {
    if (value !== undefined && value !== null && value !== "") query.set(name, String(value));
  }
  const encoded = query.toString();
  return encoded ? `${path}?${encoded}` : path;
}
