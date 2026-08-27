import { spawn } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";

import { expect, test } from "@playwright/test";

const hostilePayload = '<img src=x onerror="globalThis.__qmaxXSS = 1">';
const repositoryRoot = path.resolve(process.cwd(), "../..");
const qmaxBinary = process.env.QMAX_BINARY || path.join(repositoryRoot, "bin", process.platform === "win32" ? "qmax.exe" : "qmax");
const workbenchBinary = process.env.QMAX_WORKBENCH_BINARY || path.join(repositoryRoot, "bin", process.platform === "win32" ? "qmax-workbench.exe" : "qmax-workbench");
const apiAddress = "127.0.0.1:18080";
const workbenchAddress = "127.0.0.1:18081";
const apiURL = `http://${apiAddress}`;
const workbenchURL = `http://${workbenchAddress}`;
let dataDirectory;
let qmax;
let workbench;

test.beforeAll(async () => {
  dataDirectory = await mkdtemp(path.join(tmpdir(), "qmax-browser-"));
  qmax = startProcess(qmaxBinary, ["-listen", apiAddress, "-data-dir", dataDirectory]);
  await waitUntilReady(`${apiURL}/health/ready`, qmax);
  workbench = startProcess(workbenchBinary, ["-listen", workbenchAddress, "-api-url", apiURL]);
  await waitUntilReady(`${workbenchURL}/healthz`, workbench);
});

test.afterAll(async () => {
  await stopProcess(workbench);
  await stopProcess(qmax);
  await rm(dataDirectory, { recursive: true, force: true });
});

for (const configuration of [
  { ordering: "fifo", priority: false },
  { ordering: "lifo", priority: false },
  { ordering: "fifo", priority: true },
  { ordering: "lifo", priority: true },
]) {
  test(`public workbench lifecycle ${configuration.ordering} priority=${configuration.priority}`, async ({ page }) => {
    const consoleErrors = [];
    const pageErrors = [];
    page.on("console", (message) => {
      if (message.type() === "error") consoleErrors.push(message.text());
    });
    page.on("pageerror", (error) => pageErrors.push(error.message));

    await page.goto(workbenchURL);
    await expect(page).toHaveTitle(/Queue workbench/i);
    await page.getByRole("button", { name: /create (first )?queue/i }).first().click();

    const queueName = `e2e-${configuration.ordering}-${configuration.priority}-${Date.now()}`;
    await page.locator("#queue-name").fill(queueName);
    await page.locator(`label:has(input[name="ordering"][value="${configuration.ordering}"])`).click();
    if (configuration.priority) await page.locator("label:has(#queue-priority)").click();
    await page.locator("#create-queue-form").getByRole("button", { name: "Create queue" }).click();

    await expect(page.locator("#queue-title")).toHaveText(queueName);
    await page.locator("#enqueue-payload").fill(JSON.stringify({ hostilePayload, ordinal: 1 }));
    await page.locator("#enqueue-priority").fill(configuration.priority ? "7" : "0");
    await page.getByRole("button", { name: "Enqueue message" }).click();
    await expect(page.locator("#count-ready")).toHaveText("1");

    if (configuration.ordering === "fifo" && !configuration.priority) {
      await stopProcess(qmax);
      qmax = startProcess(qmaxBinary, ["-listen", apiAddress, "-data-dir", dataDirectory]);
      await waitUntilReady(`${apiURL}/health/ready`, qmax);
      await page.reload();
      await expect(page.locator("#queue-title")).toHaveText(queueName);
      await expect(page.locator("#count-ready")).toHaveText("1");
    }

    await page.getByRole("button", { name: "Receive next" }).click();
    await expect(page.locator("#delivery-card")).toBeVisible();
    const renderedPayload = JSON.parse(await page.locator("#delivery-payload").textContent());
    expect(renderedPayload.hostilePayload).toBe(hostilePayload);
    expect(await page.evaluate(() => globalThis.__qmaxXSS)).toBeUndefined();

    await page.getByRole("button", { name: "Acknowledge" }).click();
    await expect(page.locator("#delivery-card")).toBeHidden();
    await expect(page.locator("#count-flight")).toHaveText("0");

    expect(consoleErrors).toEqual([]);
    expect(pageErrors).toEqual([]);
  });
}

test("security headers and keyboard path", async ({ page, request }) => {
  const consoleErrors = [];
  const pageErrors = [];
  page.on("console", (message) => {
    if (message.type() === "error") consoleErrors.push(message.text());
  });
  page.on("pageerror", (error) => pageErrors.push(error.message));

  const response = await request.get(workbenchURL);
  expect(response.headers()["content-security-policy"]).toContain("default-src 'self'");
  expect(response.headers()["x-content-type-options"]).toBe("nosniff");
  expect(response.headers()["x-frame-options"]).toBe("DENY");

  await page.goto(workbenchURL);
  await page.keyboard.press("Tab");
  await expect(page.getByRole("link", { name: "Skip to workbench" })).toBeFocused();
  await page.keyboard.press("Enter");
  await expect(page.locator("#workspace")).toBeFocused();
  expect(consoleErrors).toEqual([]);
  expect(pageErrors).toEqual([]);
});

function startProcess(command, args) {
  const child = spawn(command, args, { stdio: ["ignore", "pipe", "pipe"], windowsHide: true });
  const output = [];
  for (const stream of [child.stdout, child.stderr]) {
    stream.setEncoding("utf8");
    stream.on("data", (chunk) => output.push(chunk));
  }
  child.output = output;
  child.exited = new Promise((resolve, reject) => {
    child.once("error", reject);
    child.once("exit", (code, signal) => resolve({ code, signal }));
  });
  return child;
}

async function waitUntilReady(url, child) {
  const deadline = Date.now() + 15_000;
  while (Date.now() < deadline) {
    const outcome = await Promise.race([
      child.exited.then((exit) => ({ exit })),
      fetch(url, { cache: "no-store" }).then((response) => ({ response })).catch((error) => ({ error })),
    ]);
    if (outcome.exit) throw new Error(`Process exited before ${url} became ready: ${JSON.stringify(outcome.exit)}\n${child.output.join("")}`);
    if (outcome.response?.ok) return;
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  throw new Error(`Timed out waiting for ${url}\n${child.output.join("")}`);
}

async function stopProcess(child) {
  if (!child || child.exitCode !== null || child.signalCode !== null) return;
  child.kill("SIGTERM");
  const result = await Promise.race([
    child.exited,
    new Promise((resolve) => setTimeout(() => resolve(null), 10_000)),
  ]);
  if (!result) {
    child.kill("SIGKILL");
    await child.exited;
  }
}
