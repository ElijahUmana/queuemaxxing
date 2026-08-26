import { expect, test } from "@playwright/test";

const hostilePayload = '<img src=x onerror="globalThis.__qmaxXSS = 1">';

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

    await page.goto("/");
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
  const response = await request.get("/");
  expect(response.headers()["content-security-policy"]).toContain("default-src 'self'");
  expect(response.headers()["x-content-type-options"]).toBe("nosniff");
  expect(response.headers()["x-frame-options"]).toBe("DENY");

  await page.goto("/");
  await page.keyboard.press("Tab");
  await expect(page.getByRole("link", { name: "Skip to workbench" })).toBeFocused();
  await page.keyboard.press("Enter");
  await expect(page.locator("#workspace")).toBeFocused();
});
