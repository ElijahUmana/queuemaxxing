import { defineConfig, devices } from "@playwright/test";

const baseURL = process.env.QMAX_WORKBENCH_URL || "http://127.0.0.1:8081";

export default defineConfig({
  testDir: "./tests",
  fullyParallel: false,
  forbidOnly: true,
  retries: 0,
  workers: 1,
  timeout: 30_000,
  expect: { timeout: 5_000 },
  reporter: [
    ["line"],
    ["html", { outputFolder: "../../artifacts/playwright-report", open: "never" }],
  ],
  outputDir: "../../artifacts/playwright-results",
  use: {
    baseURL,
    trace: "retain-on-failure",
    screenshot: "only-on-failure",
    video: "retain-on-failure",
  },
  projects: [
    { name: "chromium", use: { ...devices["Desktop Chrome"] } },
    { name: "firefox", use: { ...devices["Desktop Firefox"] } },
    { name: "webkit", use: { ...devices["Desktop Safari"] } },
  ],
});
