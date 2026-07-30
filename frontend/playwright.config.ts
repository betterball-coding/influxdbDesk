import { defineConfig } from '@playwright/test'

const port = 41739
const baseURL = `http://127.0.0.1:${port}`

export default defineConfig({
  testDir: './e2e',
  timeout: 60_000,
  expect: { timeout: 10_000 },
  fullyParallel: false,
  workers: 1,
  retries: 0,
  reporter: 'line',
  outputDir: '/tmp/influxdesk-playwright-results',
  preserveOutput: 'never',
  use: {
    baseURL,
    browserName: 'chromium',
    screenshot: 'off',
    trace: 'off',
    video: 'off',
  },
  webServer: {
    command: `npm run dev -- --host 127.0.0.1 --port ${port} --strictPort`,
    url: baseURL,
    reuseExistingServer: false,
    timeout: 120_000,
  },
  projects: [
    { name: 'desktop-1440x900', use: { viewport: { width: 1440, height: 900 } } },
    { name: 'review-1119x818', use: { viewport: { width: 1119, height: 818 } } },
    { name: 'minimum-1180x720', use: { viewport: { width: 1180, height: 720 } } },
  ],
})
