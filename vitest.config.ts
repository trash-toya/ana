import { defineConfig } from "vitest/config";

// One runner for every suite. Tests import explicitly (globals: false) and
// keep node:assert/strict; vitest is the driver, node:test is gone.
// DB and e2e suites talk to Postgres/docker, so forks + generous timeouts.
export default defineConfig({
  test: {
    globals: false,
    pool: "forks",
    include: [
      "packages/*/test/*.test.mjs",
      "services/*/test/*.test.mjs",
      "db/test/**/*.test.mjs",
    ],
    testTimeout: 30_000,
    hookTimeout: 30_000,
    // CI service containers keep Postgres's default max_connections=100 (unlike
    // ops/docker-compose.yml's 300). Each DB test file opens a pool of up to 40,
    // so two workers keep the whole run comfortably inside that limit.
    maxWorkers: process.env.CI ? 2 : undefined,
  },
});
