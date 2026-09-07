import { cloudflareTest } from "@cloudflare/vitest-plugin";
import { defineConfig } from "vitest/config";

export default defineConfig({
  plugins: [
    cloudflareTest({
      wrangler: { configPath: "./wrangler.jsonc" },
      miniflare: {
        bindings: {
          GITHUB_WEBHOOK_SECRET: "github-test-secret",
          ANTENNA_HANDOFF_SECRET: "handoff-test-secret",
          CF_ACCESS_CLIENT_ID: "test-client-id",
          CF_ACCESS_CLIENT_SECRET: "test-client-secret"
        }
      }
    })
  ]
});
