import { env } from "cloudflare:workers";
import { afterEach, describe, expect, it, vi } from "vitest";
import worker from "../src/index";

type UpstreamFetch = (input: string, init: RequestInit) => Promise<Response>;

async function dispatch(input: Request): Promise<Response> {
  return worker.fetch(input, env);
}

const encoder = new TextEncoder();

async function signature(body: Uint8Array): Promise<string> {
  const key = await crypto.subtle.importKey(
    "raw", encoder.encode(env.GITHUB_WEBHOOK_SECRET),
    { name: "HMAC", hash: "SHA-256" }, false, ["sign"]
  );
  const digest = new Uint8Array(await crypto.subtle.sign("HMAC", key, body));
  return `sha256=${Array.from(digest, (byte) => byte.toString(16).padStart(2, "0")).join("")}`;
}

async function request(bodyText = "{}", overrides: Record<string, string> = {}): Promise<Request> {
  const body = encoder.encode(bodyText);
  return new Request("https://ingress.minilab.work/github", {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "x-github-delivery": "test-delivery",
      "x-github-event": "push",
      "x-hub-signature-256": await signature(body),
      ...overrides
    },
    body
  });
}

describe("GitHub ingress", () => {
  afterEach(() => vi.unstubAllGlobals());

  it("rejects an invalid signature", async () => {
    const response = await dispatch(await request("{}", { "x-hub-signature-256": `sha256=${"00".repeat(32)}` }));
    expect(response.status).toBe(401);
  });

  it("rejects a tampered raw body", async () => {
    const valid = await request("{}");
    const headers = new Headers(valid.headers);
    const response = await dispatch(new Request(valid.url, { method: "POST", headers, body: "{\"tampered\":true}" }));
    expect(response.status).toBe(401);
  });

  it("requires a delivery identity", async () => {
    const valid = await request();
    const headers = new Headers(valid.headers);
    headers.delete("x-github-delivery");
    const response = await dispatch(new Request(valid.url, { method: "POST", headers, body: "{}" }));
    expect(response.status).toBe(400);
  });

  it("rejects oversized payloads before forwarding", async () => {
    const response = await dispatch(await request("x".repeat(1_048_577)));
    expect(response.status).toBe(413);
  });

  it("returns success only after Antenna acknowledges durable acceptance", async () => {
    const upstream: UpstreamFetch = async (input, init) => {
      expect(input).toBe("https://antenna.minilab.work/internal/github");
      expect(init.method).toBe("POST");
      expect(new Headers(init.headers).get("x-antenna-delivery")).toBe("test-delivery");
      return Response.json(
        { receipt_id: "rcp_live", duplicate: false, classification: "KNOWN" },
        { status: 202 }
      );
    };
    vi.stubGlobal("fetch", upstream);
    const response = await dispatch(await request());
    expect(response.status).toBe(202);
    await expect(response.json()).resolves.toMatchObject({ status: "durably_accepted", receipt_id: "rcp_live" });
  });
});
