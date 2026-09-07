const encoder = new TextEncoder();

function jsonResponse(status: number, body: Record<string, unknown>): Response {
  return Response.json(body, {
    status,
    headers: {
      "cache-control": "no-store",
      "x-content-type-options": "nosniff"
    }
  });
}

function hex(bytes: ArrayBuffer | ArrayBufferView): string {
  const view = bytes instanceof ArrayBuffer
    ? new Uint8Array(bytes)
    : new Uint8Array(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  return Array.from(view, (byte) => byte.toString(16).padStart(2, "0")).join("");
}

function unhex(value: string): Uint8Array | null {
  if (!/^[0-9a-f]{64}$/i.test(value)) return null;
  const bytes = new Uint8Array(32);
  for (let index = 0; index < bytes.length; index += 1) {
    bytes[index] = Number.parseInt(value.slice(index * 2, index * 2 + 2), 16);
  }
  return bytes;
}

async function hmac(secret: string, data: string | Uint8Array): Promise<ArrayBuffer> {
  const key = await crypto.subtle.importKey(
    "raw",
    encoder.encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"]
  );
  return crypto.subtle.sign("HMAC", key, typeof data === "string" ? encoder.encode(data) : data);
}

async function readBounded(body: ReadableStream<Uint8Array> | null, limit: number): Promise<Uint8Array> {
  if (!body) return new Uint8Array();
  const reader = body.getReader();
  const chunks: Uint8Array[] = [];
  let size = 0;
  try {
    while (true) {
      const item = await reader.read();
      if (item.done) break;
      size += item.value.byteLength;
      if (size > limit) throw new RangeError("payload too large");
      chunks.push(item.value);
    }
  } finally {
    reader.releaseLock();
  }
  const bodyBytes = new Uint8Array(size);
  let offset = 0;
  for (const chunk of chunks) {
    bodyBytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return bodyBytes;
}

async function verifyGithub(secret: string, body: Uint8Array, signature: string): Promise<boolean> {
  const offered = signature.startsWith("sha256=") ? unhex(signature.slice(7)) : null;
  if (!offered) return false;
  const expected = new Uint8Array(await hmac(secret, body));
  return crypto.subtle.timingSafeEqual(expected, offered);
}

type UpstreamFetch = (input: string, init: RequestInit) => Promise<Response>;

async function handleGithub(
  request: Request,
  env: Env,
  upstreamFetch: UpstreamFetch = (input, init) => fetch(input, init)
): Promise<Response> {
  if (request.method !== "POST") {
    return jsonResponse(405, { error: "method not allowed" });
  }
  const delivery = request.headers.get("x-github-delivery");
  const event = request.headers.get("x-github-event");
  const signature = request.headers.get("x-hub-signature-256");
  const contentType = request.headers.get("content-type")?.split(";", 1)[0]?.trim().toLowerCase();
  if (!delivery || !event || !signature) {
    return jsonResponse(400, { error: "required GitHub headers are missing" });
  }
  if (delivery.length > 200 || event.length > 100 || contentType !== "application/json") {
    return jsonResponse(400, { error: "invalid GitHub request metadata" });
  }
  const limit = Number.parseInt(env.MAX_BODY_BYTES, 10);
  const declaredLength = request.headers.get("content-length");
  if (!Number.isSafeInteger(limit) || limit <= 0) {
    return jsonResponse(503, { error: "receiver is not configured" });
  }
  if (declaredLength) {
    const size = Number.parseInt(declaredLength, 10);
    if (!Number.isSafeInteger(size) || size < 0) return jsonResponse(400, { error: "invalid content length" });
    if (size > limit) return jsonResponse(413, { error: "payload too large" });
  }

  let body: Uint8Array;
  try {
    body = await readBounded(request.body, limit);
  } catch (error) {
    if (error instanceof RangeError) return jsonResponse(413, { error: "payload too large" });
    throw error;
  }
  if (!(await verifyGithub(env.GITHUB_WEBHOOK_SECRET, body, signature))) {
    return jsonResponse(401, { error: "invalid GitHub signature" });
  }
  try {
    const parsed: unknown = JSON.parse(new TextDecoder().decode(body));
    if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
      return jsonResponse(400, { error: "GitHub payload must be a JSON object" });
    }
  } catch {
    return jsonResponse(400, { error: "malformed GitHub JSON" });
  }

  const bodyDigest = hex(await crypto.subtle.digest("SHA-256", body));
  const timestamp = Math.floor(Date.now() / 1000).toString();
  const canonical = `${timestamp}\n${delivery}\n${event}\n${bodyDigest}\n`;
  const handoffSignature = hex(await hmac(env.ANTENNA_HANDOFF_SECRET, canonical));
  const upstream = await upstreamFetch(env.UPSTREAM_URL, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "content-length": body.byteLength.toString(),
      "user-agent": "powerfarm-github-ingress/0.1",
      "x-antenna-timestamp": timestamp,
      "x-antenna-delivery": delivery,
      "x-antenna-github-event": event,
      "x-antenna-body-sha256": bodyDigest,
      "x-antenna-signature": `v1=${handoffSignature}`,
      "cf-access-client-id": env.CF_ACCESS_CLIENT_ID,
      "cf-access-client-secret": env.CF_ACCESS_CLIENT_SECRET
    },
    body
  });
  if (upstream.status !== 202) {
    console.error(JSON.stringify({ message: "Antenna rejected handoff", delivery, event, status: upstream.status }));
    return jsonResponse(502, { error: "durable receiver unavailable" });
  }
  const accepted: unknown = await upstream.json();
  if (typeof accepted !== "object" || accepted === null || Array.isArray(accepted)) {
    return jsonResponse(502, { error: "invalid durable receiver acknowledgement" });
  }
  const result = accepted as Record<string, unknown>;
  console.log(JSON.stringify({
    message: "GitHub delivery durably accepted",
    delivery,
    event,
    duplicate: result.duplicate === true,
    classification: result.classification
  }));
  return jsonResponse(202, {
    status: "durably_accepted",
    delivery_id: delivery,
    receipt_id: result.receipt_id,
    duplicate: result.duplicate === true,
    classification: result.classification
  });
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    if (url.pathname !== "/github") return jsonResponse(404, { error: "not found" });
    try {
      return await handleGithub(request, env);
    } catch (error) {
      console.error(JSON.stringify({
        message: "GitHub ingress failed",
        error: error instanceof Error ? error.message : "unknown error"
      }));
      return jsonResponse(500, { error: "internal error" });
    }
  }
} satisfies ExportedHandler<Env>;
