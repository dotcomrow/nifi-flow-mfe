import fs from "node:fs/promises";
import { createHash, createSign } from "node:crypto";

const GOOGLE_OAUTH_TOKEN_ENDPOINT = "https://oauth2.googleapis.com/token";

function asString(value, fallback = "") {
  if (typeof value === "string") {
    return value.trim();
  }
  if (typeof value === "number") {
    return String(value);
  }
  return fallback;
}

function ensureRequired(value, name) {
  const normalized = asString(value);
  if (!normalized) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return normalized;
}

function ensureObject(value, fallback = {}) {
  if (value && typeof value === "object" && !Array.isArray(value)) {
    return value;
  }
  return fallback;
}

function normalizePath(pathValue, fallback) {
  const raw = asString(pathValue, fallback);
  if (!raw) {
    return fallback;
  }
  return raw.startsWith("/") ? raw : `/${raw}`;
}

function normalizeChannel(value) {
  const normalized = asString(value, "preview").toLowerCase();
  return normalized === "prod" ? "prod" : "preview";
}

function resolveRegistryServiceBaseUrl() {
  const legacy = asString(process.env.MODULE_REGISTRY_SERVICE_URL);
  const preview = asString(process.env.MODULE_REGISTRY_SERVICE_URL_PREVIEW);
  return preview || legacy;
}

function sha256Hex(bytes) {
  return createHash("sha256").update(bytes).digest("hex");
}

function toIdempotencyKey(moduleKey, moduleVersion, moduleSha) {
  const base = `${moduleKey}:${moduleVersion}:${moduleSha}`;
  return createHash("sha256").update(base).digest("hex");
}

function toBase64Url(value) {
  const buffer = Buffer.isBuffer(value) ? value : Buffer.from(value, "utf8");
  return buffer.toString("base64").replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
}

function normalizePrivateKey(privateKey) {
  const raw = asString(privateKey);
  if (!raw) {
    return "";
  }
  return raw.includes("\\n") ? raw.replace(/\\n/g, "\n") : raw;
}

async function requestGoogleServiceAccountIdToken({
  serviceAccountEmail,
  serviceAccountPrivateKey,
  tokenAudience,
}) {
  const issuedAtEpoch = Math.floor(Date.now() / 1000);
  const expiresAtEpoch = issuedAtEpoch + 3600;

  const jwtHeader = { alg: "RS256", typ: "JWT" };
  const jwtClaims = {
    iss: serviceAccountEmail,
    aud: GOOGLE_OAUTH_TOKEN_ENDPOINT,
    iat: issuedAtEpoch,
    exp: expiresAtEpoch,
    target_audience: tokenAudience,
  };

  const encodedHeader = toBase64Url(JSON.stringify(jwtHeader));
  const encodedClaims = toBase64Url(JSON.stringify(jwtClaims));
  const unsignedAssertion = `${encodedHeader}.${encodedClaims}`;

  const signer = createSign("RSA-SHA256");
  signer.update(unsignedAssertion);
  signer.end();
  const signature = signer.sign(serviceAccountPrivateKey);
  const assertion = `${unsignedAssertion}.${toBase64Url(signature)}`;

  const body = new URLSearchParams({
    grant_type: "urn:ietf:params:oauth:grant-type:jwt-bearer",
    assertion,
  });

  const response = await fetch(GOOGLE_OAUTH_TOKEN_ENDPOINT, {
    method: "POST",
    headers: {
      "content-type": "application/x-www-form-urlencoded",
    },
    body,
  });

  const responseText = await response.text();
  let payload = {};
  try {
    payload = JSON.parse(responseText);
  } catch {
    payload = {};
  }

  if (!response.ok || !payload?.id_token) {
    throw new Error(
      `Unable to mint Google service-account id_token (${response.status}): ${responseText || "empty response"}`,
    );
  }

  return payload.id_token;
}

async function resolvePublishAuthToken() {
  const serviceAccountEmail = ensureRequired(
    process.env.MODULE_REGISTRY_SERVICE_GOOGLE_SERVICE_ACCOUNT_EMAIL,
    "MODULE_REGISTRY_SERVICE_GOOGLE_SERVICE_ACCOUNT_EMAIL",
  );
  const serviceAccountPrivateKey = normalizePrivateKey(
    ensureRequired(
      process.env.MODULE_REGISTRY_SERVICE_GOOGLE_SERVICE_ACCOUNT_PRIVATE_KEY,
      "MODULE_REGISTRY_SERVICE_GOOGLE_SERVICE_ACCOUNT_PRIVATE_KEY",
    ),
  );
  const tokenAudience = ensureRequired(
    process.env.MODULE_REGISTRY_SERVICE_GOOGLE_TOKEN_AUDIENCE,
    "MODULE_REGISTRY_SERVICE_GOOGLE_TOKEN_AUDIENCE",
  );

  return requestGoogleServiceAccountIdToken({
    serviceAccountEmail,
    serviceAccountPrivateKey,
    tokenAudience,
  });
}

async function postWithRetry(url, body, headers, retries = 4) {
  let attempt = 0;
  while (true) {
    attempt += 1;
    const response = await fetch(url, {
      method: "POST",
      headers,
      body,
    });

    if (response.ok) {
      const text = await response.text();
      if (!text) {
        return {};
      }
      try {
        return JSON.parse(text);
      } catch {
        return { raw: text };
      }
    }

    const responseBody = await response.text();
    const retryable = response.status >= 500 || response.status === 429;
    if (!retryable || attempt > retries) {
      throw new Error(
        `Module registry publish failed (${response.status}) after ${attempt} attempt(s): ${responseBody || "empty response"}`,
      );
    }

    const backoffMs = Math.min(5000, 300 * 2 ** (attempt - 1));
    await new Promise((resolve) => setTimeout(resolve, backoffMs));
  }
}

async function withGhaOutput(outputs) {
  const outputPath = asString(process.env.GITHUB_OUTPUT);
  if (!outputPath) {
    return;
  }
  const lines = Object.entries(outputs)
    .map(([key, value]) => `${key}=${String(value ?? "")}`)
    .join("\n");
  await fs.appendFile(outputPath, `${lines}\n`, "utf8");
}

async function main() {
  const channel = normalizeChannel(process.env.MODULE_CHANNEL);
  const registryServiceBaseUrl = ensureRequired(
    resolveRegistryServiceBaseUrl(),
    "MODULE_REGISTRY_SERVICE_URL_PREVIEW (or MODULE_REGISTRY_SERVICE_URL)",
  );
  const publishPath = normalizePath(
    process.env.MODULE_REGISTRY_SERVICE_PUBLISH_PATH,
    "/v1/modules/publish",
  );
  const authToken = await resolvePublishAuthToken();

  const bundlePath = asString(process.env.MODULE_PUBLISH_BUNDLE_PATH, "dist/nifi-flow-mfe.js");
  const manifestPath = asString(process.env.MODULE_PUBLISH_MANIFEST_PATH, "dist/module.publish.json");

  const bundleBytes = await fs.readFile(bundlePath);
  const manifestBytes = await fs.readFile(manifestPath);
  const manifest = JSON.parse(manifestBytes.toString("utf8"));

  const moduleKey = ensureRequired(manifest.module_key, "module.publish.module_key");
  const moduleVersion = ensureRequired(manifest.module_version, "module.publish.module_version");
  const moduleSha = asString(manifest?.bundle?.sha256, sha256Hex(bundleBytes));

  const payload = {
    module_key: moduleKey,
    module_version: moduleVersion,
    channel,
    published_at: asString(manifest.published_at),
    provider: asString(manifest.provider),
    component_type: asString(manifest.component_type),
    release: ensureObject(manifest.release),
    definition: ensureObject(manifest.definition),
    seed: ensureObject(manifest.seed),
    checksums: {
      ...ensureObject(manifest.checksums),
      bundle_sha256: moduleSha,
    },
  };

  const body = new FormData();
  body.set("payload", JSON.stringify(payload));
  body.set("bundle_file", new Blob([bundleBytes], { type: "application/javascript" }), "nifi-flow-mfe.js");
  body.set("manifest_file", new Blob([manifestBytes], { type: "application/json" }), "module.publish.json");

  const headers = {
    "x-idempotency-key": toIdempotencyKey(moduleKey, moduleVersion, moduleSha),
    authorization: `Bearer ${authToken}`,
  };

  const targetUrl = `${registryServiceBaseUrl.replace(/\/+$/g, "")}${publishPath}`;
  const result = await postWithRetry(targetUrl, body, headers, 4);

  await withGhaOutput({
    registry_publish_channel: channel,
    registry_publish_module_key: moduleKey,
    registry_publish_module_version: moduleVersion,
    registry_publish_auth_mode: "google-service-account-id-token",
  });

  console.log(
    "[notify-catalog-service] success",
    JSON.stringify({
      url: targetUrl,
      module_key: moduleKey,
      module_version: moduleVersion,
      channel,
      auth_mode: "google-service-account-id-token",
      result,
    }),
  );
}

main().catch((error) => {
  console.error("[notify-catalog-service] failed:", error instanceof Error ? error.message : error);
  process.exit(1);
});
