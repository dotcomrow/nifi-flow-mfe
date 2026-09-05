import { readFile } from "node:fs/promises";
import { existsSync } from "node:fs";
import path from "node:path";

const DEFAULT_EXECUTOR_URL = "http://platform-deploy-executor.directus.svc.cluster.local:8080";

function asString(value, fallback = "") {
  if (typeof value === "string") {
    return value.trim();
  }
  if (typeof value === "number") {
    return String(value);
  }
  return fallback;
}

function asBoolean(value, fallback) {
  if (typeof value === "boolean") {
    return value;
  }
  const normalized = asString(value).toLowerCase();
  if (["1", "true", "yes", "on"].includes(normalized)) {
    return true;
  }
  if (["0", "false", "no", "off"].includes(normalized)) {
    return false;
  }
  return fallback;
}

function normalizeAuthorization(token) {
  const normalized = asString(token);
  if (/^(bearer|basic)\s+/i.test(normalized)) {
    return normalized;
  }
  return `Bearer ${normalized}`;
}

function normalizeEnvironments(value) {
  const entries = asString(value, "preview,production")
    .split(",")
    .map((entry) => entry.trim().toLowerCase())
    .filter(Boolean);
  const normalized = new Set();
  for (const entry of entries) {
    if (entry === "prod") {
      normalized.add("production");
    } else if (entry === "preview" || entry === "production") {
      normalized.add(entry);
    }
  }
  return [...normalized];
}

async function readJsonIfExists(filePath) {
  if (!existsSync(filePath)) {
    return {};
  }
  return JSON.parse(await readFile(filePath, "utf8"));
}

function sanitizePathSegment(value, fallback) {
  const normalized = asString(value)
    .toLowerCase()
    .replace(/[^a-z0-9._-]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 120);
  return normalized || fallback;
}

function resolveModuleVersion() {
  const explicitVersion = asString(
    process.env.OPENOBSERVE_SOURCEMAP_VERSION ||
      process.env.MODULE_VERSION ||
      process.env.MFE_MODULE_VERSION ||
      process.env.GITHUB_REF_NAME,
  );
  if (explicitVersion) {
    return explicitVersion;
  }

  const shortSha = asString(process.env.GITHUB_SHA).slice(0, 12);
  if (shortSha) {
    return `sha-${shortSha}`;
  }

  throw new Error("Unable to resolve OpenObserve source-map version.");
}

async function resolveModuleKey() {
  const explicit = asString(process.env.OPENOBSERVE_SOURCEMAP_SERVICE || process.env.MODULE_KEY);
  if (explicit) {
    return explicit;
  }

  const cwd = process.cwd();
  const moduleSeed = await readJsonIfExists(path.join(cwd, "directus/cms-module.seed.json"));
  const moduleDefinition = await readJsonIfExists(path.join(cwd, "module.definition.json"));
  return sanitizePathSegment(moduleSeed.module_key || moduleDefinition.module_key, "module");
}

async function postWithRetry(url, body, headers, retries = 3) {
  let attempt = 0;
  while (true) {
    attempt += 1;
    const response = await fetch(url, {
      body,
      headers,
      method: "POST",
    });
    const responseText = await response.text();

    if (response.ok) {
      return responseText ? JSON.parse(responseText) : {};
    }

    const retryable = response.status >= 500 || response.status === 429;
    if (!retryable || attempt > retries) {
      throw new Error(
        `OpenObserve source-map executor upload failed (${response.status}) after ${attempt} attempt(s): ${responseText || "empty response"}`,
      );
    }

    const backoffMs = Math.min(5000, 300 * 2 ** (attempt - 1));
    await new Promise((resolve) => setTimeout(resolve, backoffMs));
  }
}

async function main() {
  const enabled = asBoolean(process.env.OPENOBSERVE_SOURCEMAPS_ENABLED, true);
  const required = asBoolean(process.env.OPENOBSERVE_SOURCEMAPS_REQUIRED, false);
  if (!enabled) {
    console.log("[openobserve-sourcemaps] upload disabled");
    return;
  }

  const token = asString(
    process.env.PLATFORM_DEPLOY_EXECUTOR_TOKEN || process.env.OPENOBSERVE_SOURCEMAP_EXECUTOR_TOKEN,
  );
  if (!token) {
    const message = "PLATFORM_DEPLOY_EXECUTOR_TOKEN is not configured; skipping OpenObserve source-map upload.";
    if (required) {
      throw new Error(message);
    }
    console.log(`[openobserve-sourcemaps] ${message}`);
    return;
  }

  const archivePath = path.resolve(
    process.cwd(),
    asString(process.env.OPENOBSERVE_SOURCEMAP_ARCHIVE, "dist/openobserve-sourcemaps/sourcemaps.zip"),
  );
  if (!existsSync(archivePath)) {
    const message = `OpenObserve source-map archive does not exist: ${archivePath}`;
    if (required) {
      throw new Error(message);
    }
    console.log(`[openobserve-sourcemaps] ${message}; skipping upload.`);
    return;
  }

  const executorUrl = asString(
    process.env.PLATFORM_DEPLOY_EXECUTOR_URL || process.env.OPENOBSERVE_SOURCEMAP_EXECUTOR_URL,
    DEFAULT_EXECUTOR_URL,
  ).replace(/\/+$/g, "");
  const organization = asString(process.env.OPENOBSERVE_SOURCEMAP_ORG, "default");
  const service = await resolveModuleKey();
  const version = resolveModuleVersion();
  const environments = normalizeEnvironments(process.env.OPENOBSERVE_SOURCEMAP_ENVS);
  if (environments.length === 0) {
    throw new Error("OPENOBSERVE_SOURCEMAP_ENVS did not contain preview or production.");
  }

  const archive = await readFile(archivePath);
  const headers = {
    authorization: normalizeAuthorization(token),
    "content-type": "application/zip",
  };

  for (const env of environments) {
    const uploadUrl = new URL("/internal/openobserve/sourcemaps", executorUrl);
    uploadUrl.searchParams.set("organization", organization);
    uploadUrl.searchParams.set("service", service);
    uploadUrl.searchParams.set("env", env);
    uploadUrl.searchParams.set("version", version);

    const result = await postWithRetry(uploadUrl.toString(), archive, headers, 3);
    console.log(
      "[openobserve-sourcemaps] uploaded",
      JSON.stringify({
        env,
        service,
        version,
        result,
      }),
    );
  }
}

main().catch((error) => {
  console.error(
    "[openobserve-sourcemaps] failed:",
    error instanceof Error ? error.message : error,
  );
  process.exit(1);
});
