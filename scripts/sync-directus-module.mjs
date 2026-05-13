import fs from "node:fs/promises";
import path from "node:path";

function asString(value, fallback = "") {
  if (typeof value === "string") {
    return value.trim();
  }
  if (typeof value === "number") {
    return String(value);
  }
  return fallback;
}

function isNullish(value) {
  return value === undefined || value === null;
}

function stableJson(value) {
  return JSON.stringify(value);
}

async function loadSeedFile() {
  const seedPath = path.resolve(process.cwd(), "directus/cms-module.seed.json");
  const content = await fs.readFile(seedPath, "utf8");
  return JSON.parse(content);
}

function buildPayload(seed, env) {
  const payload = { ...seed };

  if (env.MODULE_STATUS) {
    payload.status = asString(env.MODULE_STATUS, payload.status || "published");
  }
  if (env.MODULE_REFRESH_TARGET) {
    payload.refresh_target = asString(
      env.MODULE_REFRESH_TARGET,
      payload.refresh_target || "both",
    );
  }
  if (Object.prototype.hasOwnProperty.call(env, "MODULE_SITE_KEY")) {
    const siteKey = asString(env.MODULE_SITE_KEY);
    payload.site_key = siteKey || null;
  }

  const bundleUrl = asString(env.MFE_BUNDLE_URL);
  const moduleVersion = asString(env.MFE_MODULE_VERSION || env.GITHUB_REF_NAME || "");
  const releaseTag = asString(env.GITHUB_REF_NAME || "");
  const releaseSha = asString(env.GITHUB_SHA || "");

  const defaultProps =
    payload.default_props && typeof payload.default_props === "object"
      ? { ...payload.default_props }
      : {};

  const releaseMeta = {
    bundleUrl,
    moduleVersion,
    releaseTag,
    releaseSha,
    publishedAt: new Date().toISOString(),
  };

  // Keep runtime metadata under a reserved key.
  defaultProps.__mfe_release = releaseMeta;
  payload.default_props = defaultProps;

  return payload;
}

async function directusRequest(baseUrl, token, method, resourcePath, body) {
  const response = await fetch(`${baseUrl.replace(/\/+$/g, "")}${resourcePath}`, {
    method,
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
    },
    body: body ? stableJson(body) : undefined,
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(
      `Directus ${method} ${resourcePath} failed (${response.status}): ${text || "empty response"}`,
    );
  }

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

async function findExistingModule(baseUrl, token, moduleKey, siteKey) {
  const query = new URLSearchParams();
  query.set("fields", "id,module_key,site_key,status,refresh_target");
  query.set("limit", "1");
  query.set("filter[module_key][_eq]", moduleKey);
  if (isNullish(siteKey) || siteKey === "") {
    query.set("filter[site_key][_null]", "true");
  } else {
    query.set("filter[site_key][_eq]", String(siteKey));
  }

  const response = await directusRequest(
    baseUrl,
    token,
    "GET",
    `/items/cms_modules?${query.toString()}`,
  );
  const rows = Array.isArray(response?.data) ? response.data : [];
  return rows[0] ?? null;
}

async function upsertModule(baseUrl, token, payload) {
  const moduleKey = asString(payload.module_key);
  if (!moduleKey) {
    throw new Error("Seed payload is missing module_key.");
  }

  const existing = await findExistingModule(baseUrl, token, moduleKey, payload.site_key);
  if (existing?.id) {
    const updated = await directusRequest(
      baseUrl,
      token,
      "PATCH",
      `/items/cms_modules/${encodeURIComponent(existing.id)}`,
      payload,
    );
    return { action: "updated", id: existing.id, data: updated?.data ?? null };
  }

  const created = await directusRequest(baseUrl, token, "POST", "/items/cms_modules", payload);
  return { action: "created", id: created?.data?.id ?? null, data: created?.data ?? null };
}

async function main() {
  const baseUrl = asString(process.env.DIRECTUS_BASE_URL);
  const token = asString(process.env.DIRECTUS_STATIC_TOKEN);
  if (!baseUrl || !token) {
    throw new Error(
      "DIRECTUS_BASE_URL and DIRECTUS_STATIC_TOKEN are required for Directus sync.",
    );
  }

  const seed = await loadSeedFile();
  const payload = buildPayload(seed, process.env);
  const result = await upsertModule(baseUrl, token, payload);

  const summary = {
    action: result.action,
    id: result.id,
    module_key: payload.module_key,
    site_key: payload.site_key ?? null,
    status: payload.status,
    refresh_target: payload.refresh_target,
    bundle_url: payload?.default_props?.__mfe_release?.bundleUrl ?? "",
    module_version: payload?.default_props?.__mfe_release?.moduleVersion ?? "",
  };

  console.log("[sync-directus-module] success", stableJson(summary));
}

main().catch((error) => {
  console.error("[sync-directus-module] failed:", error instanceof Error ? error.message : error);
  process.exit(1);
});
