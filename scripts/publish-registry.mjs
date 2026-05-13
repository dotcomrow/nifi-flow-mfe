import fs from "node:fs/promises";
import { createHash } from "node:crypto";

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
    throw new Error(`Missing required value: ${name}`);
  }
  return normalized;
}

function toIsoNow() {
  return new Date().toISOString();
}

function sha256Hex(bytes) {
  return createHash("sha256").update(bytes).digest("hex");
}

function resolveModuleVersion() {
  const explicitVersion = asString(
    process.env.MODULE_VERSION || process.env.MFE_MODULE_VERSION || process.env.GITHUB_REF_NAME,
  );
  if (explicitVersion) {
    return explicitVersion;
  }

  const shortSha = asString(process.env.GITHUB_SHA).slice(0, 12);
  if (shortSha) {
    return `sha-${shortSha}`;
  }

  const timestamp = new Date().toISOString().replace(/[-:.TZ]/g, "").slice(0, 14);
  return `local-${timestamp}`;
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
  const moduleSeed = JSON.parse(await fs.readFile("directus/cms-module.seed.json", "utf8"));
  const moduleDefinition = JSON.parse(await fs.readFile("module.definition.json", "utf8"));
  const bundleBytes = await fs.readFile("dist/example-mfe.js");

  const moduleKey = ensureRequired(
    moduleSeed.module_key || moduleDefinition.module_key,
    "module_seed.module_key",
  );
  const moduleVersion = resolveModuleVersion();

  const releaseTag = asString(process.env.MFE_RELEASE_TAG || process.env.GITHUB_REF_NAME);
  const releaseSha = asString(process.env.MFE_RELEASE_SHA || process.env.GITHUB_SHA);
  const publishedAt = toIsoNow();
  const bundleSha256 = sha256Hex(bundleBytes);

  const publishManifest = {
    module_key: moduleKey,
    module_version: moduleVersion,
    published_at: publishedAt,
    provider: asString(moduleSeed.provider),
    component_type: asString(moduleSeed.component_type),
    release: {
      tag: releaseTag || null,
      sha: releaseSha || null,
    },
    bundle: {
      file_name: "example-mfe.js",
      sha256: bundleSha256,
      size_bytes: bundleBytes.byteLength,
    },
    definition: moduleDefinition,
    seed: moduleSeed,
  };

  await fs.writeFile("dist/module.publish.json", JSON.stringify(publishManifest, null, 2), "utf8");

  await withGhaOutput({
    registry_publish_manifest_path: "dist/module.publish.json",
    registry_publish_module_key: moduleKey,
    registry_publish_module_version: moduleVersion,
    registry_publish_bundle_sha256: bundleSha256,
  });

  console.log(
    "[publish-registry] prepared",
    JSON.stringify({
      module_key: moduleKey,
      module_version: moduleVersion,
      manifest_path: "dist/module.publish.json",
      bundle_sha256: bundleSha256,
    }),
  );
}

main().catch((error) => {
  console.error("[publish-registry] failed:", error instanceof Error ? error.message : error);
  process.exit(1);
});
