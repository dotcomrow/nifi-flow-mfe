import fs from "node:fs";
import path from "node:path";
import { execSync } from "node:child_process";
import { build } from "esbuild";
import { asEsbuildDefines, readEnvironment } from "./env.mjs";

function readPackageVersion() {
  try {
    const packageJsonPath = path.join(process.cwd(), "package.json");
    const raw = fs.readFileSync(packageJsonPath, "utf8");
    const parsed = JSON.parse(raw);
    return typeof parsed.version === "string" ? parsed.version.trim() : "";
  } catch {
    return "";
  }
}

function readGitCommit() {
  try {
    return execSync("git rev-parse --short=12 HEAD", {
      cwd: process.cwd(),
      stdio: ["ignore", "pipe", "ignore"],
      encoding: "utf8",
    }).trim();
  } catch {
    return "";
  }
}

const mode = process.env.MFE_BUILD_MODE || process.env.NODE_ENV || "production";
const env = readEnvironment(mode);
const buildCommit = (process.env.MFE_BUILD_COMMIT || process.env.GITHUB_SHA || readGitCommit())
  .trim()
  .slice(0, 12);
const buildTimestamp = new Date().toISOString();
const buildTimestampCompact = buildTimestamp.replace(/[-:TZ.]/g, "").slice(0, 14);
const moduleVersion = process.env.MFE_MODULE_VERSION || readPackageVersion() || "0.0.0";
const buildVersion =
  process.env.MFE_BUILD_VERSION ||
  `${moduleVersion}+b${buildTimestampCompact}${buildCommit ? `.${buildCommit}` : ""}`;

await build({
  entryPoints: ["src/index.ts"],
  bundle: true,
  platform: "browser",
  target: "es2020",
  format: "iife",
  globalName: "NifiFlowMfeBundle",
  outfile: "dist/nifi-flow-mfe.js",
  sourcemap: "external",
  minify: mode === "production",
  define: asEsbuildDefines(env, {
    version: buildVersion,
    commit: buildCommit,
    timestamp: buildTimestamp,
    mode,
  }),
});
