import { context } from "esbuild";
import { asEsbuildDefines, readEnvironment } from "./env.mjs";

const mode = "local";
const env = readEnvironment(mode);
const define = asEsbuildDefines(env);
const port = Number.parseInt(env.MFE_PREVIEW_PORT || "4173", 10) || 4173;

const shared = {
  bundle: true,
  platform: "browser",
  target: "es2020",
  sourcemap: true,
  define,
};

const moduleContext = await context({
  ...shared,
  entryPoints: ["src/index.ts"],
  format: "iife",
  globalName: "NifiFlowMfeBundle",
  outfile: "dev-dist/nifi-flow-mfe.js",
});

const previewContext = await context({
  ...shared,
  entryPoints: ["preview/main.ts"],
  format: "iife",
  globalName: "NifiFlowMfePreview",
  outfile: "dev-dist/preview/main.js",
});

await Promise.all([moduleContext.watch(), previewContext.watch()]);

const serveResult = await previewContext.serve({
  servedir: ".",
  host: "0.0.0.0",
  port,
});

console.log("");
console.log("[nifi-flow-mfe] local preview running");
console.log(`  URL: http://localhost:${serveResult.port}/preview/`);
console.log("");

const shutdown = async () => {
  await Promise.all([moduleContext.dispose(), previewContext.dispose()]);
  process.exit(0);
};

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
