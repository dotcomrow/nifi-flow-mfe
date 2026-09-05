import { Buffer } from "node:buffer";
import { existsSync } from "node:fs";
import { mkdir, readFile, readdir, rm, stat, writeFile } from "node:fs/promises";
import path from "node:path";

const cwd = process.cwd();
const distDir = path.resolve(cwd, process.env.MFE_DIST_DIR || "dist");
const stagingDir = path.resolve(
  cwd,
  process.env.OPENOBSERVE_SOURCEMAP_STAGING_DIR || "dist/openobserve-sourcemaps",
);
const archivePath = path.resolve(
  cwd,
  process.env.OPENOBSERVE_SOURCEMAP_ARCHIVE || path.join(stagingDir, "sourcemaps.zip"),
);
const removePublicMaps = !/^(0|false|no|off)$/i.test(
  (process.env.OPENOBSERVE_SOURCEMAP_REMOVE_PUBLIC || "true").trim(),
);

const crc32Table = new Uint32Array(256);
for (let index = 0; index < crc32Table.length; index += 1) {
  let crc = index;
  for (let bit = 0; bit < 8; bit += 1) {
    crc = crc & 1 ? 0xedb88320 ^ (crc >>> 1) : crc >>> 1;
  }
  crc32Table[index] = crc >>> 0;
}

function asString(value, fallback = "") {
  if (typeof value === "string") {
    return value.trim();
  }
  if (typeof value === "number") {
    return String(value);
  }
  return fallback;
}

function crc32(buffer) {
  let crc = 0xffffffff;
  for (const byte of buffer) {
    crc = crc32Table[(crc ^ byte) & 0xff] ^ (crc >>> 8);
  }
  return (crc ^ 0xffffffff) >>> 0;
}

function dosDateTime(date) {
  const year = Math.max(1980, date.getFullYear());
  return {
    date: ((year - 1980) << 9) | ((date.getMonth() + 1) << 5) | date.getDate(),
    time: (date.getHours() << 11) | (date.getMinutes() << 5) | Math.floor(date.getSeconds() / 2),
  };
}

async function writeZipArchive(members) {
  const localParts = [];
  const centralParts = [];
  let offset = 0;

  for (const member of members) {
    const data = Buffer.isBuffer(member.data) ? member.data : Buffer.from(member.data, "utf8");
    const name = Buffer.from(member.name, "utf8");
    const checksum = crc32(data);
    const { date, time } = dosDateTime(member.mtime || new Date());
    const localHeader = Buffer.alloc(30 + name.byteLength);
    localHeader.writeUInt32LE(0x04034b50, 0);
    localHeader.writeUInt16LE(20, 4);
    localHeader.writeUInt16LE(0x0800, 6);
    localHeader.writeUInt16LE(0, 8);
    localHeader.writeUInt16LE(time, 10);
    localHeader.writeUInt16LE(date, 12);
    localHeader.writeUInt32LE(checksum, 14);
    localHeader.writeUInt32LE(data.byteLength, 18);
    localHeader.writeUInt32LE(data.byteLength, 22);
    localHeader.writeUInt16LE(name.byteLength, 26);
    localHeader.writeUInt16LE(0, 28);
    name.copy(localHeader, 30);

    const centralHeader = Buffer.alloc(46 + name.byteLength);
    centralHeader.writeUInt32LE(0x02014b50, 0);
    centralHeader.writeUInt16LE(20, 4);
    centralHeader.writeUInt16LE(20, 6);
    centralHeader.writeUInt16LE(0x0800, 8);
    centralHeader.writeUInt16LE(0, 10);
    centralHeader.writeUInt16LE(time, 12);
    centralHeader.writeUInt16LE(date, 14);
    centralHeader.writeUInt32LE(checksum, 16);
    centralHeader.writeUInt32LE(data.byteLength, 20);
    centralHeader.writeUInt32LE(data.byteLength, 24);
    centralHeader.writeUInt16LE(name.byteLength, 28);
    centralHeader.writeUInt16LE(0, 30);
    centralHeader.writeUInt16LE(0, 32);
    centralHeader.writeUInt16LE(0, 34);
    centralHeader.writeUInt16LE(0, 36);
    centralHeader.writeUInt32LE(0, 38);
    centralHeader.writeUInt32LE(offset, 42);
    name.copy(centralHeader, 46);

    localParts.push(localHeader, data);
    centralParts.push(centralHeader);
    offset += localHeader.byteLength + data.byteLength;
  }

  const centralDirectoryOffset = offset;
  const centralDirectory = Buffer.concat(centralParts);
  const endOfCentralDirectory = Buffer.alloc(22);
  endOfCentralDirectory.writeUInt32LE(0x06054b50, 0);
  endOfCentralDirectory.writeUInt16LE(0, 4);
  endOfCentralDirectory.writeUInt16LE(0, 6);
  endOfCentralDirectory.writeUInt16LE(members.length, 8);
  endOfCentralDirectory.writeUInt16LE(members.length, 10);
  endOfCentralDirectory.writeUInt32LE(centralDirectory.byteLength, 12);
  endOfCentralDirectory.writeUInt32LE(centralDirectoryOffset, 16);
  endOfCentralDirectory.writeUInt16LE(0, 20);

  await writeFile(archivePath, Buffer.concat([...localParts, centralDirectory, endOfCentralDirectory]));
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

function normalizeChannel(value) {
  return asString(value, "preview").toLowerCase() === "prod" ? "prod" : "preview";
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

  return `local-${new Date().toISOString().replace(/[-:.TZ]/g, "").slice(0, 14)}`;
}

async function resolveBundlePath() {
  const configured = asString(process.env.MODULE_PUBLISH_BUNDLE_PATH);
  if (configured) {
    return path.resolve(cwd, configured);
  }

  const entries = await readdir(distDir, { withFileTypes: true });
  const candidates = entries
    .filter((entry) => entry.isFile())
    .map((entry) => path.join(distDir, entry.name))
    .filter((filePath) => /\.js$/u.test(filePath) && !/\.js\.map$/u.test(filePath))
    .sort();

  if (candidates.length !== 1) {
    throw new Error(
      `Unable to resolve MFE bundle file. Expected one dist/*.js file, found ${candidates.length}.`,
    );
  }
  return candidates[0];
}

function stripSourceMappingUrl(source) {
  let next = source;
  let previous = "";
  while (next !== previous) {
    previous = next;
    next = next
      .replace(/(?:\r?\n)?\/\/# sourceMappingURL=[^\r\n]*(?:\r?\n)?$/u, "\n")
      .replace(/(?:\r?\n)?\/\*# sourceMappingURL=.*?\*\/(?:\r?\n)?$/su, "\n");
  }
  return next;
}

async function main() {
  const moduleSeed = await readJsonIfExists(path.join(cwd, "directus/cms-module.seed.json"));
  const moduleDefinition = await readJsonIfExists(path.join(cwd, "module.definition.json"));
  const moduleKey = sanitizePathSegment(
    moduleSeed.module_key || moduleDefinition.module_key,
    "module",
  );
  const moduleVersion = sanitizePathSegment(resolveModuleVersion(), "version");
  const channel = normalizeChannel(process.env.MODULE_CHANNEL);
  const bundlePath = await resolveBundlePath();
  const mapPath = `${bundlePath}.map`;

  if (!existsSync(mapPath)) {
    throw new Error(`No source map found for ${path.relative(cwd, bundlePath)}.`);
  }

  const bundleStats = await stat(bundlePath);
  const mapStats = await stat(mapPath);
  const originalBundle = await readFile(bundlePath, "utf8");
  const publicBundle = stripSourceMappingUrl(originalBundle);
  const archiveBundle = `${publicBundle.replace(/\s+$/u, "")}\n//# sourceMappingURL=bundle.js.map\n`;
  const sourceMap = JSON.parse(await readFile(mapPath, "utf8"));
  sourceMap.file = "bundle.js";
  const archiveMap = `${JSON.stringify(sourceMap)}\n`;
  const archivePrefix = `assets/modules/${channel}/${moduleKey}/${moduleVersion}`;

  await mkdir(stagingDir, { recursive: true });
  await rm(archivePath, { force: true });
  await writeZipArchive([
    {
      data: Buffer.from(archiveBundle, "utf8"),
      mtime: bundleStats.mtime,
      name: `${archivePrefix}/bundle.js`,
    },
    {
      data: Buffer.from(archiveMap, "utf8"),
      mtime: mapStats.mtime,
      name: `${archivePrefix}/bundle.js.map`,
    },
  ]);

  const metadata = {
    archive: path.relative(cwd, archivePath),
    channel,
    generatedAt: new Date().toISOString(),
    moduleKey,
    moduleVersion,
    publicBundle: path.relative(cwd, bundlePath),
    publicMapRemoved: removePublicMaps,
    sourceMap: path.relative(cwd, mapPath),
    zipEntries: [`${archivePrefix}/bundle.js`, `${archivePrefix}/bundle.js.map`],
  };
  await writeFile(path.join(stagingDir, "metadata.json"), `${JSON.stringify(metadata, null, 2)}\n`);

  if (removePublicMaps) {
    if (publicBundle !== originalBundle) {
      await writeFile(bundlePath, publicBundle, "utf8");
    }
    await rm(mapPath, { force: true });
  }

  console.log(
    `[openobserve-sourcemaps] staged ${path.relative(cwd, archivePath)} module=${moduleKey} version=${moduleVersion} channel=${channel} removed_public_map=${removePublicMaps}`,
  );
}

main().catch((error) => {
  console.error(
    "[openobserve-sourcemaps] failed:",
    error instanceof Error ? error.message : error,
  );
  process.exit(1);
});
