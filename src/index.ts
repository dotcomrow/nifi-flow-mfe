import { buildEnvDefaults } from "./build-env";
import { MODULE_KEY, MODULE_VERSION } from "./constants";
import { createCmsModuleDefinition, cmsModuleDefinition, mount, resolveHostProps } from "./host-adapter";
import { moduleDefinition } from "./manifest";
import { createModule, normalizeAsyncConfig, resolveChatProps } from "./module";

type MfeBuildInfo = {
  moduleKey: string;
  moduleVersion: string;
  buildVersion: string;
  buildCommit: string;
  buildTimestamp: string;
  buildMode: string;
};

type GlobalRegistryEntry = {
  moduleKey: string;
  createModule: typeof createModule;
  createCmsModuleDefinition: typeof createCmsModuleDefinition;
  cmsModuleDefinition: typeof cmsModuleDefinition;
  mount: typeof mount;
  moduleDefinition: typeof moduleDefinition;
  resolveHostProps: typeof resolveHostProps;
  resolveChatProps: typeof resolveChatProps;
  normalizeAsyncConfig: typeof normalizeAsyncConfig;
  buildInfo: MfeBuildInfo;
};

type GlobalScope = typeof globalThis & {
  SuncoastMfeRegistry?: Record<string, GlobalRegistryEntry>;
  __SUNCOAST_MFE_BUILD_INFO__?: Record<string, MfeBuildInfo>;
  __SUNCOAST_GET_MFE_BUILD_INFO__?: (
    moduleKey?: string,
  ) => MfeBuildInfo | Record<string, MfeBuildInfo> | null;
};

const globalScope = globalThis as GlobalScope;

const buildInfo: MfeBuildInfo = Object.freeze({
  moduleKey: MODULE_KEY,
  moduleVersion: MODULE_VERSION,
  buildVersion: buildEnvDefaults.buildVersion || MODULE_VERSION,
  buildCommit: buildEnvDefaults.buildCommit,
  buildTimestamp: buildEnvDefaults.buildTimestamp,
  buildMode: buildEnvDefaults.buildMode,
});

if (!globalScope.SuncoastMfeRegistry) {
  globalScope.SuncoastMfeRegistry = {};
}

globalScope.SuncoastMfeRegistry[MODULE_KEY] = {
  moduleKey: MODULE_KEY,
  createModule,
  createCmsModuleDefinition,
  cmsModuleDefinition,
  mount,
  moduleDefinition,
  resolveHostProps,
  resolveChatProps,
  normalizeAsyncConfig,
  buildInfo,
};

if (!globalScope.__SUNCOAST_MFE_BUILD_INFO__) {
  globalScope.__SUNCOAST_MFE_BUILD_INFO__ = {};
}
globalScope.__SUNCOAST_MFE_BUILD_INFO__[MODULE_KEY] = buildInfo;

if (typeof globalScope.__SUNCOAST_GET_MFE_BUILD_INFO__ !== "function") {
  globalScope.__SUNCOAST_GET_MFE_BUILD_INFO__ = (moduleKey) => {
    const registry = globalScope.__SUNCOAST_MFE_BUILD_INFO__ || {};
    if (typeof moduleKey === "string" && moduleKey.trim()) {
      const normalizedKey = moduleKey.trim();
      if (registry[normalizedKey]) {
        return registry[normalizedKey];
      }
      const caseInsensitiveMatch = Object.keys(registry).find(
        (key) => key.toLowerCase() === normalizedKey.toLowerCase(),
      );
      return caseInsensitiveMatch ? registry[caseInsensitiveMatch] : null;
    }
    return { ...registry };
  };
}

export {
  MODULE_KEY,
  createModule,
  createCmsModuleDefinition,
  cmsModuleDefinition,
  moduleDefinition,
  mount,
  normalizeAsyncConfig,
  resolveChatProps,
  resolveHostProps,
};
