import {
  MODULE_COMPONENT_TYPE,
  MODULE_KEY,
  MODULE_PROVIDER,
  MODULE_VERSION,
} from "./constants";
import type { CmsModuleDefinition, CmsModuleMountContext, ModuleAsyncConfig } from "./contracts";
import { createModule, normalizeAsyncConfig, resolveChatProps } from "./module";

function asRecord(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return {};
  }
  return value as Record<string, unknown>;
}

function asString(value: unknown): string {
  if (typeof value === "string") {
    return value.trim();
  }
  if (typeof value === "number") {
    return String(value);
  }
  return "";
}

export function resolveHostProps(rawProps: unknown): Record<string, unknown> {
  const props = asRecord(rawProps);
  const asyncConfig = normalizeAsyncConfig(props.async);
  const normalized = resolveChatProps(props, asyncConfig);
  return {
    ...props,
    ...normalized,
    async: normalized.async,
  };
}

export function createCmsModuleDefinition(moduleKey = MODULE_KEY): CmsModuleDefinition {
  return {
    moduleKey,
    version: MODULE_VERSION,
    provider: MODULE_PROVIDER,
    componentType: MODULE_COMPONENT_TYPE,
    supportsAsyncConfig: true,
    resolveProps: resolveHostProps,
    mount: async (context: CmsModuleMountContext) => {
      const normalizedProps = resolveHostProps(context.props);
      const asyncConfig = normalizeAsyncConfig(
        asRecord(normalizedProps).async,
      ) as ModuleAsyncConfig;
      const runtime = createModule({
        moduleKey: context.moduleKey || moduleKey,
        instanceId:
          asString(normalizedProps.instanceId) || `${context.moduleKey || moduleKey}:${Date.now()}`,
        hostElement: context.element,
        props: normalizedProps,
        async: asyncConfig,
        signal: context.signal,
        environment: context.environment,
        emit: (event) => {
          context.element.dispatchEvent(
            new CustomEvent("cms-module-event", {
              bubbles: true,
              detail: event,
            }),
          );
        },
      });

      if (typeof runtime.init === "function") {
        await runtime.init({ props: normalizedProps });
      }
      await runtime.mount({ element: context.element });

      const onAbort = () => {
        void runtime.unmount();
      };
      context.signal.addEventListener("abort", onAbort, { once: true });

      return () => {
        context.signal.removeEventListener("abort", onAbort);
        void runtime.unmount();
      };
    },
  };
}

export const cmsModuleDefinition = createCmsModuleDefinition();

export function mount(context: CmsModuleMountContext) {
  return cmsModuleDefinition.mount(context);
}
