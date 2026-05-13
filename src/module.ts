import { createClient } from "graphql-ws";
import { MODULE_KEY } from "./constants";
import type {
  JsonObject,
  JsonValue,
  ModuleAsyncConfig,
  ModuleContext,
  ModuleEventEnvelope,
  ModuleFactory,
  ModuleRuntime,
} from "./contracts";

type GraphqlConfig = {
  httpUrl: string;
  wsUrl: string;
  authToken: string;
  tokenExchange: GraphqlTokenExchangeConfig;
  submitMutation: string;
  submitVariables: JsonValue;
  submitRequestIdPath: string;
  streamSubscription: string;
  streamVariables: JsonValue;
  streamTextPath: string;
  streamDonePath: string;
  streamErrorPath: string;
  streamChunkMode: "append" | "replace";
  conversationId: string;
};

type GraphqlTokenExchangeConfig = {
  enabled: boolean;
  requestedAudience: string;
  requestedAudiences: string[];
  requestedScope: string;
  tokenUrl: string;
  clientId: string;
  exchangeUrl: string;
  appSlug: string;
};

type ChatProps = {
  title: string;
  inputPlaceholder: string;
  parametersLabel: string;
  parametersPlaceholder: string;
  defaultParametersJson: string;
  resultLabel: string;
  submitLabel: string;
  assistantLabel: string;
  maxMessages: number;
  requestCommand: string;
  async: ModuleAsyncConfig;
  graphql: GraphqlConfig;
};

type TemplateVariables = Record<string, JsonValue>;

const defaultAsyncConfig: ModuleAsyncConfig = {
  enabled: true,
  mode: "kafka-graphql-bridge",
  requestChannel: "graphql.async.requests.v1",
  responseChannel: "graphql.async.responses.v1",
  correlationIdPath: "publish_async_request.request_id",
  request: {
    supported: true,
    defaultTimeoutMs: 30000,
    retry: {
      maxAttempts: 2,
      initialBackoffMs: 250,
      maxBackoffMs: 2000,
      jitter: true,
    },
  },
  stream: {
    supported: true,
    transport: "graphql-ws",
    endpointRef: "GRAPHQL_STREAM_ENDPOINT",
    authMode: "inherit",
    reconnect: {
      maxAttempts: 10,
      initialBackoffMs: 500,
      maxBackoffMs: 10000,
      jitter: true,
    },
  },
  queue: {
    supported: true,
    maxInflight: 64,
    dropPolicy: "backpressure",
    orderingKey: "instance_id",
  },
};

const defaultSubmitMutation = `
mutation PublishAsyncRequest($input: json!) {
  publish_async_request(input: $input)
}
`.trim();

const defaultStreamSubscription = `
subscription StreamClientAsyncMessage($requestId: String!, $responseChannel: String!) {
  graphql_client_async_messages(
    where: {
      _and: [
        { request_id: { _eq: $requestId } }
        { kafka_topic: { _eq: $responseChannel } }
      ]
    }
    order_by: { updated_at: desc }
    limit: 1
  ) {
    request_id
    status
    response_payload
    error_payload
    completed_at
    updated_at
  }
}
`.trim();

const RUNTIME_TOKEN_STORAGE_KEYS = [
  "suncoast.auth.access_token",
  "suncoast.auth.token",
  "suncoast.auth.bearer_token",
  "mfe.preview.authToken",
  "access_token",
] as const;

const TOKEN_EXCHANGE_GRANT_TYPE = "urn:ietf:params:oauth:grant-type:token-exchange";
const ACCESS_TOKEN_TYPE_URN = "urn:ietf:params:oauth:token-type:access_token";
const TOKEN_EXCHANGE_EXPIRY_LEEWAY_MS = 30_000;
const AUTH_LOGOUT_EVENTS = ["auth-logout", "suncoast-auth-logout"] as const;

type RuntimeGraphqlConfig = {
  httpUrl: string;
  wsUrl: string;
  authToken: string;
};

type RuntimeTokenExchangeDefaults = {
  tokenUrl: string;
  clientId: string;
  exchangeUrl: string;
  appSlug: string;
};

type TokenExchangeCacheEntry = {
  sourceToken: string;
  tokenUrl: string;
  clientId: string;
  exchangeUrl: string;
  appSlug: string;
  requestedAudienceKey: string;
  requestedScope: string;
  exchangedToken: string;
  expiresAt: number | null;
};

const THEME_COLOR = {
  text: "var(--cms-ui-text, var(--cms-text, #0f172a))",
  muted: "var(--cms-ui-muted, var(--cms-muted, #64748b))",
  surface: "var(--cms-ui-surface, var(--cms-surface, #ffffff))",
  elevated: "var(--cms-ui-elevated, #f8fafc)",
  border: "var(--cms-ui-border, var(--cms-border, #cbd5e1))",
  accent: "var(--cms-ui-accent, var(--cms-accent, #1d4ed8))",
  onAccent: "var(--cms-on-accent, #ffffff)",
} as const;

function asRecord(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return {};
  }
  return value as Record<string, unknown>;
}

function asString(value: unknown, fallback = ""): string {
  if (typeof value === "string") {
    return value;
  }
  if (typeof value === "number") {
    return String(value);
  }
  return fallback;
}

function firstNonEmpty(...values: string[]): string {
  for (const value of values) {
    const normalized = value.trim();
    if (normalized) {
      return normalized;
    }
  }
  return "";
}

function decodeBase64UrlToString(value: string): string {
  const normalized = value.replace(/-/g, "+").replace(/_/g, "/");
  const paddingLength = (4 - (normalized.length % 4)) % 4;
  const padded = `${normalized}${"=".repeat(paddingLength)}`;
  try {
    const binary = atob(padded);
    const bytes = Uint8Array.from(binary, (entry) => entry.charCodeAt(0));
    if (typeof TextDecoder !== "undefined") {
      return new TextDecoder().decode(bytes);
    }
    let fallback = "";
    bytes.forEach((byte) => {
      fallback += String.fromCharCode(byte);
    });
    return fallback;
  } catch {
    return "";
  }
}

function parseJwtPayload(token: string): Record<string, unknown> {
  const normalized = token.trim();
  if (!normalized) {
    return {};
  }
  const parts = normalized.split(".");
  if (parts.length < 2) {
    return {};
  }
  const decoded = decodeBase64UrlToString(parts[1] || "");
  if (!decoded) {
    return {};
  }
  try {
    const parsed = JSON.parse(decoded) as unknown;
    return asRecord(parsed);
  } catch {
    return {};
  }
}

function parseJwtExpiryMs(token: string): number | null {
  const payload = parseJwtPayload(token);
  const exp = payload.exp;
  if (typeof exp === "number" && Number.isFinite(exp) && exp > 0) {
    return exp * 1000;
  }
  if (typeof exp === "string") {
    const parsed = Number.parseInt(exp, 10);
    if (Number.isFinite(parsed) && parsed > 0) {
      return parsed * 1000;
    }
  }
  return null;
}

function tokenHasAudience(token: string, audience: string): boolean {
  const requestedAudience = audience.trim();
  if (!requestedAudience) {
    return false;
  }
  const payload = parseJwtPayload(token);
  const aud = payload.aud;
  if (typeof aud === "string") {
    return aud.trim() === requestedAudience;
  }
  if (Array.isArray(aud)) {
    return aud.some((entry) => asString(entry).trim() === requestedAudience);
  }
  return false;
}

function tokenHasAllAudiences(token: string, audiences: string[]): boolean {
  const normalized = normalizeAudienceValues("", audiences);
  if (normalized.length === 0) {
    return false;
  }
  return normalized.every((audience) => tokenHasAudience(token, audience));
}

function normalizeAudienceValues(singleAudience: string, audiences: string[]): string[] {
  const out: string[] = [];
  const seen = new Set<string>();

  const push = (value: string) => {
    const normalized = value.trim();
    if (!normalized || seen.has(normalized)) {
      return;
    }
    seen.add(normalized);
    out.push(normalized);
  };

  push(singleAudience);
  for (const value of audiences) {
    push(value);
  }

  return out;
}

function parseStringArray(value: unknown): string[] {
  const normalize = (entries: unknown[]): string[] => {
    return entries
      .map((entry) => asString(entry).trim())
      .filter(Boolean);
  };

  if (Array.isArray(value)) {
    return normalize(value);
  }

  if (typeof value === "string") {
    const trimmed = value.trim();
    if (!trimmed) {
      return [];
    }

    if (trimmed.startsWith("[")) {
      try {
        const parsed = JSON.parse(trimmed) as unknown;
        if (Array.isArray(parsed)) {
          return normalize(parsed);
        }
      } catch {
        // Fall through to comma/newline split.
      }
    }

    return trimmed
      .split(/[\n,]/g)
      .map((entry) => entry.trim())
      .filter(Boolean);
  }

  return [];
}

function deriveTokenEndpointFromTokenIssuer(token: string): string {
  const payload = parseJwtPayload(token);
  const issuer = asString(payload.iss).trim().replace(/\/+$/g, "");
  if (!issuer) {
    return "";
  }
  if (issuer.endsWith("/protocol/openid-connect")) {
    return `${issuer}/token`;
  }
  return `${issuer}/protocol/openid-connect/token`;
}

function readTokenFromStorage(): string {
  if (typeof window === "undefined") {
    return "";
  }

  const read = (storage: Storage | undefined): string => {
    if (!storage) {
      return "";
    }
    for (const key of RUNTIME_TOKEN_STORAGE_KEYS) {
      try {
        const value = storage.getItem(key);
        const normalized = asString(value).trim();
        if (normalized) {
          return normalized;
        }
      } catch {
        continue;
      }
    }
    return "";
  };

  const readSafely = (pickStorage: () => Storage): string => {
    try {
      return read(pickStorage());
    } catch {
      return "";
    }
  };

  return (
    readSafely(() => window.sessionStorage) ||
    readSafely(() => window.localStorage)
  );
}

function readGraphqlFromWindow(): RuntimeGraphqlConfig {
  if (typeof window === "undefined") {
    return { httpUrl: "", wsUrl: "", authToken: "" };
  }

  const runtime = asRecord((window as Window & { __SUNCOAST_RUNTIME__?: unknown }).__SUNCOAST_RUNTIME__);
  const runtimeGraphql = asRecord(runtime.graphql);

  const globalAuth = asRecord((window as Window & { __SUNCOAST_AUTH__?: unknown }).__SUNCOAST_AUTH__);
  const authTokenFromGetter = (() => {
    const getter = globalAuth.getAccessToken;
    if (typeof getter !== "function") {
      return "";
    }
    try {
      return asString(getter()).trim();
    } catch {
      return "";
    }
  })();

  return {
    httpUrl: asString(runtimeGraphql.httpUrl).trim(),
    wsUrl: asString(runtimeGraphql.wsUrl).trim(),
    authToken:
      firstNonEmpty(
        asString(runtimeGraphql.authToken),
        authTokenFromGetter,
        asString(globalAuth.accessToken),
      ) || readTokenFromStorage(),
  };
}

function readTokenExchangeDefaults(): RuntimeTokenExchangeDefaults {
  if (typeof window === "undefined") {
    return { tokenUrl: "", clientId: "", exchangeUrl: "", appSlug: "" };
  }

  const root = document.getElementById("cms-root");
  const tokenUrlFromDom = asString(root?.dataset.authTokenUrl).trim();
  const clientIdFromDom = asString(root?.dataset.authClientId).trim();
  const gatewayUrlFromDom = asString(root?.dataset.authGatewayUrl).trim();
  const gatewayExchangePathFromDom = asString(root?.dataset.authGatewayExchangePath).trim();
  const gatewayAppSlugFromDom = asString(root?.dataset.authGatewayAppSlug).trim();

  const globalAuth = asRecord((window as Window & { __SUNCOAST_AUTH__?: unknown }).__SUNCOAST_AUTH__);
  const authConfig = asRecord(globalAuth.config);
  const gatewayUrl = firstNonEmpty(
    asString(authConfig.gatewayUrl).trim(),
    gatewayUrlFromDom,
  );
  const gatewayExchangePath = firstNonEmpty(
    asString(authConfig.gatewayExchangePath).trim(),
    gatewayExchangePathFromDom,
    "/v1/auth/token-exchange",
  );
  const exchangeUrl = (() => {
    if (!gatewayUrl) {
      return "";
    }
    try {
      return new URL(gatewayExchangePath, gatewayUrl).toString();
    } catch {
      return "";
    }
  })();

  return {
    tokenUrl: firstNonEmpty(asString(authConfig.tokenUrl), tokenUrlFromDom),
    clientId: firstNonEmpty(asString(authConfig.clientId), clientIdFromDom),
    exchangeUrl,
    appSlug: firstNonEmpty(
      asString(authConfig.gatewayAppSlug).trim(),
      gatewayAppSlugFromDom,
    ),
  };
}

function readGraphqlFromDom(): RuntimeGraphqlConfig {
  if (typeof document === "undefined") {
    return { httpUrl: "", wsUrl: "", authToken: "" };
  }

  const root = document.getElementById("cms-root");
  if (!(root instanceof HTMLElement)) {
    return { httpUrl: "", wsUrl: "", authToken: "" };
  }

  return {
    httpUrl: asString(root.dataset.graphqlHttpUrl).trim(),
    wsUrl: asString(root.dataset.graphqlWsUrl).trim(),
    authToken: asString(root.dataset.graphqlAuthToken).trim(),
  };
}

function toDisplayText(value: unknown, depth = 0): string {
  if (depth > 8) {
    return "";
  }

  if (typeof value === "string") {
    return value;
  }
  if (typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }
  if (Array.isArray(value)) {
    for (const entry of value) {
      const nested = toDisplayText(entry, depth + 1).trim();
      if (nested) {
        return nested;
      }
    }
    return "";
  }
  if (!value || typeof value !== "object") {
    return "";
  }

  const record = value as Record<string, unknown>;
  const candidateKeys = [
    // Primary payload keys returned by async response writer.
    "answer_text",
    "answerText",
    // Common generic payload keys.
    "text",
    "content",
    "message",
    "response",
    "answer",
    "output",
    "value",
  ];
  for (const key of candidateKeys) {
    const nested = toDisplayText(record[key], depth + 1).trim();
    if (nested) {
      return nested;
    }
  }

  // OpenAI-compatible fallback structures.
  const nestedCandidates = [
    record.raw,
    record.delta,
    record.choice,
    record.choices,
    record.payload,
    record.data,
  ];
  for (const candidate of nestedCandidates) {
    const nested = toDisplayText(candidate, depth + 1).trim();
    if (nested) {
      return nested;
    }
  }

  return "";
}

function describeUnknownError(error: unknown, depth = 0): string {
  if (depth > 6) {
    return "";
  }
  if (error instanceof Error) {
    return error.message.trim();
  }
  if (typeof error === "string") {
    return error.trim();
  }
  if (typeof error === "number" || typeof error === "boolean") {
    return String(error);
  }
  if (Array.isArray(error)) {
    const messages = error
      .map((entry) => describeUnknownError(entry, depth + 1).trim())
      .filter(Boolean);
    if (messages.length > 0) {
      return messages.join("; ");
    }
    return "";
  }
  if (!error || typeof error !== "object") {
    return "";
  }

  const record = asRecord(error);
  const explicitKeys = [
    "message",
    "error",
    "reason",
    "description",
    "details",
  ];
  for (const key of explicitKeys) {
    const nested = describeUnknownError(record[key], depth + 1).trim();
    if (nested) {
      return nested;
    }
  }

  const maybeCode = asString(record.code).trim();
  const maybeReason = asString(record.reason).trim();
  const maybeType = asString(record.type).trim();
  if (maybeCode || maybeReason) {
    return [maybeType || "stream", maybeCode, maybeReason].filter(Boolean).join(" ");
  }

  const fallbackText = toDisplayText(record, depth + 1).trim();
  if (fallbackText) {
    return fallbackText;
  }

  try {
    const serialized = JSON.stringify(record);
    return serialized.length > 240 ? `${serialized.slice(0, 237)}...` : serialized;
  } catch {
    return "";
  }
}

function asInteger(value: unknown, fallback: number, min: number, max: number): number {
  if (typeof value === "number" && Number.isFinite(value)) {
    return Math.max(min, Math.min(max, Math.round(value)));
  }
  if (typeof value === "string") {
    const parsed = Number.parseInt(value, 10);
    if (Number.isFinite(parsed)) {
      return Math.max(min, Math.min(max, parsed));
    }
  }
  return fallback;
}

function asBoolean(value: unknown, fallback = false): boolean {
  if (typeof value === "boolean") {
    return value;
  }
  if (typeof value === "string") {
    const normalized = value.trim().toLowerCase();
    if (["1", "true", "yes", "on"].includes(normalized)) {
      return true;
    }
    if (["0", "false", "no", "off"].includes(normalized)) {
      return false;
    }
  }
  return fallback;
}

function asAsyncMode(value: unknown): ModuleAsyncConfig["mode"] {
  const normalized = asString(value).trim().toLowerCase();
  if (
    normalized === "none" ||
    normalized === "request-response" ||
    normalized === "subscribe" ||
    normalized === "mixed" ||
    normalized === "graphql-stream" ||
    normalized === "kafka-graphql-bridge"
  ) {
    return normalized;
  }
  return "none";
}

function normalizeRetryPolicy(value: unknown, fallback: ModuleAsyncConfig["request"]["retry"]) {
  const record = asRecord(value);
  return {
    maxAttempts: asInteger(record.maxAttempts, fallback.maxAttempts, 0, 100),
    initialBackoffMs: asInteger(record.initialBackoffMs, fallback.initialBackoffMs, 0, 600000),
    maxBackoffMs: asInteger(record.maxBackoffMs, fallback.maxBackoffMs, 0, 600000),
    jitter: asBoolean(record.jitter, fallback.jitter),
  };
}

export function normalizeAsyncConfig(
  value: unknown,
  inherited?: ModuleAsyncConfig,
): ModuleAsyncConfig {
  const base = inherited ?? defaultAsyncConfig;
  const record = asRecord(value);
  const request = asRecord(record.request);
  const stream = asRecord(record.stream);
  const queue = asRecord(record.queue);
  const mode = asAsyncMode(record.mode || base.mode);
  const streamTransportFallback = mode === "kafka-graphql-bridge" ? "kafka-bridge" : base.stream.transport;

  return {
    enabled: asBoolean(record.enabled, base.enabled),
    mode,
    requestChannel: asString(
      record.requestChannel || record.request_channel,
      base.requestChannel,
    ).trim(),
    responseChannel: asString(
      record.responseChannel || record.response_channel,
      base.responseChannel,
    ).trim(),
    correlationIdPath: asString(
      record.correlationIdPath || record.correlation_id_path,
      base.correlationIdPath,
    ).trim(),
    request: {
      supported: asBoolean(request.supported, base.request.supported),
      defaultTimeoutMs: asInteger(
        request.defaultTimeoutMs,
        base.request.defaultTimeoutMs,
        1,
        600000,
      ),
      retry: normalizeRetryPolicy(request.retry, base.request.retry),
    },
    stream: {
      supported: asBoolean(stream.supported, base.stream.supported),
      transport: ((): ModuleAsyncConfig["stream"]["transport"] => {
        const transport = asString(stream.transport, streamTransportFallback).toLowerCase();
        if (
          transport === "none" ||
          transport === "graphql-ws" ||
          transport === "sse" ||
          transport === "webhook" ||
          transport === "kafka-bridge"
        ) {
          return transport;
        }
        return base.stream.transport;
      })(),
      endpointRef: asString(stream.endpointRef, base.stream.endpointRef),
      authMode: ((): ModuleAsyncConfig["stream"]["authMode"] => {
        const mode = asString(stream.authMode, base.stream.authMode).toLowerCase();
        if (mode === "inherit" || mode === "token" || mode === "mtls" || mode === "none") {
          return mode;
        }
        return base.stream.authMode;
      })(),
      reconnect: normalizeRetryPolicy(stream.reconnect, base.stream.reconnect),
    },
    queue: {
      supported: asBoolean(queue.supported, base.queue.supported),
      maxInflight: asInteger(queue.maxInflight, base.queue.maxInflight, 1, 10000),
      dropPolicy: ((): ModuleAsyncConfig["queue"]["dropPolicy"] => {
        const policy = asString(queue.dropPolicy, base.queue.dropPolicy).toLowerCase();
        if (policy === "backpressure" || policy === "drop-oldest" || policy === "drop-newest") {
          return policy;
        }
        return base.queue.dropPolicy;
      })(),
      orderingKey: asString(queue.orderingKey, base.queue.orderingKey),
    },
  };
}

function toJsonValue(value: unknown): JsonValue {
  if (value === null) {
    return null;
  }
  if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
    return value;
  }
  if (Array.isArray(value)) {
    return value.map((entry) => toJsonValue(entry));
  }
  if (typeof value === "object") {
    const record = value as Record<string, unknown>;
    const output: JsonObject = {};
    for (const [key, entry] of Object.entries(record)) {
      output[key] = toJsonValue(entry);
    }
    return output;
  }
  return String(value);
}

function toTemplateString(value: JsonValue): string {
  if (value === null) {
    return "";
  }
  if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }
  try {
    return JSON.stringify(value);
  } catch {
    return "";
  }
}

function parseTemplateValue(value: unknown): JsonValue {
  if (value === null) {
    return null;
  }
  if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
    return value;
  }
  if (Array.isArray(value)) {
    return value.map((entry) => parseTemplateValue(entry));
  }
  if (typeof value === "object") {
    const record = value as Record<string, unknown>;
    const out: JsonObject = {};
    for (const [key, entry] of Object.entries(record)) {
      out[key] = parseTemplateValue(entry);
    }
    return out;
  }
  return String(value);
}

function applyTemplate(value: JsonValue, variables: TemplateVariables): JsonValue {
  if (typeof value === "string") {
    const wholePlaceholder = value.match(/^\{\{\s*([a-zA-Z0-9_.-]+)\s*\}\}$/);
    if (wholePlaceholder) {
      const key = wholePlaceholder[1] || "";
      return key in variables ? variables[key] ?? "" : "";
    }
    return value.replace(/\{\{\s*([a-zA-Z0-9_.-]+)\s*\}\}/g, (_, key: string) => {
      const replacement = key in variables ? variables[key] : "";
      return toTemplateString(replacement ?? "");
    });
  }
  if (Array.isArray(value)) {
    return value.map((entry) => applyTemplate(entry, variables));
  }
  if (value && typeof value === "object") {
    const out: JsonObject = {};
    for (const [key, entry] of Object.entries(value)) {
      out[key] = applyTemplate(entry, variables);
    }
    return out;
  }
  return value;
}

function parseParametersJson(value: string): JsonObject {
  const trimmed = value.trim();
  if (!trimmed) {
    return {};
  }
  const parsed = JSON.parse(trimmed) as unknown;
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new Error("Parameters must be a JSON object.");
  }
  return toJsonValue(parsed) as JsonObject;
}

function buildParameterTemplateVariables(parameters: JsonObject): TemplateVariables {
  const out: TemplateVariables = {
    parameters,
    parametersJson: toTemplateString(parameters),
  };
  for (const [key, value] of Object.entries(parameters)) {
    out[`param.${key}`] = value;
  }
  return out;
}

function asPathParts(path: string): string[] {
  return path
    .split(".")
    .map((part) => part.trim())
    .filter(Boolean);
}

function getPathValue(data: unknown, path: string): unknown {
  const parts = asPathParts(path);
  let current: unknown = data;
  for (const part of parts) {
    if (Array.isArray(current)) {
      const index = Number.parseInt(part, 10);
      if (!Number.isFinite(index)) {
        return undefined;
      }
      current = current[index];
      continue;
    }
    if (!current || typeof current !== "object") {
      return undefined;
    }
    current = (current as Record<string, unknown>)[part];
  }
  return current;
}

function resolveRequestIdFromPaths(
  payload: unknown,
  candidatePaths: string[],
): { requestId: string; path: string } {
  for (const candidatePath of candidatePaths) {
    const path = asString(candidatePath).trim();
    if (!path) {
      continue;
    }
    const requestId = asString(getPathValue(payload, path)).trim();
    if (requestId) {
      return { requestId, path };
    }
  }
  return { requestId: "", path: "" };
}

function formatAsyncChannelSummary(config: ModuleAsyncConfig): string {
  const requestChannel = asString(config.requestChannel).trim();
  const responseChannel = asString(config.responseChannel).trim();
  if (!requestChannel && !responseChannel) {
    return "";
  }
  const parts: string[] = [];
  if (requestChannel) {
    parts.push(`req:${requestChannel}`);
  }
  if (responseChannel) {
    parts.push(`resp:${responseChannel}`);
  }
  return parts.join(" / ");
}

function emitEvent(ctx: ModuleContext, type: string, payload: JsonObject) {
  if (!ctx.emit) {
    return;
  }
  const event: ModuleEventEnvelope = {
    type,
    sourceModuleKey: ctx.moduleKey || MODULE_KEY,
    instanceId: ctx.instanceId,
    payload,
    timestamp: new Date().toISOString(),
  };
  ctx.emit(event);
}

function normalizeTokenExchangeConfig(value: unknown): GraphqlTokenExchangeConfig {
  const record = asRecord(value);
  const hasConfig = Object.keys(record).length > 0;

  const requestedAudience = asString(record.requestedAudience || record.requested_audience).trim();
  const requestedAudiences = normalizeAudienceValues(
    requestedAudience,
    parseStringArray(record.requestedAudiences || record.requested_audiences),
  );
  const requestedScope = asString(record.requestedScope || record.requested_scope).trim();
  const enabled = hasConfig ? asBoolean(record.enabled, Boolean(requestedAudiences.length || requestedScope)) : false;

  return {
    enabled,
    requestedAudience,
    requestedAudiences,
    requestedScope,
    tokenUrl: asString(record.tokenUrl || record.token_url).trim(),
    clientId: asString(record.clientId || record.client_id).trim(),
    exchangeUrl: asString(record.exchangeUrl || record.exchange_url).trim(),
    appSlug: asString(record.appSlug || record.app_slug).trim(),
  };
}

function normalizeGraphqlConfig(rawProps: Record<string, unknown>, asyncConfig: ModuleAsyncConfig): GraphqlConfig {
  const graphql = asRecord(rawProps.graphql);
  const chunkModeRaw = asString(graphql.streamChunkMode || graphql.stream_chunk_mode || "append")
    .trim()
    .toLowerCase();
  const submitRequestIdPath = firstNonEmpty(
    asString(graphql.submitRequestIdPath || graphql.submit_request_id_path).trim(),
    asString(asyncConfig.correlationIdPath).trim(),
    "publish_async_request.request_id",
  );

  return {
    httpUrl: asString(graphql.httpUrl || graphql.http_url).trim(),
    wsUrl: asString(graphql.wsUrl || graphql.ws_url).trim(),
    authToken: asString(graphql.authToken || graphql.auth_token).trim(),
    tokenExchange: normalizeTokenExchangeConfig(
      graphql.tokenExchange || graphql.token_exchange,
    ),
    submitMutation: asString(graphql.submitMutation, defaultSubmitMutation).trim() || defaultSubmitMutation,
    submitVariables: parseTemplateValue(
      graphql.submitVariables ?? {
        input: {
          handler: "batch-dataflow",
          operation: "nifi.flow.invoke",
          payload: {
            message: "{{prompt}}",
            parameters: "{{parameters}}",
            conversationId: "{{conversationId}}",
          },
          metadata: {
            moduleKey: "{{moduleKey}}",
            instanceId: "{{instanceId}}",
            source: "{{source}}",
            cacheKey: "{{cacheKey}}",
            contentHash: "{{contentHash}}",
            asyncMode: "{{asyncMode}}",
            requestChannel: "{{requestChannel}}",
            responseChannel: "{{responseChannel}}",
            correlationIdPath: "{{correlationIdPath}}",
          },
          expires_in_seconds: 86400,
        },
      },
    ),
    submitRequestIdPath,
    streamSubscription:
      asString(graphql.streamSubscription, defaultStreamSubscription).trim() ||
      defaultStreamSubscription,
    streamVariables: parseTemplateValue(
      graphql.streamVariables ?? {
        requestId: "{{requestId}}",
        responseChannel: "{{responseChannel}}",
      },
    ),
    streamTextPath:
      asString(graphql.streamTextPath, "graphql_client_async_messages.0.response_payload").trim() ||
      "graphql_client_async_messages.0.response_payload",
    streamDonePath:
      asString(graphql.streamDonePath, "graphql_client_async_messages.0.status").trim() ||
      "graphql_client_async_messages.0.status",
    streamErrorPath:
      asString(graphql.streamErrorPath, "graphql_client_async_messages.0.error_payload").trim() ||
      "graphql_client_async_messages.0.error_payload",
    streamChunkMode: chunkModeRaw === "append" ? "append" : "replace",
    conversationId: asString(graphql.conversationId || graphql.conversation_id).trim(),
  };
}

export function resolveChatProps(rawProps: unknown, inheritedAsync?: ModuleAsyncConfig): ChatProps {
  const props = asRecord(rawProps);
  const defaultTitle = "NiFi Flow Runner";
  const defaultInput = "Enter message payload...";
  const defaultParametersLabel = "Parameters (JSON object)";
  const defaultParametersPlaceholder = "{\n  \"tenant\": \"internal\",\n  \"priority\": \"normal\"\n}";
  const defaultParametersJson = "{\n  \"tenant\": \"internal\",\n  \"priority\": \"normal\"\n}";
  const defaultResultLabel = "Latest Result";
  const defaultSubmit = "Run Flow";
  const defaultAssistant = "Flow Result";
  const defaultCommand = "mfe.nifi.flow.send";
  const normalizedAsync = normalizeAsyncConfig(props.async, inheritedAsync);

  return {
    title: asString(props.title, defaultTitle).trim() || defaultTitle,
    inputPlaceholder: asString(props.inputPlaceholder, defaultInput).trim() || defaultInput,
    parametersLabel:
      asString(props.parametersLabel, defaultParametersLabel).trim() || defaultParametersLabel,
    parametersPlaceholder:
      asString(props.parametersPlaceholder, defaultParametersPlaceholder).trim() ||
      defaultParametersPlaceholder,
    defaultParametersJson:
      asString(props.defaultParametersJson, defaultParametersJson).trim() || defaultParametersJson,
    resultLabel: asString(props.resultLabel, defaultResultLabel).trim() || defaultResultLabel,
    submitLabel: asString(props.submitLabel, defaultSubmit).trim() || defaultSubmit,
    assistantLabel: asString(props.assistantLabel, defaultAssistant).trim() || defaultAssistant,
    maxMessages: asInteger(props.maxMessages, 20, 1, 200),
    requestCommand: asString(props.requestCommand, defaultCommand).trim() || defaultCommand,
    async: normalizedAsync,
    graphql: normalizeGraphqlConfig(props, normalizedAsync),
  };
}

async function executeGraphqlHttp<TData>(
  url: string,
  query: string,
  variables: JsonValue,
  authToken: string,
  signal?: AbortSignal,
): Promise<TData> {
  const response = await fetch(url, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      ...(authToken ? { authorization: `Bearer ${authToken}` } : {}),
    },
    body: JSON.stringify({ query, variables }),
    signal,
  });

  if (!response.ok) {
    throw new Error(`GraphQL HTTP request failed with status ${response.status}`);
  }
  const payload = (await response.json()) as {
    data?: TData;
    errors?: Array<{ message?: string }>;
  };

  if (payload.errors && payload.errors.length > 0) {
    throw new Error(payload.errors.map((entry) => entry.message || "GraphQL error").join("; "));
  }
  if (!payload.data) {
    throw new Error("GraphQL HTTP response did not include data.");
  }
  return payload.data;
}

export const createModule: ModuleFactory = (ctx): ModuleRuntime => {
  let host: HTMLElement = ctx.hostElement;
  let props = resolveChatProps(ctx.props, ctx.async);
  let destroyed = false;
  let pendingRequest = false;
  let formEl: HTMLFormElement | null = null;
  let inputEl: HTMLInputElement | null = null;
  let paramsEl: HTMLTextAreaElement | null = null;
  let submitEl: HTMLButtonElement | null = null;
  let listEl: HTMLDivElement | null = null;
  let resultEl: HTMLPreElement | null = null;
  let statusEl: HTMLDivElement | null = null;
  let statusBadgeEl: HTMLSpanElement | null = null;
  let statusDotEl: HTMLSpanElement | null = null;
  let activeDispose: (() => void) | null = null;
  let detachAuthLogoutListeners: (() => void) | null = null;
  let tokenExchangeCache: TokenExchangeCacheEntry | null = null;
  let healthState: "idle" | "running" | "ok" | "error" = "idle";

  const resolveRuntimeGraphql = (): RuntimeGraphqlConfig => {
    const runtimeFromContext = asRecord(ctx.environment?.graphql);
    const runtimeFromDom = readGraphqlFromDom();
    const runtimeFromWindow = readGraphqlFromWindow();
    return {
      httpUrl: firstNonEmpty(
        asString(props.graphql.httpUrl),
        asString(runtimeFromContext.httpUrl),
        runtimeFromDom.httpUrl,
        runtimeFromWindow.httpUrl,
      ),
      wsUrl: firstNonEmpty(
        asString(props.graphql.wsUrl),
        asString(runtimeFromContext.wsUrl),
        runtimeFromDom.wsUrl,
        runtimeFromWindow.wsUrl,
      ),
      authToken: firstNonEmpty(
        asString(props.graphql.authToken),
        asString(runtimeFromContext.authToken),
        runtimeFromDom.authToken,
        runtimeFromWindow.authToken,
      ),
    };
  };

  const resolveEffectiveGraphqlConfig = (): GraphqlConfig => {
    const runtime = resolveRuntimeGraphql();
    const tokenExchangeDefaults = readTokenExchangeDefaults();
    const tokenExchange = props.graphql.tokenExchange;
    return {
      ...props.graphql,
      httpUrl: runtime.httpUrl,
      wsUrl: runtime.wsUrl,
      authToken: runtime.authToken,
      tokenExchange: {
        ...tokenExchange,
        tokenUrl: firstNonEmpty(tokenExchange.tokenUrl, tokenExchangeDefaults.tokenUrl),
        clientId: firstNonEmpty(tokenExchange.clientId, tokenExchangeDefaults.clientId),
        exchangeUrl: firstNonEmpty(tokenExchange.exchangeUrl, tokenExchangeDefaults.exchangeUrl),
        appSlug: firstNonEmpty(tokenExchange.appSlug, tokenExchangeDefaults.appSlug),
      },
    };
  };

  const resolveGraphqlForRequest = async (graphql: GraphqlConfig): Promise<GraphqlConfig> => {
    const sourceToken = asString(graphql.authToken).trim();
    const tokenExchange = graphql.tokenExchange;
    if (!sourceToken || !tokenExchange.enabled) {
      return graphql;
    }

    const requestedAudiences = normalizeAudienceValues(
      asString(tokenExchange.requestedAudience).trim(),
      tokenExchange.requestedAudiences,
    );
    const requestedScope = asString(tokenExchange.requestedScope).trim();
    if (requestedAudiences.length === 0 && !requestedScope) {
      return graphql;
    }

    const exchangeUrl = asString(tokenExchange.exchangeUrl).trim();
    if (
      !exchangeUrl &&
      requestedAudiences.length > 0 &&
      tokenHasAllAudiences(sourceToken, requestedAudiences)
    ) {
      return graphql;
    }

    const tokenUrl = firstNonEmpty(
      asString(tokenExchange.tokenUrl).trim(),
      deriveTokenEndpointFromTokenIssuer(sourceToken),
    );
    const clientId = asString(tokenExchange.clientId).trim();
    const appSlug = asString(tokenExchange.appSlug).trim();

    if (!exchangeUrl) {
      if (!tokenUrl) {
        throw new Error(
          "Token exchange is enabled but no token endpoint is configured. Set graphql.tokenExchange.tokenUrl.",
        );
      }
      if (!clientId) {
        throw new Error(
          "Token exchange is enabled but no client id is configured. Set graphql.tokenExchange.clientId.",
        );
      }
    } else if (!appSlug) {
      throw new Error(
        "Token exchange via gateway is enabled but no app slug is configured. Set graphql.tokenExchange.appSlug.",
      );
    }

    const cache = tokenExchangeCache;
    if (
      cache &&
      cache.sourceToken === sourceToken &&
      cache.tokenUrl === tokenUrl &&
      cache.clientId === clientId &&
      cache.exchangeUrl === exchangeUrl &&
      cache.appSlug === appSlug &&
      cache.requestedAudienceKey === requestedAudiences.join("|") &&
      cache.requestedScope === requestedScope &&
      cache.exchangedToken &&
      (!cache.expiresAt || Date.now() + TOKEN_EXCHANGE_EXPIRY_LEEWAY_MS < cache.expiresAt)
    ) {
      return { ...graphql, authToken: cache.exchangedToken };
    }

    let response: Response;
    if (exchangeUrl) {
      const gatewayBody: Record<string, unknown> = {
        app_slug: appSlug,
        subject_token: sourceToken,
      };
      if (requestedAudiences.length === 1) {
        gatewayBody.requested_audience = requestedAudiences[0];
      } else if (requestedAudiences.length > 1) {
        gatewayBody.requested_audiences = requestedAudiences;
      }
      if (requestedScope) {
        gatewayBody.requested_scope = requestedScope;
      }

      setStatus("Exchanging auth token via auth gateway...");
      response = await fetch(exchangeUrl, {
        method: "POST",
        headers: {
          accept: "application/json",
          "content-type": "application/json",
          authorization: `Bearer ${sourceToken}`,
        },
        body: JSON.stringify(gatewayBody),
        signal: ctx.signal,
        cache: "no-store",
      });
    } else {
      const body = new URLSearchParams();
      body.set("grant_type", TOKEN_EXCHANGE_GRANT_TYPE);
      body.set("client_id", clientId);
      body.set("subject_token", sourceToken);
      body.set("subject_token_type", ACCESS_TOKEN_TYPE_URN);
      body.set("requested_token_type", ACCESS_TOKEN_TYPE_URN);
      for (const audience of requestedAudiences) {
        body.append("audience", audience);
      }
      if (requestedScope) {
        body.set("scope", requestedScope);
      }

      setStatus("Exchanging auth token for GraphQL audience...");
      response = await fetch(tokenUrl, {
        method: "POST",
        headers: {
          accept: "application/json",
          "content-type": "application/x-www-form-urlencoded",
        },
        body: body.toString(),
        signal: ctx.signal,
        cache: "no-store",
      });
    }
    if (!response.ok) {
      const details = (await response.text()).trim().slice(0, 240);
      throw new Error(
        `Token exchange failed (${response.status})${details ? `: ${details}` : ""}`,
      );
    }

    const payload = (await response.json()) as Record<string, unknown>;
    const exchangedToken = asString(payload.access_token).trim();
    if (!exchangedToken) {
      throw new Error("Token exchange response did not include access_token.");
    }

    let expiresAt: number | null = null;
    const expiresIn = payload.expires_in;
    if (typeof expiresIn === "number" && Number.isFinite(expiresIn) && expiresIn > 0) {
      expiresAt = Date.now() + Math.floor(expiresIn * 1000);
    } else {
      expiresAt = parseJwtExpiryMs(exchangedToken);
    }

    tokenExchangeCache = {
      sourceToken,
      tokenUrl,
      clientId,
      exchangeUrl,
      appSlug,
      requestedAudienceKey: requestedAudiences.join("|"),
      requestedScope,
      exchangedToken,
      expiresAt,
    };

    return { ...graphql, authToken: exchangedToken };
  };

  const clearActiveSubscription = () => {
    if (activeDispose) {
      activeDispose();
      activeDispose = null;
    }
  };

  const clearModuleAuthSessionState = () => {
    tokenExchangeCache = null;
    clearActiveSubscription();
    setPending(false);
    setHealth("idle", "Signed Out");
    if (inputEl) {
      inputEl.value = "";
    }
    setStatus("Signed out. Session state cleared.");
  };

  const bindAuthLogoutListeners = (): (() => void) | null => {
    if (typeof window === "undefined") {
      return null;
    }
    const onAuthLogout = () => {
      clearModuleAuthSessionState();
      appendLine("system", "Sign-out detected. Cleared local module auth state.");
    };
    for (const eventName of AUTH_LOGOUT_EVENTS) {
      window.addEventListener(eventName, onAuthLogout as EventListener);
    }
    return () => {
      for (const eventName of AUTH_LOGOUT_EVENTS) {
        window.removeEventListener(eventName, onAuthLogout as EventListener);
      }
    };
  };

  const ensureStyles = () => {
    if (typeof document === "undefined") {
      return;
    }
    const styleId = "nifi-flow-mfe-runner-styles-v1";
    if (document.getElementById(styleId)) {
      return;
    }
    const style = document.createElement("style");
    style.id = styleId;
    style.textContent = `
.nifi-runner-shell {
  position: relative;
  isolation: isolate;
  display: grid;
  gap: 0.95rem;
  width: 100%;
  max-width: 1080px;
  padding: 1rem;
  border: 1px solid color-mix(in srgb, ${THEME_COLOR.border} 86%, #38bdf8);
  border-radius: 16px;
  background:
    radial-gradient(130% 120% at 0% 0%, color-mix(in srgb, ${THEME_COLOR.accent} 16%, transparent), transparent 62%),
    radial-gradient(110% 120% at 100% 100%, color-mix(in srgb, #f59e0b 13%, transparent), transparent 60%),
    linear-gradient(135deg, color-mix(in srgb, ${THEME_COLOR.surface} 90%, #0ea5e9 10%), ${THEME_COLOR.surface});
  color: ${THEME_COLOR.text};
  box-shadow: 0 20px 44px rgba(15, 23, 42, 0.16);
  box-sizing: border-box;
}
.nifi-runner-shell::before {
  content: "";
  position: absolute;
  inset: 0;
  z-index: -1;
  pointer-events: none;
  border-radius: 16px;
  background-image:
    linear-gradient(90deg, color-mix(in srgb, ${THEME_COLOR.border} 40%, transparent) 1px, transparent 1px),
    linear-gradient(0deg, color-mix(in srgb, ${THEME_COLOR.border} 40%, transparent) 1px, transparent 1px);
  background-size: 18px 18px;
  opacity: 0.22;
}
.nifi-runner-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 0.8rem;
}
.nifi-runner-title {
  margin: 0;
  font-size: 1.12rem;
  font-weight: 800;
  letter-spacing: 0.01em;
}
.nifi-runner-subtitle {
  margin: 0.22rem 0 0;
  font-size: 0.81rem;
  color: ${THEME_COLOR.muted};
}
.nifi-runner-health {
  display: inline-flex;
  align-items: center;
  gap: 0.42rem;
  border: 1px solid color-mix(in srgb, ${THEME_COLOR.border} 82%, transparent);
  border-radius: 999px;
  padding: 0.28rem 0.68rem;
  font-size: 0.74rem;
  font-weight: 700;
  letter-spacing: 0.06em;
  text-transform: uppercase;
  background: color-mix(in srgb, ${THEME_COLOR.elevated} 80%, transparent);
}
.nifi-runner-health-dot {
  width: 0.58rem;
  height: 0.58rem;
  border-radius: 50%;
  background: #94a3b8;
  box-shadow: 0 0 0 0 rgba(148, 163, 184, 0.7);
}
.nifi-runner-health[data-state="running"] .nifi-runner-health-dot {
  background: #f59e0b;
  animation: nifiPulse 1.4s ease-out infinite;
}
.nifi-runner-health[data-state="ok"] .nifi-runner-health-dot {
  background: #22c55e;
  box-shadow: 0 0 0 0 rgba(34, 197, 94, 0.55);
}
.nifi-runner-health[data-state="error"] .nifi-runner-health-dot {
  background: #ef4444;
  box-shadow: 0 0 0 0 rgba(239, 68, 68, 0.58);
}
.nifi-runner-map {
  display: grid;
  grid-template-columns: repeat(7, minmax(0, 1fr));
  gap: 0.5rem;
  align-items: center;
}
.nifi-stage-card {
  display: grid;
  gap: 0.25rem;
  min-height: 70px;
  padding: 0.55rem 0.62rem;
  border-radius: 10px;
  border: 1px solid color-mix(in srgb, ${THEME_COLOR.border} 80%, #38bdf8 20%);
  background: linear-gradient(
    180deg,
    color-mix(in srgb, ${THEME_COLOR.surface} 85%, #ffffff 15%),
    color-mix(in srgb, ${THEME_COLOR.elevated} 82%, #e0f2fe 18%)
  );
}
.nifi-stage-card > strong {
  font-size: 0.76rem;
  line-height: 1.15;
}
.nifi-stage-card > span {
  font-size: 0.69rem;
  color: ${THEME_COLOR.muted};
  line-height: 1.25;
}
.nifi-stage-link {
  text-align: center;
  font-size: 1.02rem;
  color: color-mix(in srgb, ${THEME_COLOR.accent} 72%, #0891b2);
  opacity: 0.92;
}
.nifi-runner-grid {
  display: grid;
  grid-template-columns: minmax(300px, 1fr) minmax(320px, 1fr);
  gap: 0.9rem;
}
.nifi-panel {
  display: grid;
  gap: 0.56rem;
  padding: 0.76rem;
  border: 1px solid color-mix(in srgb, ${THEME_COLOR.border} 86%, transparent);
  border-radius: 12px;
  background: color-mix(in srgb, ${THEME_COLOR.surface} 88%, #e2e8f0 12%);
}
.nifi-panel-title {
  margin: 0;
  font-size: 0.84rem;
  font-weight: 700;
  letter-spacing: 0.02em;
}
.nifi-field {
  display: grid;
  gap: 0.34rem;
}
.nifi-field > label {
  font-size: 0.76rem;
  color: ${THEME_COLOR.muted};
}
.nifi-input,
.nifi-textarea {
  border: 1px solid color-mix(in srgb, ${THEME_COLOR.border} 82%, #0ea5e9 18%);
  border-radius: 9px;
  background: color-mix(in srgb, ${THEME_COLOR.surface} 95%, #ffffff 5%);
  color: ${THEME_COLOR.text};
  padding: 0.5rem 0.62rem;
}
.nifi-input {
  min-height: 2.2rem;
}
.nifi-textarea {
  min-height: 7.25rem;
  font-size: 0.82rem;
  line-height: 1.35;
  font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
}
.nifi-submit {
  justify-self: start;
  min-height: 2.2rem;
  min-width: 8.7rem;
  border-radius: 999px;
  border: 1px solid color-mix(in srgb, ${THEME_COLOR.accent} 68%, #0ea5e9);
  background: linear-gradient(135deg, ${THEME_COLOR.accent}, #0284c7);
  color: ${THEME_COLOR.onAccent};
  font-size: 0.81rem;
  font-weight: 700;
  letter-spacing: 0.02em;
  cursor: pointer;
  padding: 0.46rem 0.9rem;
  transition: transform 120ms ease, box-shadow 120ms ease, filter 120ms ease;
}
.nifi-submit:hover {
  transform: translateY(-1px);
  box-shadow: 0 10px 18px rgba(2, 132, 199, 0.26);
}
.nifi-submit:disabled {
  opacity: 0.58;
  cursor: not-allowed;
  transform: none;
  box-shadow: none;
}
.nifi-status {
  font-size: 0.78rem;
  color: ${THEME_COLOR.muted};
}
.nifi-log {
  display: grid;
  gap: 0.48rem;
  max-height: 13rem;
  overflow: auto;
  padding: 0.24rem;
}
.nifi-log-line {
  display: grid;
  gap: 0.2rem;
  padding: 0.46rem 0.56rem;
  border-radius: 9px;
  border: 1px solid color-mix(in srgb, ${THEME_COLOR.border} 88%, transparent);
  background: color-mix(in srgb, ${THEME_COLOR.elevated} 92%, #e2e8f0 8%);
}
.nifi-log-line[data-role="user"] {
  border-color: color-mix(in srgb, #0ea5e9 34%, ${THEME_COLOR.border});
  background: color-mix(in srgb, #e0f2fe 78%, ${THEME_COLOR.elevated});
}
.nifi-log-line[data-role="assistant"] {
  border-color: color-mix(in srgb, #22c55e 30%, ${THEME_COLOR.border});
  background: color-mix(in srgb, #dcfce7 78%, ${THEME_COLOR.elevated});
}
.nifi-log-line[data-role="system"] {
  border-color: color-mix(in srgb, #f59e0b 34%, ${THEME_COLOR.border});
  background: color-mix(in srgb, #fef3c7 80%, ${THEME_COLOR.elevated});
}
.nifi-log-line strong {
  font-size: 0.72rem;
  letter-spacing: 0.04em;
  text-transform: uppercase;
}
.nifi-log-line pre {
  margin: 0;
  white-space: pre-wrap;
  word-break: break-word;
  font-family: inherit;
  font-size: 0.79rem;
}
.nifi-result {
  margin: 0;
  min-height: 14rem;
  max-height: 28rem;
  overflow: auto;
  padding: 0.6rem 0.7rem;
  border-radius: 10px;
  border: 1px solid color-mix(in srgb, ${THEME_COLOR.border} 86%, #0ea5e9 14%);
  background: linear-gradient(
    180deg,
    color-mix(in srgb, #020617 95%, #0c4a6e 5%),
    #020617
  );
  color: #c7f9ff;
  font-size: 0.79rem;
  line-height: 1.36;
  font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
  white-space: pre-wrap;
  word-break: break-word;
}
@keyframes nifiPulse {
  0% { box-shadow: 0 0 0 0 rgba(245, 158, 11, 0.62); }
  70% { box-shadow: 0 0 0 10px rgba(245, 158, 11, 0); }
  100% { box-shadow: 0 0 0 0 rgba(245, 158, 11, 0); }
}
@media (max-width: 980px) {
  .nifi-runner-map { grid-template-columns: 1fr; }
  .nifi-stage-link { transform: rotate(90deg); }
  .nifi-runner-grid { grid-template-columns: 1fr; }
}
`;
    document.head.append(style);
  };

  const setHealth = (state: "idle" | "running" | "ok" | "error", label?: string) => {
    healthState = state;
    if (statusBadgeEl) {
      statusBadgeEl.dataset.state = state;
      statusBadgeEl.setAttribute("data-state", state);
      statusBadgeEl.title = label || "";
      statusBadgeEl.lastChild && (statusBadgeEl.lastChild.textContent = label || state);
    }
    if (statusDotEl) {
      statusDotEl.dataset.state = state;
    }
  };

  const appendLine = (role: "user" | "assistant" | "system", text: string) => {
    if (!listEl) {
      return;
    }
    const item = document.createElement("article");
    item.className = "nifi-log-line";
    item.setAttribute("data-role", role);

    const label = document.createElement("strong");
    label.textContent = role === "user" ? "You" : role === "assistant" ? props.assistantLabel : "System";
    const body = document.createElement("pre");
    body.textContent = text;
    item.append(label, body);
    listEl.append(item);
    listEl.scrollTop = listEl.scrollHeight;

    while (listEl.children.length > props.maxMessages) {
      listEl.removeChild(listEl.firstElementChild as Element);
    }
  };

  const appendAssistantPendingLine = () => {
    if (!listEl) {
      return null as HTMLDivElement | null;
    }
    const item = document.createElement("article");
    item.className = "nifi-log-line";
    item.setAttribute("data-role", "assistant");
    const label = document.createElement("strong");
    label.textContent = props.assistantLabel;
    const body = document.createElement("div");
    body.textContent = "Waiting for stream...";
    item.append(label, body);
    listEl.append(item);
    listEl.scrollTop = listEl.scrollHeight;
    return body;
  };

  const setStatus = (message: string) => {
    if (!statusEl) {
      return;
    }
    statusEl.textContent = message;
    if (statusBadgeEl && healthState !== "running") {
      statusBadgeEl.title = message;
    }
  };

  const setResult = (value: unknown) => {
    if (!resultEl) {
      return;
    }
    try {
      resultEl.textContent = JSON.stringify(value, null, 2);
    } catch {
      resultEl.textContent = asString(value);
    }
  };

  const setPending = (nextValue: boolean) => {
    pendingRequest = nextValue;
    if (nextValue) {
      setHealth("running", "Running");
    } else if (healthState === "running") {
      setHealth("ok", "Ready");
    }
    if (submitEl) {
      submitEl.disabled = nextValue;
    }
    if (inputEl) {
      inputEl.disabled = nextValue;
    }
    if (paramsEl) {
      paramsEl.disabled = nextValue;
    }
  };

  const readGlobalAuth = (): Record<string, unknown> => {
    if (typeof window === "undefined") {
      return {};
    }
    return asRecord((window as Window & { __SUNCOAST_AUTH__?: unknown }).__SUNCOAST_AUTH__);
  };

  const refreshShellAuthTokenIfPossible = async (minValiditySeconds = 120): Promise<void> => {
    const auth = readGlobalAuth();
    const attempts: Array<{ name: string; argsList: unknown[][] }> = [
      { name: "ensureFreshToken", argsList: [[minValiditySeconds], []] },
      { name: "updateToken", argsList: [[minValiditySeconds], []] },
      { name: "refreshAccessToken", argsList: [[minValiditySeconds], []] },
      { name: "refreshToken", argsList: [[minValiditySeconds], []] },
      { name: "refreshSession", argsList: [[false], []] },
    ];

    for (const attempt of attempts) {
      const fn = auth[attempt.name];
      if (typeof fn !== "function") {
        continue;
      }
      for (const args of attempt.argsList) {
        try {
          await Promise.resolve((fn as (...fnArgs: unknown[]) => unknown).apply(auth, args));
          return;
        } catch {
          continue;
        }
      }
    }
  };

  const subscribeForStream = async (
    requestId: string,
    assistantBody: HTMLDivElement | null,
    graphql: GraphqlConfig,
  ): Promise<{
    lastPayload: unknown;
    lastError: string;
    lastStatus: string;
  }> => {
    if (!graphql.wsUrl) {
      throw new Error("Missing graphql.wsUrl.");
    }

    const templateVariables = applyTemplate(graphql.streamVariables, {
      requestId,
      conversationId: graphql.conversationId,
      moduleKey: ctx.moduleKey,
      instanceId: ctx.instanceId,
      cacheKey: ctx.environment.cacheKey || "",
      contentHash: ctx.environment.contentHash || "",
      source: ctx.environment.source || "",
      asyncMode: props.async.mode,
      requestChannel: props.async.requestChannel || "",
      responseChannel: props.async.responseChannel || "",
      correlationIdPath: props.async.correlationIdPath || "",
    });
    const streamVariables = asRecord(templateVariables);

    return new Promise<{
      lastPayload: unknown;
      lastError: string;
      lastStatus: string;
    }>((resolve, reject) => {
      let settled = false;
      let buffer = "";
      let lastPayload: unknown = null;
      let lastError = "";
      let lastStatus = "";
      const connectionHeaders: Record<string, string> = {};
      if (graphql.authToken) {
        connectionHeaders.authorization = `Bearer ${graphql.authToken}`;
      }
      const client = createClient({
        url: graphql.wsUrl,
        lazy: true,
        retryAttempts: props.async.stream.reconnect.maxAttempts,
        connectionParams:
          Object.keys(connectionHeaders).length > 0
            ? {
                headers: connectionHeaders,
              }
            : undefined,
      });

      const settleResolve = () => {
        if (settled) {
          return;
        }
        settled = true;
        clearActiveSubscription();
        resolve({ lastPayload, lastError, lastStatus });
      };

      const settleReject = (error: unknown) => {
        if (settled) {
          return;
        }
        settled = true;
        clearActiveSubscription();
        reject(error);
      };

      const dispose = client.subscribe(
        {
          query: graphql.streamSubscription,
          variables: streamVariables,
        },
        {
          next: (payload) => {
            if (!payload || typeof payload !== "object") {
              return;
            }
            if ("errors" in payload && Array.isArray(payload.errors) && payload.errors.length > 0) {
              const message = payload.errors
                .map((entry) => entry?.message || "GraphQL stream error")
                .join("; ");
              settleReject(new Error(message));
              return;
            }
            const data = "data" in payload ? (payload.data as Record<string, unknown>) : {};
            lastPayload = data;
            const nextText = toDisplayText(getPathValue(data, graphql.streamTextPath)).trim();
            const doneValue = getPathValue(data, graphql.streamDonePath);
            const errorValue = toDisplayText(getPathValue(data, graphql.streamErrorPath)).trim();
            lastError = errorValue;
            lastStatus = asString(doneValue).trim();

            if (errorValue) {
              settleReject(new Error(errorValue));
              return;
            }

            if (nextText) {
              if (graphql.streamChunkMode === "replace") {
                buffer = nextText;
              } else {
                buffer += nextText;
              }
              if (assistantBody) {
                assistantBody.textContent = buffer;
              }
            }

            const doneText = asString(doneValue).trim().toLowerCase();
            const done =
              doneValue === true ||
              doneText === "true" ||
              doneText === "done" ||
              doneText === "complete" ||
              doneText === "completed";
            if (!errorValue && (doneText === "error" || doneText === "failed")) {
              settleReject(new Error("Request failed."));
              return;
            }
            if (done) {
              settleResolve();
            }
          },
          error: (error) => {
            settleReject(
              new Error(
                describeUnknownError(error) || "GraphQL stream connection failed.",
              ),
            );
          },
          complete: () => {
            settleResolve();
          },
        },
      );

      activeDispose = () => {
        dispose();
        client.dispose();
      };

      const abortSignal = ctx.signal;
      if (abortSignal) {
        if (abortSignal.aborted) {
          settleReject(new Error("Stream aborted"));
          return;
        }
        const onAbort = () => {
          settleReject(new Error("Stream aborted"));
        };
        abortSignal.addEventListener("abort", onAbort, { once: true });
      }
    });
  };

  const submitMessage = async (message: string, parameters: JsonObject) => {
    const paramsText = toTemplateString(parameters) || "{}";
    appendLine("user", `Message: ${message}\nParameters: ${paramsText}`);
    setResult({
      request: {
        message,
        parameters,
      },
      state: "submitting",
    });
    emitEvent(ctx, "mfe.nifi.flow.runner.submitted", {
      text: message,
      parameters,
      mode: asString(props.async.mode) || defaultAsyncConfig.mode,
      requestChannel: props.async.requestChannel || "",
      responseChannel: props.async.responseChannel || "",
    });
    await refreshShellAuthTokenIfPossible();
    const runtimeGraphql = resolveEffectiveGraphqlConfig();

    if (!props.async.enabled || props.async.mode === "none") {
      appendLine("system", "Async disabled. Enable props.async for GraphQL stream mode.");
      return;
    }
    if (!runtimeGraphql.httpUrl) {
      appendLine("system", "Missing graphql.httpUrl in module props.");
      return;
    }
    if (!runtimeGraphql.wsUrl) {
      appendLine("system", "Missing graphql.wsUrl in module props.");
      return;
    }

    const assistantBody = appendAssistantPendingLine();
    try {
      setPending(true);
      setStatus("Submitting flow request...");
      const graphql = await resolveGraphqlForRequest(runtimeGraphql);
      const parameterVariables = buildParameterTemplateVariables(parameters);

      const submitVariables = applyTemplate(graphql.submitVariables, {
        prompt: message,
        message,
        conversationId: graphql.conversationId,
        moduleKey: ctx.moduleKey,
        instanceId: ctx.instanceId,
        cacheKey: ctx.environment.cacheKey || "",
        contentHash: ctx.environment.contentHash || "",
        source: ctx.environment.source || "",
        asyncMode: props.async.mode,
        requestChannel: props.async.requestChannel || "",
        responseChannel: props.async.responseChannel || "",
        correlationIdPath: props.async.correlationIdPath || "",
        ...parameterVariables,
      });

      const submitData = await executeGraphqlHttp<Record<string, unknown>>(
        graphql.httpUrl,
        graphql.submitMutation,
        submitVariables,
        graphql.authToken,
        ctx.signal,
      );

      const requestIdCandidatePaths = Array.from(
        new Set([
          graphql.submitRequestIdPath,
          props.async.correlationIdPath,
          "publish_async_request.request_id",
          "requestId",
        ].map((entry) => asString(entry).trim()).filter(Boolean)),
      );
      const requestIdResolution = resolveRequestIdFromPaths(submitData, requestIdCandidatePaths);
      const requestId = requestIdResolution.requestId;
      if (!requestId) {
        throw new Error(
          `Submit response missing request id. Tried paths: ${requestIdCandidatePaths.join(", ") || "(none)"}.`,
        );
      }

      const resolvedRequestIdPath = requestIdResolution.path || graphql.submitRequestIdPath;
      const channelSummary = formatAsyncChannelSummary(props.async);
      setStatus(
        `Request accepted (${requestId}). Listening for stream via '${resolvedRequestIdPath}'${
          channelSummary ? ` [${channelSummary}]` : ""
        }...`,
      );
      const streamResult = await subscribeForStream(requestId, assistantBody, graphql);
      setStatus("Response stream completed.");
      setHealth("ok", "Completed");
      setResult({
        requestId,
        submitResponse: submitData,
        streamResult,
      });
      emitEvent(ctx, "mfe.nifi.flow.runner.responded", {
        requestId,
        parameters,
        source: asString(props.async.mode) || defaultAsyncConfig.mode,
        requestChannel: props.async.requestChannel || "",
        responseChannel: props.async.responseChannel || "",
      });
    } catch (error) {
      const message = describeUnknownError(error) || "Unknown stream error";
      if (assistantBody) {
        assistantBody.textContent = `Error: ${message}`;
      } else {
        appendLine("system", `Error: ${message}`);
      }
      setStatus("Request failed.");
      setHealth("error", "Failed");
      setResult({
        error: message,
        state: "failed",
      });
    } finally {
      clearActiveSubscription();
      setPending(false);
    }
  };

  const render = () => {
    host.innerHTML = "";
    ensureStyles();

    const shell = document.createElement("section");
    shell.setAttribute("data-nifi-flow-mfe", "runner");
    shell.className = "nifi-runner-shell";

    const header = document.createElement("header");
    header.className = "nifi-runner-header";

    const headingGroup = document.createElement("div");
    const titleEl = document.createElement("h3");
    titleEl.className = "nifi-runner-title";
    titleEl.textContent = props.title;
    const subtitleEl = document.createElement("p");
    subtitleEl.className = "nifi-runner-subtitle";
    subtitleEl.textContent = "Message + parameter orchestration through GraphQL async bridge";
    headingGroup.append(titleEl, subtitleEl);

    statusBadgeEl = document.createElement("span");
    statusBadgeEl.className = "nifi-runner-health";
    statusDotEl = document.createElement("span");
    statusDotEl.className = "nifi-runner-health-dot";
    const statusBadgeText = document.createElement("span");
    statusBadgeText.textContent = "Ready";
    statusBadgeEl.append(statusDotEl, statusBadgeText);

    header.append(headingGroup, statusBadgeEl);

    const map = document.createElement("div");
    map.className = "nifi-runner-map";

    const stage = (name: string, detail: string) => {
      const card = document.createElement("div");
      card.className = "nifi-stage-card";
      const heading = document.createElement("strong");
      heading.textContent = name;
      const text = document.createElement("span");
      text.textContent = detail;
      card.append(heading, text);
      return card;
    };

    const link = () => {
      const arrow = document.createElement("div");
      arrow.className = "nifi-stage-link";
      arrow.textContent = "→";
      return arrow;
    };

    map.append(
      stage("Input", "Message + JSON parameters"),
      link(),
      stage("Flink Stage 1", "Topic consume + rule route"),
      link(),
      stage("NiFi Stage", "Flow processing + API calls"),
      link(),
      stage("Output", "Kafka response stream"),
    );

    const grid = document.createElement("div");
    grid.className = "nifi-runner-grid";

    const left = document.createElement("div");
    left.style.display = "grid";
    left.style.gap = "0.9rem";

    const requestPanel = document.createElement("section");
    requestPanel.className = "nifi-panel";
    const requestTitle = document.createElement("h4");
    requestTitle.className = "nifi-panel-title";
    requestTitle.textContent = "Flow Request Builder";
    statusEl = document.createElement("div");
    statusEl.className = "nifi-status";

    formEl = document.createElement("form");
    formEl.style.display = "grid";
    formEl.style.gap = "0.56rem";

    const messageField = document.createElement("div");
    messageField.className = "nifi-field";
    const messageLabel = document.createElement("label");
    messageLabel.textContent = "Message";
    inputEl = document.createElement("input");
    inputEl.className = "nifi-input";
    inputEl.type = "text";
    inputEl.placeholder = props.inputPlaceholder;
    inputEl.autocomplete = "off";
    messageField.append(messageLabel, inputEl);

    const parametersField = document.createElement("div");
    parametersField.className = "nifi-field";
    const paramsLabel = document.createElement("label");
    paramsLabel.textContent = props.parametersLabel;
    paramsEl = document.createElement("textarea");
    paramsEl.className = "nifi-textarea";
    paramsEl.placeholder = props.parametersPlaceholder;
    paramsEl.value = props.defaultParametersJson;
    parametersField.append(paramsLabel, paramsEl);

    submitEl = document.createElement("button");
    submitEl.className = "nifi-submit";
    submitEl.type = "submit";
    submitEl.textContent = props.submitLabel;

    formEl.append(messageField, parametersField, submitEl);
    formEl.addEventListener("submit", (event) => {
      event.preventDefault();
      if (destroyed || pendingRequest || !inputEl || !paramsEl) {
        return;
      }
      const message = inputEl.value.trim();
      if (!message) {
        appendLine("system", "Message is required.");
        return;
      }
      let parameters: JsonObject;
      try {
        parameters = parseParametersJson(paramsEl.value);
      } catch (error) {
        const parseError = describeUnknownError(error) || "Invalid parameters JSON.";
        appendLine("system", parseError);
        setStatus("Request blocked due to invalid parameters JSON.");
        return;
      }
      inputEl.value = "";
      void submitMessage(message, parameters);
    });

    requestPanel.append(requestTitle, statusEl, formEl);

    const activityPanel = document.createElement("section");
    activityPanel.className = "nifi-panel";
    const activityTitle = document.createElement("h4");
    activityTitle.className = "nifi-panel-title";
    activityTitle.textContent = "Activity Feed";
    listEl = document.createElement("div");
    listEl.className = "nifi-log";
    activityPanel.append(activityTitle, listEl);

    left.append(requestPanel, activityPanel);

    const right = document.createElement("section");
    right.className = "nifi-panel";
    const resultLabel = document.createElement("h4");
    resultLabel.className = "nifi-panel-title";
    resultLabel.textContent = props.resultLabel;
    resultEl = document.createElement("pre");
    resultEl.className = "nifi-result";
    setResult({
      status: "idle",
      note: "Submit a message to run the flow and view responses.",
    });
    right.append(resultLabel, resultEl);

    grid.append(left, right);
    shell.append(header, map, grid);
    host.append(shell);

    {
      const channelSummary = formatAsyncChannelSummary(props.async);
      setStatus(
        `Async mode: ${props.async.mode} (${props.async.stream.transport})${
          channelSummary ? ` [${channelSummary}]` : ""
        }`,
      );
      setHealth("ok", "Ready");
    }
  };

  return {
    init(input) {
      if (input?.props) {
        props = resolveChatProps(input.props, props.async);
      }
    },
    mount(input) {
      host = input.element ?? host;
      render();
      if (detachAuthLogoutListeners) {
        detachAuthLogoutListeners();
      }
      detachAuthLogoutListeners = bindAuthLogoutListeners();
      appendLine("system", "Module mounted.");
    },
    update(input) {
      if (!input?.props) {
        return;
      }
      props = resolveChatProps(input.props, props.async);
      render();
      appendLine("system", "Module updated.");
    },
    async handleCommand(input) {
      if (input.name !== props.requestCommand) {
        return { ok: false, error: `Unsupported command: ${input.name}` };
      }
      const payload = asRecord(input.payload);
      const text = asString(payload.text || payload.message).trim();
      if (!text) {
        return { ok: false, error: "Missing payload.message" };
      }
      let parameters: JsonObject = {};
      try {
        if (typeof payload.parameters === "string") {
          parameters = parseParametersJson(payload.parameters);
        } else if (payload.parameters && typeof payload.parameters === "object") {
          parameters = toJsonValue(payload.parameters) as JsonObject;
        }
      } catch (error) {
        return {
          ok: false,
          error: describeUnknownError(error) || "Invalid payload.parameters JSON",
        };
      }
      await submitMessage(text, parameters);
      return { ok: true, data: toJsonValue({ accepted: true, text, parameters }) };
    },
    unmount() {
      destroyed = true;
      if (detachAuthLogoutListeners) {
        detachAuthLogoutListeners();
        detachAuthLogoutListeners = null;
      }
      clearActiveSubscription();
      if (formEl) {
        const clone = formEl.cloneNode(true);
        formEl.replaceWith(clone);
      }
      host.innerHTML = "";
    },
  };
};
