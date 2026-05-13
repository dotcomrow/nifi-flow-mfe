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
  submitLabel: string;
  assistantLabel: string;
  maxMessages: number;
  requestCommand: string;
  async: ModuleAsyncConfig;
  graphql: GraphqlConfig;
};

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

function applyTemplate(value: JsonValue, variables: Record<string, string>): JsonValue {
  if (typeof value === "string") {
    return value.replace(/\{\{\s*([a-zA-Z0-9_.-]+)\s*\}\}/g, (_, key: string) => {
      return variables[key] ?? "";
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
          handler: "ai-service",
          operation: "chat.completion",
          payload: {
            prompt: "{{prompt}}",
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
  const defaultTitle = "Example Chat MFE";
  const defaultInput = "Ask AI...";
  const defaultSubmit = "Submit";
  const defaultAssistant = "Assistant";
  const defaultCommand = "mfe.example.chat.send";
  const normalizedAsync = normalizeAsyncConfig(props.async, inheritedAsync);

  return {
    title: asString(props.title, defaultTitle).trim() || defaultTitle,
    inputPlaceholder: asString(props.inputPlaceholder, defaultInput).trim() || defaultInput,
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
  let submitEl: HTMLButtonElement | null = null;
  let listEl: HTMLDivElement | null = null;
  let statusEl: HTMLDivElement | null = null;
  let activeDispose: (() => void) | null = null;
  let detachAuthLogoutListeners: (() => void) | null = null;
  let tokenExchangeCache: TokenExchangeCacheEntry | null = null;

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

  const appendLine = (role: "user" | "assistant" | "system", text: string) => {
    if (!listEl) {
      return;
    }
    const item = document.createElement("div");
    item.setAttribute("data-role", role);
    item.style.display = "grid";
    item.style.gap = "0.25rem";
    item.style.padding = "0.45rem 0.55rem";
    item.style.border = `1px solid ${THEME_COLOR.border}`;
    item.style.borderRadius = "8px";
    item.style.background =
      role === "user"
        ? THEME_COLOR.elevated
        : role === "assistant"
          ? THEME_COLOR.surface
          : THEME_COLOR.elevated;
    item.style.color = THEME_COLOR.text;
    const label = document.createElement("strong");
    label.textContent = role === "user" ? "You" : role === "assistant" ? props.assistantLabel : "System";
    const body = document.createElement("div");
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
    const item = document.createElement("div");
    item.setAttribute("data-role", "assistant");
    item.style.display = "grid";
    item.style.gap = "0.25rem";
    item.style.padding = "0.45rem 0.55rem";
    item.style.border = `1px solid ${THEME_COLOR.border}`;
    item.style.borderRadius = "8px";
    item.style.background = THEME_COLOR.surface;
    item.style.color = THEME_COLOR.text;
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
  };

  const setPending = (nextValue: boolean) => {
    pendingRequest = nextValue;
    if (submitEl) {
      submitEl.disabled = nextValue;
    }
    if (inputEl) {
      inputEl.disabled = nextValue;
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
  ): Promise<void> => {
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

    await new Promise<void>((resolve, reject) => {
      let settled = false;
      let buffer = "";
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
        resolve();
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
            const nextText = toDisplayText(getPathValue(data, graphql.streamTextPath)).trim();
            const doneValue = getPathValue(data, graphql.streamDonePath);
            const errorValue = toDisplayText(getPathValue(data, graphql.streamErrorPath)).trim();

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

  const submitMessage = async (text: string) => {
    appendLine("user", text);
    emitEvent(ctx, "mfe.example.chat.submitted", {
      text,
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
      setStatus("Submitting prompt...");
      const graphql = await resolveGraphqlForRequest(runtimeGraphql);

      const submitVariables = applyTemplate(graphql.submitVariables, {
        prompt: text,
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
      await subscribeForStream(requestId, assistantBody, graphql);
      setStatus("Response stream completed.");
      emitEvent(ctx, "mfe.example.chat.responded", {
        requestId,
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
    } finally {
      clearActiveSubscription();
      setPending(false);
    }
  };

  const render = () => {
    host.innerHTML = "";

    const container = document.createElement("section");
    container.setAttribute("data-example-mfe", "chat");
    container.style.display = "grid";
    container.style.gap = "0.75rem";
    container.style.width = "100%";
    container.style.maxWidth = "44rem";
    container.style.padding = "0.875rem";
    container.style.border = `1px solid ${THEME_COLOR.border}`;
    container.style.borderRadius = "10px";
    container.style.background = THEME_COLOR.surface;
    container.style.color = THEME_COLOR.text;
    container.style.boxSizing = "border-box";

    const header = document.createElement("header");
    header.style.display = "grid";
    header.style.gap = "0.25rem";

    const titleEl = document.createElement("h3");
    titleEl.style.margin = "0";
    titleEl.style.fontSize = "1rem";
    titleEl.style.fontWeight = "700";
    titleEl.textContent = props.title;

    statusEl = document.createElement("div");
    statusEl.style.fontSize = "0.78rem";
    statusEl.style.color = THEME_COLOR.muted;
    {
      const channelSummary = formatAsyncChannelSummary(props.async);
      setStatus(
        `Async mode: ${props.async.mode} (${props.async.stream.transport})${
          channelSummary ? ` [${channelSummary}]` : ""
        }`,
      );
    }

    header.append(titleEl, statusEl);

    listEl = document.createElement("div");
    listEl.style.display = "grid";
    listEl.style.gap = "0.5rem";
    listEl.style.maxHeight = "14rem";
    listEl.style.overflow = "auto";
    listEl.style.padding = "0.2rem";
    listEl.style.background = THEME_COLOR.elevated;
    listEl.style.border = `1px solid ${THEME_COLOR.border}`;
    listEl.style.borderRadius = "8px";

    formEl = document.createElement("form");
    formEl.style.display = "grid";
    formEl.style.gridTemplateColumns = "1fr auto";
    formEl.style.gap = "0.5rem";

    inputEl = document.createElement("input");
    inputEl.type = "text";
    inputEl.placeholder = props.inputPlaceholder;
    inputEl.autocomplete = "off";
    inputEl.style.minHeight = "2.25rem";
    inputEl.style.border = `1px solid ${THEME_COLOR.border}`;
    inputEl.style.borderRadius = "8px";
    inputEl.style.background = THEME_COLOR.surface;
    inputEl.style.color = THEME_COLOR.text;
    inputEl.style.padding = "0.4rem 0.6rem";
    inputEl.style.fontSize = "0.9rem";

    submitEl = document.createElement("button");
    submitEl.type = "submit";
    submitEl.textContent = props.submitLabel;
    submitEl.style.minWidth = "6rem";
    submitEl.style.minHeight = "2.25rem";
    submitEl.style.border = `1px solid ${THEME_COLOR.accent}`;
    submitEl.style.borderRadius = "8px";
    submitEl.style.padding = "0.4rem 0.75rem";
    submitEl.style.fontWeight = "600";
    submitEl.style.background = THEME_COLOR.accent;
    submitEl.style.color = THEME_COLOR.onAccent;
    submitEl.style.cursor = "pointer";

    formEl.append(inputEl, submitEl);
    formEl.addEventListener("submit", (event) => {
      event.preventDefault();
      if (destroyed || pendingRequest || !inputEl) {
        return;
      }
      const text = inputEl.value.trim();
      if (!text) {
        return;
      }
      inputEl.value = "";
      void submitMessage(text);
    });

    container.append(header, listEl, formEl);
    host.append(container);
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
      const text = asString(payload.text).trim();
      if (!text) {
        return { ok: false, error: "Missing payload.text" };
      }
      await submitMessage(text);
      return { ok: true, data: toJsonValue({ accepted: true, text }) };
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
