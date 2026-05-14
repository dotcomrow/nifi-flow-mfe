import { buildEnvDefaults } from "../src/build-env";
import { MODULE_KEY } from "../src/constants";
import { createCmsModuleDefinition } from "../src/host-adapter";

type Cleanup = (() => void) | undefined;
type AuthStatusState = "idle" | "ok" | "error";
type ThemeMode = "auto" | "light" | "dark";

type AuthFormState = {
  gatewayUrl: string;
  appSlug: string;
  codeParam: string;
};

const AUTH_TOKEN_STORAGE_KEY = "mfe.preview.authToken";
const AUTH_FORM_STORAGE_KEY = "mfe.preview.authFormState";
const THEME_STORAGE_KEY = "suncoast:cms:theme-mode";
const DEFAULT_PREVIEW_GRAPHQL_HTTP_URLS = {
  dev: "https://cf-suncoast-graphql-proxy.dev.suncoast.systems/graphql",
  prod: "https://cf-suncoast-graphql-proxy.prod.suncoast.systems/graphql",
} as const;

function inferPreviewEnvironment(): "dev" | "prod" {
  const hostname = window.location.hostname.trim().toLowerCase();
  if (
    hostname === "localhost" ||
    hostname === "127.0.0.1" ||
    hostname.endsWith(".dev.suncoast.systems") ||
    hostname.includes("preview")
  ) {
    return "dev";
  }
  return "prod";
}

function toWebSocketUrl(httpUrl: string): string {
  const normalized = httpUrl.trim();
  if (!normalized) {
    return "";
  }
  try {
    const parsed = new URL(normalized);
    parsed.protocol = parsed.protocol === "http:" ? "ws:" : "wss:";
    return parsed.toString();
  } catch {
    if (normalized.startsWith("https://")) {
      return `wss://${normalized.slice("https://".length)}`;
    }
    if (normalized.startsWith("http://")) {
      return `ws://${normalized.slice("http://".length)}`;
    }
    return normalized;
  }
}

function inferDefaultGraphqlHttpUrl(): string {
  const environment = inferPreviewEnvironment();
  return environment === "dev"
    ? DEFAULT_PREVIEW_GRAPHQL_HTTP_URLS.dev
    : DEFAULT_PREVIEW_GRAPHQL_HTTP_URLS.prod;
}

function getInput(id: string): HTMLInputElement {
  const element = document.getElementById(id);
  if (!(element instanceof HTMLInputElement)) {
    throw new Error(`Missing input element #${id}`);
  }
  return element;
}

function getHost(): HTMLElement {
  const element = document.getElementById("host");
  if (!(element instanceof HTMLElement)) {
    throw new Error("Missing host element #host");
  }
  return element;
}

function getButton(id: string): HTMLButtonElement {
  const element = document.getElementById(id);
  if (!(element instanceof HTMLButtonElement)) {
    throw new Error(`Missing button element #${id}`);
  }
  return element;
}

function getAuthStatusElement(): HTMLElement {
  const element = document.getElementById("authStatus");
  if (!(element instanceof HTMLElement)) {
    throw new Error("Missing auth status element #authStatus");
  }
  return element;
}

const httpUrlInput = getInput("httpUrl");
const wsUrlInput = getInput("wsUrl");
const authTokenInput = getInput("authToken");
const authGatewayInput = getInput("authGateway");
const authAppSlugInput = getInput("authAppSlug");
const authCodeParamInput = getInput("authCodeParam");
const conversationIdInput = getInput("conversationId");
const applyButton = getButton("applyButton");
const loginButton = getButton("loginButton");
const clearTokenButton = getButton("clearTokenButton");
const themeToggleButton = getButton("themeToggle");
const authStatus = getAuthStatusElement();
const host = getHost();

httpUrlInput.value = inferDefaultGraphqlHttpUrl();
wsUrlInput.value = toWebSocketUrl(httpUrlInput.value);
authGatewayInput.value = buildEnvDefaults.previewAuthGatewayUrl || "https://login.suncoast.systems";
authAppSlugInput.value = buildEnvDefaults.previewAuthAppSlug || "nifi-flow-mfe-dev";
authCodeParamInput.value = buildEnvDefaults.previewAuthCodeParam || "gateway_code";
conversationIdInput.value = "";

let currentAbort: AbortController | null = null;
let currentCleanup: Cleanup;

loadSavedAuthForm();
authTokenInput.value = getSavedToken();

function toErrorMessage(error: unknown): string {
  if (error instanceof Error && error.message) {
    return error.message;
  }
  return "Unexpected error";
}

function parseJsonObject<T>(value: string | null): T | null {
  if (!value) {
    return null;
  }
  try {
    return JSON.parse(value) as T;
  } catch {
    return null;
  }
}

function getSavedToken(): string {
  try {
    return localStorage.getItem(AUTH_TOKEN_STORAGE_KEY)?.trim() || "";
  } catch {
    return "";
  }
}

function getSavedTheme(): ThemeMode | null {
  try {
    const raw = localStorage.getItem(THEME_STORAGE_KEY)?.trim();
    if (raw === "auto" || raw === "light" || raw === "dark") {
      return raw;
    }
    return null;
  } catch {
    return null;
  }
}

function saveTheme(theme: ThemeMode): void {
  try {
    localStorage.setItem(THEME_STORAGE_KEY, theme);
  } catch {
    // Ignore storage write errors in preview harness.
  }
}

function inferSystemTheme(): ThemeMode {
  try {
    return window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
  } catch {
    return "light";
  }
}

function resolveThemeForRender(theme: ThemeMode): "light" | "dark" {
  if (theme === "auto") {
    const system = inferSystemTheme();
    return system === "dark" ? "dark" : "light";
  }
  return theme;
}

function applyTheme(theme: ThemeMode): void {
  const resolvedTheme = resolveThemeForRender(theme);
  document.documentElement.setAttribute("data-theme-mode", theme);
  document.documentElement.setAttribute("data-theme-mode-resolved", resolvedTheme);
  const label =
    theme === "auto"
      ? "Theme: Auto"
      : theme === "light"
      ? "Theme: Light"
      : "Theme: Dark";
  themeToggleButton.textContent = label;
}

function initializeTheme(): ThemeMode {
  const initial = getSavedTheme() ?? "auto";
  applyTheme(initial);
  return initial;
}

function saveToken(value: string): void {
  const token = value.trim();
  try {
    if (token) {
      localStorage.setItem(AUTH_TOKEN_STORAGE_KEY, token);
    } else {
      localStorage.removeItem(AUTH_TOKEN_STORAGE_KEY);
    }
  } catch {
    // Ignore storage write errors in preview harness.
  }
}

function saveAuthForm(): void {
  const state: AuthFormState = {
    gatewayUrl: authGatewayInput.value.trim(),
    appSlug: authAppSlugInput.value.trim(),
    codeParam: authCodeParamInput.value.trim(),
  };
  try {
    localStorage.setItem(AUTH_FORM_STORAGE_KEY, JSON.stringify(state));
  } catch {
    // Ignore storage write errors in preview harness.
  }
}

function loadSavedAuthForm(): void {
  const state = parseJsonObject<AuthFormState>(
    (() => {
      try {
        return localStorage.getItem(AUTH_FORM_STORAGE_KEY);
      } catch {
        return null;
      }
    })(),
  );
  if (!state) {
    return;
  }

  if (state.gatewayUrl) authGatewayInput.value = state.gatewayUrl;
  if (state.appSlug) authAppSlugInput.value = state.appSlug;
  if (state.codeParam) authCodeParamInput.value = state.codeParam;
}

function setAuthStatus(message: string, state: AuthStatusState = "idle"): void {
  authStatus.textContent = message;
  if (state === "idle") {
    authStatus.removeAttribute("data-state");
    return;
  }
  authStatus.setAttribute("data-state", state);
}

function normalizeCodeParam(value: string): string {
  const trimmed = value.trim();
  if (!trimmed) {
    return "gateway_code";
  }
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(trimmed)) {
    throw new Error("Auth code param must be a valid query key");
  }
  return trimmed;
}

function clearAuthParamsFromUrl(codeParam: string, clearHash = false): void {
  const url = new URL(window.location.href);
  url.searchParams.delete(codeParam);
  url.searchParams.delete("code");
  url.searchParams.delete("state");
  url.searchParams.delete("error");
  url.searchParams.delete("error_description");
  if (clearHash) {
    url.hash = "";
  }

  const nextUrl = `${url.pathname}${url.search}${url.hash}`;
  window.history.replaceState({}, document.title, nextUrl);
}

function normalizeGatewayUrl(value: string): string {
  const trimmed = value.trim().replace(/\/+$/g, "");
  if (!trimmed) {
    throw new Error("Auth Gateway URL is required before logging in");
  }

  try {
    const parsed = new URL(trimmed);
    if (parsed.protocol !== "https:" && parsed.protocol !== "http:") {
      throw new Error("gateway must use https or http");
    }
    return parsed.toString().replace(/\/+$/g, "");
  } catch {
    throw new Error("Auth Gateway URL must be a valid URL");
  }
}

function getRedirectUri(): string {
  return `${window.location.origin}${window.location.pathname}`;
}

async function startLoginRedirect(): Promise<void> {
  try {
    const gatewayUrl = normalizeGatewayUrl(authGatewayInput.value);
    const appSlug = authAppSlugInput.value.trim();
    if (!appSlug) {
      throw new Error("Auth App Slug is required before logging in");
    }

    saveAuthForm();
    setAuthStatus("Redirecting to auth gateway...");

    const startUrl = new URL(`${gatewayUrl}/start`);
    startUrl.searchParams.set("app", appSlug);
    startUrl.searchParams.set("return_to", getRedirectUri());
    window.location.assign(startUrl.toString());
  } catch (error) {
    setAuthStatus(`Login setup failed: ${toErrorMessage(error)}`, "error");
  }
}

async function exchangeGatewayCode(
  gatewayUrl: string,
  appSlug: string,
  gatewayCode: string,
): Promise<string> {
  const requestBody = {
    code: gatewayCode,
    app_slug: appSlug,
    request_hasura_claims: true,
  };

  const response = await fetch(`${gatewayUrl}/v1/auth/exchange`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Accept: "application/json",
    },
    body: JSON.stringify(requestBody),
  });

  if (!response.ok) {
    const body = (await response.text()).slice(0, 250);
    throw new Error(`Gateway exchange failed (${response.status}) ${body}`);
  }

  const payload = (await response.json()) as Record<string, unknown>;
  const accessToken =
    typeof payload.access_token === "string" ? payload.access_token.trim() : "";

  if (!accessToken) {
    throw new Error("Gateway response missing access_token");
  }
  return accessToken;
}

async function tryHandleAuthRedirect(): Promise<void> {
  const codeParam = normalizeCodeParam(authCodeParamInput.value);
  const hash = window.location.hash.startsWith("#")
    ? window.location.hash.slice(1)
    : "";
  if (hash) {
    const hashParams = new URLSearchParams(hash);
    const implicitToken = hashParams.get("access_token")?.trim() || "";
    if (implicitToken) {
      authTokenInput.value = implicitToken;
      saveToken(implicitToken);
      setAuthStatus("Login complete. Access token loaded.", "ok");
      clearAuthParamsFromUrl(codeParam, true);
      return;
    }
  }

  const callbackUrl = new URL(window.location.href);
  const gatewayCode = callbackUrl.searchParams.get(codeParam)?.trim() || "";
  const authError = callbackUrl.searchParams.get("error")?.trim() || "";
  const authErrorDescription =
    callbackUrl.searchParams.get("error_description")?.trim() || "";

  if (authError) {
    setAuthStatus(
      authErrorDescription || `Login failed: ${authError}`,
      "error",
    );
    clearAuthParamsFromUrl(codeParam, true);
    return;
  }

  if (!gatewayCode) {
    return;
  }

  try {
    const gatewayUrl = normalizeGatewayUrl(authGatewayInput.value);
    const appSlug = authAppSlugInput.value.trim();
    if (!appSlug) {
      throw new Error("Auth App Slug is required for login callback");
    }

    setAuthStatus("Exchanging login code for access token (requesting Hasura claims)...");
    const accessToken = await exchangeGatewayCode(gatewayUrl, appSlug, gatewayCode);

    authTokenInput.value = accessToken;
    saveToken(accessToken);
    setAuthStatus("Login complete. Access token loaded.", "ok");
  } catch (error) {
    setAuthStatus(`Login callback failed: ${toErrorMessage(error)}`, "error");
  } finally {
    clearAuthParamsFromUrl(codeParam, true);
  }
}

async function mountFromForm() {
  if (currentAbort) {
    currentAbort.abort();
    currentAbort = null;
  }
  if (typeof currentCleanup === "function") {
    currentCleanup();
    currentCleanup = undefined;
  }

  const abortController = new AbortController();
  currentAbort = abortController;

  const moduleDefinition = createCmsModuleDefinition(MODULE_KEY);

  const props = {
    title: "NiFi Flow Runner (Local)",
    inputPlaceholder: "Enter message payload...",
    parametersLabel: "Parameters (JSON object)",
    parametersPlaceholder: "{\n  \"tenant\": \"internal\",\n  \"priority\": \"normal\"\n}",
    defaultParametersJson: "{\n  \"tenant\": \"internal\",\n  \"priority\": \"normal\"\n}",
    resultLabel: "Latest Result",
    submitLabel: "Run Flow",
    assistantLabel: "Flow Result",
    maxMessages: 50,
    requestCommand: "mfe.nifi.flow.send",
    async: {
      enabled: true,
      mode: "kafka-graphql-bridge",
      requestChannel: "graphql.async.requests.v1",
      responseChannel: "graphql.async.responses.v1",
      correlationIdPath: "publish_async_request.request_id",
    },
    graphql: {
      httpUrl: httpUrlInput.value.trim(),
      wsUrl: wsUrlInput.value.trim(),
      authToken: authTokenInput.value.trim(),
      hasuraRole: "",
      submitMutation:
        "mutation PublishAsyncRequest($input: json!) { publish_async_request(input: $input) }",
      submitVariables: {
        input: {
          handler: "batch-dataflow",
          operation: "nifi.flow.invoke",
          payload: {
            message: "{{message}}",
            parameters: "{{parameters}}",
            conversationId: "{{conversationId}}",
          },
          metadata: {
            moduleKey: "{{moduleKey}}",
            instanceId: "{{instanceId}}",
            source: "{{source}}",
            asyncMode: "{{asyncMode}}",
            requestChannel: "{{requestChannel}}",
            responseChannel: "{{responseChannel}}",
            correlationIdPath: "{{correlationIdPath}}",
          },
          expires_in_seconds: 86400,
        },
      },
      submitRequestIdPath: "publish_async_request.request_id",
      streamSubscription:
        "subscription StreamClientAsyncMessage($requestId: String!, $responseChannel: String!) { graphql_client_async_messages(where: { _and: [{ request_id: { _eq: $requestId } }, { kafka_topic: { _eq: $responseChannel } }] }, order_by: { updated_at: desc }, limit: 1) { request_id status response_payload error_payload completed_at updated_at } }",
      streamVariables: {
        requestId: "{{requestId}}",
        responseChannel: "{{responseChannel}}",
      },
      streamTextPath: "graphql_client_async_messages.0.response_payload",
      streamDonePath: "graphql_client_async_messages.0.status",
      streamErrorPath: "graphql_client_async_messages.0.error_payload",
      streamChunkMode: "replace",
      conversationId: conversationIdInput.value.trim(),
    },
  };

  const maybeCleanup = await moduleDefinition.mount({
    element: host,
    moduleKey: MODULE_KEY,
    props,
    signal: abortController.signal,
    environment: {
      source: "local-preview",
      cacheKey: "local-preview",
      contentHash: "",
    },
  });

  saveToken(authTokenInput.value.trim());
  currentCleanup = typeof maybeCleanup === "function" ? maybeCleanup : undefined;
}

for (const input of [
  authGatewayInput,
  authAppSlugInput,
  authCodeParamInput,
]) {
  input.addEventListener("change", () => {
    saveAuthForm();
  });
}

authTokenInput.addEventListener("change", () => {
  saveToken(authTokenInput.value.trim());
});

applyButton.addEventListener("click", () => {
  void mountFromForm();
});

let activeTheme: ThemeMode = initializeTheme();

themeToggleButton.addEventListener("click", () => {
  activeTheme = activeTheme === "auto" ? "light" : activeTheme === "light" ? "dark" : "auto";
  saveTheme(activeTheme);
  applyTheme(activeTheme);
});

try {
  const media = window.matchMedia("(prefers-color-scheme: dark)");
  const handleSystemThemeChange = () => {
    if (activeTheme === "auto") {
      applyTheme("auto");
    }
  };
  if (typeof media.addEventListener === "function") {
    media.addEventListener("change", handleSystemThemeChange);
  } else if (typeof media.addListener === "function") {
    media.addListener(handleSystemThemeChange);
  }
} catch {
  // Ignore unavailable matchMedia support in preview harness.
}

loginButton.addEventListener("click", () => {
  void startLoginRedirect();
});

clearTokenButton.addEventListener("click", () => {
  authTokenInput.value = "";
  saveToken("");
  setAuthStatus("Token cleared", "ok");
  void mountFromForm();
});

async function bootstrap() {
  await tryHandleAuthRedirect();
  await mountFromForm();
}

void bootstrap();
