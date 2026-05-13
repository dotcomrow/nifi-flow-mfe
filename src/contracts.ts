export type JsonPrimitive = string | number | boolean | null;
export type JsonValue = JsonPrimitive | JsonObject | JsonValue[];
export type JsonObject = { [key: string]: JsonValue };

export type AsyncMode =
  | "none"
  | "request-response"
  | "subscribe"
  | "mixed"
  | "graphql-stream"
  | "kafka-graphql-bridge";

export type ModuleRetryPolicy = {
  maxAttempts: number;
  initialBackoffMs: number;
  maxBackoffMs: number;
  jitter: boolean;
};

export type ModuleAsyncConfig = {
  enabled: boolean;
  mode: AsyncMode;
  requestChannel: string;
  responseChannel: string;
  correlationIdPath: string;
  request: {
    supported: boolean;
    defaultTimeoutMs: number;
    retry: ModuleRetryPolicy;
  };
  stream: {
    supported: boolean;
    transport: "none" | "graphql-ws" | "sse" | "webhook" | "kafka-bridge";
    endpointRef: string;
    authMode: "inherit" | "token" | "mtls" | "none";
    reconnect: ModuleRetryPolicy;
  };
  queue: {
    supported: boolean;
    maxInflight: number;
    dropPolicy: "backpressure" | "drop-oldest" | "drop-newest";
    orderingKey: string;
  };
};

export type ModuleEventEnvelope = {
  type: string;
  sourceModuleKey: string;
  instanceId: string;
  payload: JsonValue;
  timestamp: string;
};

export type ModuleContext = {
  moduleKey: string;
  instanceId: string;
  hostElement: HTMLElement;
  props: Record<string, unknown>;
  async: ModuleAsyncConfig;
  signal?: AbortSignal;
  environment: {
    cacheKey?: string;
    contentHash?: string;
    source?: string;
    graphql?: {
      httpUrl?: string;
      wsUrl?: string;
      authToken?: string;
    };
  };
  emit?: (event: ModuleEventEnvelope) => void;
  request?: (command: string, payload: JsonValue, timeoutMs?: number) => Promise<unknown>;
};

export type InitInput = { props?: Record<string, unknown> };
export type MountInput = { element?: HTMLElement };
export type UpdateInput = { props?: Record<string, unknown> };
export type CommandInput = { name: string; payload?: JsonValue };
export type CommandResult = { ok: boolean; data?: JsonValue; error?: string };

export interface ModuleRuntime {
  init?(input: InitInput): Promise<void> | void;
  mount(input: MountInput): Promise<void> | void;
  update?(input: UpdateInput): Promise<void> | void;
  handleCommand?(input: CommandInput): Promise<CommandResult | void> | CommandResult | void;
  suspend?(reason: string): Promise<void> | void;
  resume?(): Promise<void> | void;
  unmount(): Promise<void> | void;
}

export type ModuleFactory = (ctx: ModuleContext) => ModuleRuntime;

export type CmsModuleProps = Record<string, unknown>;

export type CmsModuleMountContext = {
  element: HTMLElement;
  moduleKey: string;
  props: CmsModuleProps;
  signal: AbortSignal;
  environment: {
    cacheKey?: string;
    contentHash?: string;
    source?: string;
    graphql?: {
      httpUrl?: string;
      wsUrl?: string;
      authToken?: string;
    };
  };
};

export type CmsModuleDefinition = {
  moduleKey: string;
  version: string;
  provider: string;
  componentType: string;
  supportsAsyncConfig: boolean;
  resolveProps?: (rawProps: unknown) => CmsModuleProps;
  mount: (
    context: CmsModuleMountContext,
  ) => void | (() => void) | Promise<void | (() => void)>;
};
