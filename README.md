# NiFi Flow MFE (Suncoast Contract)

Example micro-frontend package that:

- follows the Suncoast UI module contract (`module.definition.json`)
- includes a host adapter for the current shell `CmsModuleDefinition` contract
- builds to a single browser JS file (`dist/nifi-flow-mfe.js`)
- provides Directus seed data for `cms_modules`
- demonstrates GraphQL async communication:
  - submit via GraphQL HTTP mutation
  - receive streamed response via GraphQL WS subscription

## Quick Start

```bash
npm install
npm run dev
```

Local preview URL:

- `http://localhost:4173/preview/` (default)

Build production artifact:

```bash
npm run build
```

Build output:

- `dist/nifi-flow-mfe.js` (single-file bundle)
- `dist/nifi-flow-mfe.js.map`
- `dist/module.definition.json`

## GraphQL Runtime Configuration

GraphQL endpoint/token values are runtime-resolved. They are not baked into the built bundle.

Resolution order:

1. `props.graphql.*` from CMS block config
2. Shell runtime values (`#cms-root` data attrs / `window.__SUNCOAST_RUNTIME__.graphql`)
3. Browser auth storage fallback for token (for preview helper flows)

Session lifecycle ownership:

- Token refresh scheduling and session-expiry warning UI are owned by the shell runtime.
- This MFE only consumes shell auth globals (for example `window.__SUNCOAST_AUTH__`) before making requests.
- For template guidance on logout events and local auth-state cleanup, see `MFE_AUTH_INTEGRATION_REFERENCE.md`.

Build metadata debug helper (devtools):

- `window.__SUNCOAST_GET_MFE_BUILD_INFO__("mfe-nifi-flow-runner")`
- Returns compile-time metadata for the loaded bundle (module version, build version, commit, timestamp, mode).
- `window.__SUNCOAST_GET_MFE_BUILD_INFO__()` returns all loaded MFE build entries by module key.
- Lookup is exact-key first, then case-insensitive.

Optional token exchange:

- Configure `graphql.tokenExchange` to exchange the shell bearer token to the audience this MFE needs before GraphQL submit/stream calls.
- Preferred: set `graphql.tokenExchange.exchangeUrl` to your backend exchange endpoint (for example `https://login.suncoast.systems/v1/auth/token-exchange`) so the browser never calls Keycloak token exchange directly.
- When `exchangeUrl` is set, the MFE performs gateway exchange even if the source token already contains the requested audience (so gateway can still mint/augment required claims).
- If `tokenExchange.tokenUrl` / `tokenExchange.clientId` are omitted, the module falls back to shell auth runtime values (`data-auth-token-url`, `data-auth-client-id`, `window.__SUNCOAST_AUTH__.config`).

Local preview:

1. Copy `.env.local.example` to `.env.local`
2. Set optional preview auth values
3. Run `npm run dev`

Supported env vars:

- `MFE_PREVIEW_AUTH_GATEWAY_URL` (preview login gateway, usually `https://login.suncoast.systems`)
- `MFE_PREVIEW_AUTH_APP_SLUG` (registered app slug in auth-gateway, for example `nifi-flow-mfe-dev` for local/preview and `nifi-flow-mfe-prod` for production)
- `MFE_PREVIEW_AUTH_CODE_PARAM` (query key returned by gateway callback, default `gateway_code`)
- `MFE_PREVIEW_PORT` (dev only)

## Local Preview Login

The local preview page (`/preview/`) now includes a login helper that uses the shared auth-gateway flow:

1. Fill `Auth Gateway URL` and `Auth App Slug` (or set `MFE_PREVIEW_AUTH_*` env vars).
2. Click `Login` on the preview page.
3. Gateway returns to `/preview/` with a one-time code (`gateway_code` by default).
4. Preview exchanges that code at `/v1/auth/exchange` and auto-fills `Auth Token`.
   - Preview requests Hasura claims (`request_hasura_claims: true`), and gateway resolves audience server-side.
5. Click `Apply / Remount` to use that token for GraphQL HTTP/WS requests.

If your auth provider returns `access_token` in URL hash (implicit flow), the preview page will capture that too.

## What This MFE Does

- Module key: `mfe-nifi-flow-runner`
- Renders a form UI for `message` + JSON `parameters`
- Submits flow requests using GraphQL mutation `publish_async_request`
- Listens for response row updates from `graphql.client_async_messages` over GraphQL subscription
- Shows the latest raw submit + stream payload in a result panel
- Emits module events

## Directus Setup

1. Host `dist/nifi-flow-mfe.js` at a URL reachable by your shell runtime.
2. Create/update a `cms_modules` record using `directus/cms-module.seed.json`.
3. In a `cms_block_module` block, choose module key `mfe-nifi-flow-runner`.
4. Use `directus/cms-block-module.props.example.json` as your `props_json` baseline.
5. Set:
   - optional `graphql.httpUrl` and `graphql.wsUrl` overrides (leave unset to use shell runtime defaults)
   - optional `graphql.authToken` override (usually leave unset and let shell runtime auth provide token)
   - `graphql.hasuraRole` is deprecated in async mode and ignored by runtime (role comes from token claims)
   - optional `graphql.tokenExchange.*`:
     - `enabled`
     - `requestedAudience` (set this to the Hasura/GraphQL audience expected by auth hook)
     - `requestedAudiences` (optional array; request multiple audiences in one exchanged token)
     - `requestedScope` (optional)
     - `exchangeUrl` (recommended; dedicated backend token exchange endpoint)
     - `appSlug` (required for backend exchange mode)
     - `tokenUrl` (optional; defaults from shell auth runtime)
     - `clientId` (optional; defaults from shell auth runtime)
   - `graphql.submitMutation` and `graphql.streamSubscription` (defaults are preconfigured for `publish_async_request` + `graphql_client_async_messages`)
   - optional async transport hints:
     - `async.mode` (`none`, `graphql-stream`, `kafka-graphql-bridge`, `request-response`, `subscribe`, `mixed`)
     - `async.requestChannel`
     - `async.responseChannel`
     - `async.correlationIdPath`
   - path mappings:
     - `graphql.submitRequestIdPath`
     - `graphql.streamTextPath`
     - `graphql.streamDonePath`
     - `graphql.streamErrorPath`

`async.requestChannel`, `async.responseChannel`, and `async.correlationIdPath` are consumed by this NiFi Flow MFE and injected into submit/stream template variables and emitted events.

## Automated Registry Publish (No Directus/GitOps PR Required)

`publish.yml` now publishes module artifacts directly to the module registry service `POST /v1/modules/publish` using multipart upload.

1. Add repository variable(s) or secret(s):
   - `MODULE_REGISTRY_SERVICE_URL_PREVIEW`
   - optional legacy fallback: `MODULE_REGISTRY_SERVICE_URL`
2. Add repository secrets for module registry publish API auth:
   - `MODULE_REGISTRY_SERVICE_GOOGLE_SERVICE_ACCOUNT_EMAIL`
   - `MODULE_REGISTRY_SERVICE_GOOGLE_SERVICE_ACCOUNT_PRIVATE_KEY`
   - `MODULE_REGISTRY_SERVICE_GOOGLE_TOKEN_AUDIENCE`
3. Optional repository variable:
   - `MODULE_REGISTRY_SERVICE_PUBLISH_PATH` (default `/v1/modules/publish`)
4. Publish tag `v*` (or run Publish workflow manually).
5. Workflow will:
   - build artifacts
   - run `npm run publish:registry` to generate `dist/module.publish.json`
   - run `npm run notify:catalog` to upload `dist/nifi-flow-mfe.js` and `dist/module.publish.json` to the registry service
   - attach release artifacts in GitHub

`publish:registry` writes metadata in `dist/module.publish.json`:

- `module_version`
- `published_at`
- `release.tag`
- `release.sha`
- `bundle.sha256`
- full `definition` + `seed` documents

Module registry service endpoint called by workflow:

- `POST <resolved-service-url><MODULE_REGISTRY_SERVICE_PUBLISH_PATH>`
- Default path: `/v1/modules/publish`
- Channel defaults:
  - all publishes use `preview` (tag and manual)
- Service URL selection:
  - publish workflow always targets `MODULE_REGISTRY_SERVICE_URL_PREVIEW` (fallback `MODULE_REGISTRY_SERVICE_URL`)
  - manual dispatch input `registry_target` is restricted to `preview`

## Important Runtime Note

This repo provides the MFE contract + bundle. Your shell runtime must include or load this module definition at runtime.

If your shell currently only mounts modules from an internal registry, wire this module using the exported host adapter:

- export: `createCmsModuleDefinition()`

The bundle also self-registers at:

- `globalThis.SuncoastMfeRegistry["mfe-nifi-flow-runner"]`

## Scripts

- `npm run clean` - remove `dist`
- `npm run clean:dev` - remove `dev-dist`
- `npm run typecheck` - TS type check
- `npm run build` - compile single JS + copy module definition
- `npm run dev` - local preview server with live rebuild + preview harness
- `npm run publish:registry` - generate local publish manifest from built artifacts
- `npm run notify:catalog` - upload built bundle + publish manifest to module registry API
- `npm run sync:directus` - direct Directus upsert (optional; only if network access exists)

## GitHub Actions

Workflows included:

- `.github/workflows/ci.yml`
  - runs typecheck/build on push + PR
  - uploads dist artifacts
- `.github/workflows/publish.yml`
  - runs on `v*` tags or manual dispatch
  - manual inputs:
    - `build_mode` (`production`/`local`)
    - `registry_target` (`preview`)
  - builds bundle + publish metadata
  - uploads artifacts
  - creates a GitHub Release for tag pushes

Secrets expected by publish workflow:

- `MODULE_REGISTRY_SERVICE_GOOGLE_SERVICE_ACCOUNT_EMAIL`
- `MODULE_REGISTRY_SERVICE_GOOGLE_SERVICE_ACCOUNT_PRIVATE_KEY`
- `MODULE_REGISTRY_SERVICE_GOOGLE_TOKEN_AUDIENCE`

Repository variables used by publish workflow:

- `MODULE_REGISTRY_SERVICE_URL_PREVIEW`
- `MODULE_REGISTRY_SERVICE_URL` (legacy fallback)
- `MODULE_REGISTRY_SERVICE_PUBLISH_PATH` (optional, default `/v1/modules/publish`)
