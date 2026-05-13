# Secured Async Service Setup (Group-Based Access)

This document explains how to stand up a new async service behind `publish_async_request` with group-based authorization, without hardcoding per-service logic in GraphQL or MFE code.

## Goal

- Keep async transport generic (`publish_async_request` stays shared).
- Restrict specific handlers/operations by role.
- Drive role membership from Google Groups.
- Use backend token exchange for MFE audience shaping.

## Repos and Control Points

- MFE runtime/config: `/Users/christopherlyons/GitHub/example-mfe`
- GraphQL async transport + policy sync: `/Users/christopherlyons/GitHub/k8s-graphql-server`
- Keycloak realm/config + Google group mapping: `/Users/christopherlyons/GitHub/k8s-keycloak`
- Token exchange backend: `/Users/christopherlyons/GitHub/keycloak-auth-gateway`

## End-to-End Flow

1. User logs in via Keycloak external realm (Google IdP).
2. Google group membership maps to Keycloak client roles.
3. Token includes Hasura claims (`x-hasura-allowed-roles`, default role).
4. MFE calls auth gateway `/v1/auth/token-exchange` for required audience(s).
5. MFE submits `publish_async_request` with `handler` + `operation`.
6. Async request publisher calls Gravitee policy endpoint.
7. Policy allows/denies based on labels + token roles.
8. Request is published only when authorized.

## Step 1: Define the Service Role

Choose a role name for the secured service operation, for example:

- `ai_user`
- `reporting_user`
- `payments_approver`

Use client role scope under GraphQL API client (not a random hardcoded app role).

## Step 2: Map Google Group -> Role (Vault)

Update Vault secret used by Keycloak Google group mapping:

- Secret key: `keycloak-google-group-role-map`
- Value format (JSON object):

```json
{
  "ai-testers@suncoast.systems": ["client:graphql-api:<role-name>"]
}
```

Example:

```json
{
  "ai-testers@suncoast.systems": ["client:graphql-api:ai_user"]
}
```

Notes:

- Use the **actual** GraphQL API client id in your environment if it is suffixed (for example `graphql-api-xxxxxxx`).
- Keycloak configurator logic auto-creates roles referenced in this map when possible.

## Step 3: Ensure Keycloak Claims Mapping Is Active

Keycloak must emit:

- `https://hasura.io/jwt/claims.x-hasura-allowed-roles`
- `https://hasura.io/jwt/claims.x-hasura-default-role`
- `https://hasura.io/jwt/claims.x-hasura-user-id`

In this platform, that is already configured through the `graphql-api` client mappers and configurator.

## Step 4: Configure Token Exchange Backend App

Each MFE/shell origin should be registered in auth gateway apps (`/v1/apps`).

Minimum:

- `slug`
- `base_url`
- `enabled=true`

MFE will call:

- `POST /v1/auth/token-exchange`

with:

- `app_slug`
- `requested_audience` or `requested_audiences`
- bearer token

Do not do browser-direct token exchange with a public client.

## Step 5: Annotate the Target API in Gravitee

On the service/API definition labels, add:

- `hasura.async.handler=<handler-name>`
- `hasura.action.roles=<default-roles>`
- `hasura.async.operation.roles.<operation>=<restricted-role-list>`

Example:

```yaml
gravitee.io/definition-labels: "internal,hasura,hasura.async.handler=ai-service,hasura.action.roles=user|service,hasura.async.operation.roles.chat.completion=ai_user"
```

Meaning:

- `ai-service` handler exists for generic async routing.
- default handler usage can allow `user|service` (optional baseline).
- `chat.completion` operation is restricted to `ai_user`.

## Step 6: MFE Configuration

In module props (`graphql.tokenExchange`):

- `enabled: true`
- `exchangeUrl: https://login.suncoast.systems/v1/auth/token-exchange`
- `appSlug: <registered-app-slug>`
- `requestedAudience` (or `requestedAudiences`) for GraphQL/Hasura audience

In submit variables:

- set `input.handler` to your handler label value.
- set `input.operation` to your operation label suffix.

Important:

- `graphql.hasuraRole` is deprecated/ignored in async mode.
- Do not force `X-Hasura-Role` from the MFE.

## Step 7: Deploy Order

1. Vault group-role mapping update.
2. Keycloak/configurator sync.
3. Gravitee service label update.
4. GraphQL sync/policy refresh.
5. MFE config deploy.

If role mapping is new, wait for Keycloak/group cache propagation before final testing.

## Step 8: Verification Checklist

1. Token claims for allowed user include the restricted role in `x-hasura-allowed-roles`.
2. Token claims for denied user do not include it.
3. Positive test (group member): async request accepted and response streamed.
4. Negative test (non-member): async request denied with policy error.

Optional policy check from cluster:

- Call `http://graphql-gravitee-sync.graphql.svc.cluster.local:8080/async-policy/authorize`
- Use payload with `role`, `handler`, `operation`, and `allowed_roles`.

## Common Failure Modes

- `invalid_client` on token exchange:
  - Exchange attempted with public client directly; use backend exchange client.
- `Authentication hook unauthorized this request`:
  - audience/claims mismatch or wrong exchanged token used.
- `no subscriptions exist`:
  - role used by websocket lacks subscription permission on `graphql.client_async_messages`.
- `role not allowed for requested async route`:
  - Gravitee labels or group-role mapping do not include required role.
- `token exchange failed`:
  - gateway client secret/config mismatch, app slug not registered/enabled, or upstream Keycloak exchange failure.

## Operational Guardrails

1. Keep async transport generic; enforce service security through policy labels + claims.
2. Avoid hardcoding service roles in MFE runtime.
3. Prefer operation-level restrictions (`hasura.async.operation.roles.*`) over global handler-wide broad grants.
4. Keep `ASYNC_POLICY_FAIL_OPEN=false` for secure default.
5. Treat all client secrets as Vault-only; never commit to manifests.

## Template for New Secured Service

1. Create Google group: `<service>-users@...`
2. Add Vault mapping:
   - `"<group>": ["client:graphql-api:<service_role>"]`
3. Add/verify API labels:
   - `hasura.async.handler=<service-handler>`
   - `hasura.async.operation.roles.<service.operation>=<service_role>`
4. Set MFE token exchange for required audience.
5. Use matching `handler` + `operation` in `publish_async_request` payload.
6. Verify allow/deny with one user in group and one user outside group.
