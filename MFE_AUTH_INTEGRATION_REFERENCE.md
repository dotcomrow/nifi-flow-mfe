# MFE Auth Integration Reference

This repo is a template baseline for MFEs that run inside a shell-owned auth/session model.

## Ownership Model

- Shell owns login, logout, token refresh, and session-expiry UX.
- MFE consumes auth state and tokens exposed by host runtime.
- MFE may clear its own local state when sign-out is announced.

## Standard Logout Event

Use `auth-logout` as the platform-standard browser event.

- Event name: `auth-logout`
- Event payload (`event.detail`):
  - `localOnly` (`boolean`): `true` when only local auth state was cleared.
  - `at` (`number`): epoch milliseconds when logout was emitted.

Backward compatibility:

- Current shell also emits legacy `suncoast-auth-logout`.
- Template MFEs should listen to both while older shells still exist.

## Listener Pattern for MFEs

```ts
const AUTH_LOGOUT_EVENTS = ["auth-logout", "suncoast-auth-logout"] as const;

const onAuthLogout = () => {
  // Clear module-local auth/session data only.
  // Example: exchanged token cache, pending subscriptions, drafts.
};

for (const eventName of AUTH_LOGOUT_EVENTS) {
  window.addEventListener(eventName, onAuthLogout);
}

// On unmount:
for (const eventName of AUTH_LOGOUT_EVENTS) {
  window.removeEventListener(eventName, onAuthLogout);
}
```

This template module (`src/module.ts`) already implements this pattern and clears:

- token-exchange cache
- active stream subscription
- pending submit state
- input draft text

## Token Sources (Host-Provided)

Resolve runtime tokens/URLs in this order:

1. Module props (`props.graphql.*`)
2. Host runtime (`window.__SUNCOAST_RUNTIME__.graphql`, DOM data attrs)
3. Host auth API (`window.__SUNCOAST_AUTH__`)
4. Storage fallback (preview/dev only)

Do not hardcode auth endpoints or static bearer tokens in source.

## Token Exchange Guidance

- Preferred: exchange via backend endpoint (`graphql.tokenExchange.exchangeUrl`).
- Use audience/scope from config (`requestedAudience`, `requestedAudiences`, `requestedScope`).
- Keep exchanged tokens in memory cache with expiry checks; avoid persisting them to browser storage.

## Security Checklist for New MFEs

- Listen for `auth-logout` and clear module-local auth state.
- Do not persist exchanged access tokens.
- Do not bypass shell auth runtime unless explicitly required.
- Treat all auth settings as runtime config, not build-time constants.
