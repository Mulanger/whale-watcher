# Security Audit Notes

Date: 2026-05-16

Scope: `whale-watcher` dependency tree, Polymarket upstream client, Redis/Mongo publishing path, and health endpoint.

## Hardening Applied

- Upgraded Vitest to remove the vulnerable Vite/esbuild dev dependency chain.
- Added explicit upstream request header/body timeouts for Polymarket GET requests.
- Removed upstream error response bodies from thrown request errors to avoid retaining large or unexpected remote payloads.
- Added `Cache-Control: no-store` and `X-Content-Type-Options: nosniff` to `/health`.

## Residual Notes

- `npm audit` reports zero vulnerabilities.
- The health endpoint exposes operational status and recent upstream state. Prefer keeping the Railway health port/path reachable only where needed for health checks.
- Keep MongoDB and Redis on private/internal networking and avoid exposing those URLs outside Railway service configuration.

## Verification

- `npm test`
- `npm run build`
- `npm audit --json`
