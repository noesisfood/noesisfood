# Android License Protection Rollout

This rollout keeps production behavior unchanged until feature flags are changed in Render.

## Flags

- `APP_ACCESS_LOCKDOWN_ENABLED=false`: browsers and the current PWA keep existing access.
- `PLAY_INTEGRITY_ENFORCEMENT_ENABLED=false`: `/license/session` does not issue sessions.
- `PLAY_INTEGRITY_ENFORCEMENT_ENABLED=true`: backend verifies Google Play Integrity tokens, but public browser access is still unchanged while lockdown is false.
- `APP_ACCESS_LOCKDOWN_ENABLED=true`: root serves the public landing page unless a valid licensed session cookie is present, and private APIs require that cookie.
- `APP_ACCESS_LOCKDOWN_ENABLED=true` with `PLAY_INTEGRITY_ENFORCEMENT_ENABLED=false` is refused at startup.
- `LICENSE_STATE_BACKEND=redis` is required for production Play Integrity enforcement. Local tests may use `memory`.

## Render Secrets

Set these as Render environment variables, not source files:

- `LICENSE_SESSION_SECRET`: long random value generated with a secret manager.
- `LICENSE_STATE_BACKEND=redis`
- `REDIS_URL`: Redis-compatible instance used for one-time challenges, rate limits, decode concurrency and logout revocation.
- `PLAY_INTEGRITY_CLOUD_PROJECT_NUMBER`: numeric Google Cloud project number linked to Play Integrity.
- `GOOGLE_APPLICATION_CREDENTIALS`: only if Render uses a mounted secret file path.
- Google service-account JSON: store only in Render secret storage or use Application Default Credentials; never commit it.

Keep:

- `PLAY_INTEGRITY_PACKAGE_NAME=com.noesisfood.app`
- `PLAY_INTEGRITY_MIN_VERSION_CODE=9`
- `LICENSE_SESSION_TTL_SECONDS=900`
- `LICENSE_CHALLENGE_TTL_SECONDS=120`
- `LICENSE_CHALLENGE_MAX_ATTEMPTS=1`
- `LICENSE_CHALLENGE_RATE_LIMIT_PER_MINUTE=10`
- `LICENSE_SESSION_RATE_LIMIT_PER_MINUTE=5`
- `LICENSE_SESSION_BODY_MAX_BYTES=16384`
- `PLAY_INTEGRITY_MAX_CONCURRENT_DECODES=5`
- `INVALID_TOKEN_DENY_TTL_SECONDS=60`
- `PUBLIC_BASE_URL=https://noesisfood.app`
- `CORS_ALLOWED_ORIGINS=https://noesisfood.app`

## Server-Side State

Challenges are one-time-use. The backend stores each challenge id, expected request hash, issue time and expiry in the configured state backend.

Production Redis consumption uses `GETDEL` as the atomic operation. If `GETDEL` is unavailable, the implementation uses a Lua script that performs `GET` and `DEL` atomically on the Redis server. A duplicate or concurrent request sees a missing challenge and fails before any Google decode call. Google or network failure consumes the challenge and the client must request a fresh one.

The same backend stores short-lived session revocations after logout until the original session expiry. No raw Integrity tokens are stored; invalid-token deny cache keys use a SHA-256 digest of the submitted token.

## Quota Protection

Public license endpoints are protected by:

- per-client challenge rate limit;
- per-client session verification rate limit;
- one-time challenge consumption before decode;
- invalid-token deny cache;
- global concurrent decode cap;
- request body-size limit for `/license/session`.

Client identifiers are hashed before limiter storage. `X-Forwarded-For` is only considered according to `TRUSTED_PROXY_HOPS`.

## Google Cloud

1. Link the NoesisFood Play app to a Google Cloud project in Play Console.
2. Enable the Play Integrity API for that project.
3. Create a service account with permission to call the Play Integrity decode API.
4. Configure Render with Application Default Credentials or the service-account JSON secret.
5. Put the cloud project number into Android build configuration for the protected build.

## Play Console

1. Keep the existing listing published.
2. Upload only after local debug validation and manual code review.
3. Use Managed Publishing for versionCode 9.
4. Publish versionName `1.0.8`, versionCode `9`, package `com.noesisfood.app`.
5. Verify a clean Google Play install receives versionCode 9 before enabling lockdown.

## Internal Testing

1. Build and install a debug/internal test artifact configured with `PLAY_INTEGRITY_CLOUD_PROJECT_NUMBER`.
2. Deploy backend code with both flags false.
3. Enable `PLAY_INTEGRITY_ENFORCEMENT_ENABLED=true` in a test environment first.
4. Confirm the TWA receives a challenge, returns an integrity token, and gets a session cookie.
5. Confirm `/scan/{key}`, `/scan/manual`, `/scan/photo`, and `/feedback/correction` work with the licensed session.
6. Confirm a normal browser still sees the existing app while lockdown is false.

## Production Cutover

Enable in this order only after versionCode 9 is approved and verified from Google Play:

1. Set `PLAY_INTEGRITY_ENFORCEMENT_ENABLED=true`.
2. Verify session creation from a clean Play-installed TWA.
3. Verify scan, manual, photo/OCR, scoring, allergens, dietary signals, usage context, safety sources, history, and feedback.
4. Set `APP_ACCESS_LOCKDOWN_ENABLED=true`.
5. Verify ordinary browsers see only the landing page.
6. Verify private APIs return 401 without a licensed session.

## Rollback

Immediate rollback is one environment change:

1. Set `APP_ACCESS_LOCKDOWN_ENABLED=false`.
2. If session creation is also failing, set `PLAY_INTEGRITY_ENFORCEMENT_ENABLED=false`.
3. Restart the Render service if the platform does not apply environment changes live.
4. Root and private APIs return to pre-lockdown behavior with the code still deployed.

## Known Risk

The pinned `androidbrowserhelper:2.6.2` launcher does not expose a stable public session accessor for postMessage. The Android implementation first tries a public getter if present, then uses a narrowly isolated reflective accessor for the current TWA launcher session. Replacing this with an official accessor should be prioritized when the pinned TWA stack can be upgraded.
