import asyncio
import importlib
import json
import os
import time
import unittest
from unittest.mock import patch

from fastapi import Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.testclient import TestClient

from app.config import get_settings, validate_license_secret, validate_runtime_settings
from app.license_rate_limit import MemoryLicenseRateLimiter
from app.license_state import (
    ChallengeConsumed,
    ChallengeRecord,
    ChallengeUnavailable,
    MemoryLicenseStateStore,
    RedisLicenseStateStore,
)
from app.licensing import COOKIE_NAME, LicenseError, create_challenge


SECRET = "test-license-secret-with-enough-entropy-0123456789abcdef"


def _env(**extra):
    base = {
        "APP_ACCESS_LOCKDOWN_ENABLED": "true",
        "PLAY_INTEGRITY_ENFORCEMENT_ENABLED": "true",
        "PLAY_INTEGRITY_PACKAGE_NAME": "com.noesisfood.app",
        "PLAY_INTEGRITY_MIN_VERSION_CODE": "9",
        "LICENSE_SESSION_SECRET": SECRET,
        "LICENSE_STATE_BACKEND": "memory",
        "LICENSE_CHALLENGE_RATE_LIMIT_PER_MINUTE": "20",
        "LICENSE_SESSION_RATE_LIMIT_PER_MINUTE": "20",
        "LICENSE_SESSION_BODY_MAX_BYTES": "16384",
        "PLAY_INTEGRITY_MAX_CONCURRENT_DECODES": "5",
        "INVALID_TOKEN_DENY_TTL_SECONDS": "60",
    }
    base.update(extra)
    return base


def _decoded(request_hash):
    return {
        "requestDetails": {
            "requestPackageName": "com.noesisfood.app",
            "requestHash": request_hash,
            "timestampMillis": int(time.time() * 1000),
        },
        "appIntegrity": {
            "packageName": "com.noesisfood.app",
            "versionCode": 9,
            "appRecognitionVerdict": "PLAY_RECOGNIZED",
        },
        "accountDetails": {"appLicensingVerdict": "LICENSED"},
    }


class CountingVerifier:
    decoded = None
    error = None
    calls = 0

    async def decode(self, integrity_token):
        self.__class__.calls += 1
        if self.error:
            raise self.error
        return self.decoded


class ConflictStore:
    def __init__(self, error):
        self.error = error

    async def store_challenge(self, record):
        return None

    async def consume_challenge(self, challenge_id):
        raise self.error

    async def revoke_session(self, session_id, expires_at):
        return None

    async def is_session_revoked(self, session_id):
        return False


class FakeRedis:
    def __init__(self, payload):
        self.payload = payload

    async def getdel(self, key):
        return self.payload


def _redis_store_with_payload(payload):
    store = RedisLicenseStateStore.__new__(RedisLicenseStateStore)
    store._redis = FakeRedis(payload)
    store._prefix = "nf:license"
    return store


class ChallengeStoreTests(unittest.IsolatedAsyncioTestCase):
    async def test_challenge_is_accepted_once_and_second_use_rejected(self):
        store = MemoryLicenseStateStore()
        record = ChallengeRecord("cid", "hash", 1, int(time.time()) + 60, 1)
        await store.store_challenge(record)
        self.assertEqual((await store.consume_challenge("cid")).request_hash, "hash")
        with self.assertRaises(ChallengeConsumed):
            await store.consume_challenge("cid")

    async def test_two_simultaneous_claims_allow_exactly_one_success(self):
        store = MemoryLicenseStateStore()
        await store.store_challenge(ChallengeRecord("cid", "hash", 1, int(time.time()) + 60, 1))

        async def claim():
            try:
                await store.consume_challenge("cid")
                return True
            except ChallengeConsumed:
                return False

        results = await asyncio.gather(claim(), claim())
        self.assertEqual(results.count(True), 1)
        self.assertEqual(results.count(False), 1)

    async def test_memory_missing_and_expired_states_have_bounded_reasons(self):
        store = MemoryLicenseStateStore()
        with self.assertRaises(ChallengeConsumed) as missing:
            await store.consume_challenge("missing")
        self.assertEqual(missing.exception.reason_code, "memory_missing_or_consumed")

        store = MemoryLicenseStateStore()
        await store.store_challenge(ChallengeRecord("cid", "hash", 1, 100, 1))
        with patch("app.license_state.time.time", side_effect=[100, 101]):
            with self.assertRaises(ChallengeUnavailable) as expired:
                await store.consume_challenge("cid")
        self.assertEqual(expired.exception.reason_code, "memory_expired_state")

    async def test_redis_state_failures_have_bounded_reasons(self):
        with self.assertRaises(ChallengeConsumed) as missing:
            await _redis_store_with_payload(None).consume_challenge("missing")
        self.assertEqual(missing.exception.reason_code, "redis_missing_or_consumed")

        with self.assertRaises(ChallengeUnavailable) as malformed:
            await _redis_store_with_payload("not-json").consume_challenge("malformed")
        self.assertEqual(malformed.exception.reason_code, "redis_malformed_state")

        expired_payload = json.dumps(
            {
                "challenge_id": "expired",
                "request_hash": "hash",
                "issued_at": 1,
                "expires_at": 1,
                "max_attempts": 1,
            }
        )
        with patch("app.license_state.time.time", return_value=2):
            with self.assertRaises(ChallengeUnavailable) as expired:
                await _redis_store_with_payload(expired_payload).consume_challenge("expired")
        self.assertEqual(expired.exception.reason_code, "redis_expired_state")


class LicenseHardeningRouteTests(unittest.TestCase):
    def setUp(self):
        self.store = MemoryLicenseStateStore()
        self.limiter = MemoryLicenseRateLimiter()
        CountingVerifier.calls = 0
        CountingVerifier.error = None

    def _client(self):
        import app.main as main
        from app.api.routes.license import get_integrity_verifier
        from app.license_rate_limit import get_license_rate_limiter
        from app.license_state import get_license_state_store

        main = importlib.reload(main)
        app = main.app
        app.dependency_overrides[get_integrity_verifier] = lambda: CountingVerifier()
        app.dependency_overrides[get_license_state_store] = lambda: self.store
        app.dependency_overrides[get_license_rate_limiter] = lambda: self.limiter
        app.state.license_state_store_override = self.store
        self._deps = [get_integrity_verifier, get_license_state_store, get_license_rate_limiter]
        return TestClient(app, base_url="https://noesisfood.app")

    def tearDown(self):
        from app.main import app

        for dep in getattr(self, "_deps", []):
            app.dependency_overrides.pop(dep, None)
        if hasattr(app.state, "license_state_store_override"):
            delattr(app.state, "license_state_store_override")

    def _challenge(self, client):
        response = client.get("/license/challenge")
        self.assertEqual(response.status_code, 200)
        return response.json()

    @staticmethod
    def _request_with_headers(headers, body=b""):
        async def receive():
            return {"type": "http.request", "body": body, "more_body": False}

        return Request(
            {
                "type": "http",
                "http_version": "1.1",
                "method": "POST",
                "scheme": "https",
                "path": "/license/session",
                "raw_path": b"/license/session",
                "query_string": b"",
                "headers": [(key.encode(), value.encode()) for key, value in headers.items()],
                "client": ("test", 1),
                "server": ("test", 443),
                "root_path": "",
            },
            receive,
        )

    def test_malformed_content_length_middleware_logs_fixed_reason_and_keeps_response(self):
        import app.main as main

        request = self._request_with_headers({"content-length": "invalid"})

        async def call_next(_request):
            self.fail("middleware must reject malformed content length")

        with self.assertLogs("noesisfood.license", level="WARNING") as captured:
            response = asyncio.run(main.license_session_body_limit_middleware(request, call_next))
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.body, b"Malformed license request")
        self.assertEqual(len(captured.records), 1)
        self.assertEqual(captured.records[0].getMessage(), "license_session_bad_request reason=malformed_content_length_middleware")

    def test_malformed_content_length_route_logs_fixed_reason_and_keeps_response(self):
        from app.api.routes import license as license_route

        with patch.dict(os.environ, _env(), clear=False):
            request = self._request_with_headers({"content-length": "invalid"})
            settings = get_settings()
            with self.assertLogs("noesisfood.license", level="WARNING") as captured:
                with self.assertRaises(Exception) as raised:
                    asyncio.run(
                        license_route.session(
                            request,
                            Response(),
                            settings=settings,
                            verifier=CountingVerifier(),
                            store=self.store,
                            limiter=self.limiter,
                        )
                    )
            self.assertEqual(raised.exception.status_code, 400)
            self.assertEqual(raised.exception.detail, "Malformed license request")
            self.assertEqual(len(captured.records), 1)
            self.assertEqual(captured.records[0].getMessage(), "license_session_bad_request reason=malformed_content_length_route")

    def test_request_validation_error_logs_fixed_reason_and_keeps_response(self):
        import app.main as main

        request = self._request_with_headers({})
        with self.assertLogs("noesisfood.license", level="WARNING") as captured:
            response = asyncio.run(main.validation_exception_handler(request, RequestValidationError([])))
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.body, b"Malformed license request")
        self.assertEqual(len(captured.records), 1)
        self.assertEqual(captured.records[0].getMessage(), "license_session_bad_request reason=request_validation_error")

    def test_malformed_json_route_logs_fixed_reason_and_keeps_response(self):
        from app.api.routes import license as license_route

        with patch.dict(os.environ, _env(), clear=False):
            request = self._request_with_headers({}, b"{bad")
            with self.assertLogs("noesisfood.license", level="WARNING") as captured:
                with self.assertRaises(Exception) as raised:
                    asyncio.run(
                        license_route.session(
                            request,
                            Response(),
                            settings=get_settings(),
                            verifier=CountingVerifier(),
                            store=self.store,
                            limiter=self.limiter,
                        )
                    )
            self.assertEqual(raised.exception.status_code, 400)
            self.assertEqual(raised.exception.detail, "Malformed license request")
            self.assertEqual(len(captured.records), 1)
            self.assertEqual(captured.records[0].getMessage(), "license_session_bad_request reason=malformed_json_body")

    def test_consumed_challenge_does_not_call_google_again(self):
        with patch.dict(os.environ, _env(), clear=False):
            client = self._client()
            challenge = self._challenge(client)
            CountingVerifier.decoded = _decoded(challenge["request_hash"])
            self.assertEqual(
                client.post("/license/session", json={"integrity_token": "ok", "challenge_token": challenge["challenge_token"]}).status_code,
                200,
            )
            self.assertEqual(CountingVerifier.calls, 1)
            self.assertEqual(
                client.post("/license/session", json={"integrity_token": "ok", "challenge_token": challenge["challenge_token"]}).status_code,
                409,
            )
            self.assertEqual(CountingVerifier.calls, 1)

    def test_each_conflict_reason_logs_once_without_changing_generic_409(self):
        reasons = [
            (ChallengeConsumed("missing", "memory_missing_or_consumed"), "memory_missing_or_consumed"),
            (ChallengeUnavailable("expired", "memory_expired_state"), "memory_expired_state"),
            (ChallengeConsumed("missing", "redis_missing_or_consumed"), "redis_missing_or_consumed"),
            (ChallengeUnavailable("malformed", "redis_malformed_state"), "redis_malformed_state"),
            (ChallengeUnavailable("expired", "redis_expired_state"), "redis_expired_state"),
        ]
        for error, reason in reasons:
            with self.subTest(reason=reason), patch.dict(os.environ, _env(), clear=False):
                client = self._client()
                challenge = self._challenge(client)
                self.store = ConflictStore(error)
                with self.assertLogs("noesisfood.license", level="WARNING") as captured:
                    response = client.post(
                        "/license/session",
                        json={"integrity_token": "ok", "challenge_token": challenge["challenge_token"]},
                    )
                self.assertEqual(response.status_code, 409)
                self.assertEqual(response.json(), {"detail": "License challenge is no longer valid"})
                self.assertEqual(len(captured.records), 1)
                self.assertEqual(captured.records[0].getMessage(), f"license_session_conflict reason={reason}")
                self.assertNotIn("integrity_token", captured.records[0].getMessage())
                self.assertNotIn("challenge_token", captured.records[0].getMessage())
                self.assertNotIn(challenge["challenge_token"], captured.records[0].getMessage())

    def test_malformed_and_oversized_requests_do_not_call_google(self):
        with patch.dict(os.environ, _env(LICENSE_SESSION_BODY_MAX_BYTES="32"), clear=False):
            client = self._client()
            with self.assertLogs("noesisfood.license", level="WARNING") as captured:
                malformed = client.post("/license/session", content=b"{bad")
            self.assertEqual(malformed.status_code, 400)
            self.assertEqual(malformed.text, "Malformed license request")
            self.assertEqual(len(captured.records), 1)
            self.assertEqual(captured.records[0].getMessage(), "license_session_bad_request reason=request_validation_error")
            self.assertEqual(client.post("/license/session", data="x" * 128).status_code, 413)
            self.assertEqual(CountingVerifier.calls, 0)

    def test_missing_fields_log_distinct_fixed_reasons_and_keep_400(self):
        with patch.dict(os.environ, _env(), clear=False):
            client = self._client()
            challenge = self._challenge(client)
            with self.assertLogs("noesisfood.license", level="WARNING") as captured_integrity:
                missing_integrity = client.post(
                    "/license/session",
                    json={"challenge_token": challenge["challenge_token"]},
                )
            self.assertEqual(missing_integrity.status_code, 400)
            self.assertEqual(missing_integrity.json(), {"detail": "Malformed license request"})
            self.assertEqual(len(captured_integrity.records), 1)
            self.assertEqual(captured_integrity.records[0].getMessage(), "license_session_bad_request reason=missing_integrity_token")

            challenge = self._challenge(client)
            with self.assertLogs("noesisfood.license", level="WARNING") as captured_challenge:
                missing_challenge = client.post(
                    "/license/session",
                    json={"integrity_token": "ok"},
                )
            self.assertEqual(missing_challenge.status_code, 400)
            self.assertEqual(missing_challenge.json(), {"detail": "Malformed license request"})
            self.assertEqual(len(captured_challenge.records), 1)
            self.assertEqual(captured_challenge.records[0].getMessage(), "license_session_bad_request reason=missing_challenge_token")

    def test_challenge_and_session_rate_limits_do_not_call_google(self):
        with patch.dict(os.environ, _env(LICENSE_CHALLENGE_RATE_LIMIT_PER_MINUTE="1", LICENSE_SESSION_RATE_LIMIT_PER_MINUTE="1"), clear=False):
            client = self._client()
            challenge = self._challenge(client)
            self.assertEqual(client.get("/license/challenge").status_code, 429)
            CountingVerifier.decoded = _decoded(challenge["request_hash"])
            self.assertEqual(
                client.post("/license/session", json={"integrity_token": "ok", "challenge_token": challenge["challenge_token"]}).status_code,
                200,
            )
            second = create_challenge(settings=get_settings())
            self.assertEqual(
                client.post("/license/session", json={"integrity_token": "ok", "challenge_token": second.challenge_token}).status_code,
                429,
            )
            self.assertEqual(CountingVerifier.calls, 1)

    def test_invalid_token_deny_cache_blocks_repeated_token_before_google(self):
        with patch.dict(os.environ, _env(), clear=False):
            client = self._client()
            challenge = self._challenge(client)
            CountingVerifier.decoded = _decoded("wrong")
            self.assertEqual(
                client.post("/license/session", json={"integrity_token": "bad-token", "challenge_token": challenge["challenge_token"]}).status_code,
                403,
            )
            second = self._challenge(client)
            self.assertEqual(
                client.post("/license/session", json={"integrity_token": "bad-token", "challenge_token": second["challenge_token"]}).status_code,
                403,
            )
            self.assertEqual(CountingVerifier.calls, 1)

    def test_logout_revokes_copied_session_cookie(self):
        with patch.dict(os.environ, _env(), clear=False):
            client = self._client()
            challenge = self._challenge(client)
            CountingVerifier.decoded = _decoded(challenge["request_hash"])
            response = client.post("/license/session", json={"integrity_token": "ok", "challenge_token": challenge["challenge_token"]})
            self.assertEqual(response.status_code, 200)
            copied_cookie = response.cookies.get(COOKIE_NAME)
            self.assertTrue(copied_cookie)
            self.assertEqual(client.post("/license/logout").status_code, 200)
            fresh_client = self._client()
            self.assertEqual(fresh_client.get("/scan/5201005073111", headers={"Cookie": f"{COOKIE_NAME}={copied_cookie}"}).status_code, 401)

    def test_static_private_shell_cannot_bypass_lockdown_but_licensed_session_can_load(self):
        with patch.dict(os.environ, _env(), clear=False):
            client = self._client()
            self.assertIn("Buy on Google Play", client.get("/static/index.html").text)
            self.assertEqual(client.get("/static/ingredient_glossary.json").status_code, 404)
            challenge = self._challenge(client)
            CountingVerifier.decoded = _decoded(challenge["request_hash"])
            self.assertEqual(client.post("/license/session", json={"integrity_token": "ok", "challenge_token": challenge["challenge_token"]}).status_code, 200)
            self.assertIn("noindex,nofollow,noarchive", client.get("/static/index.html").text)

    def test_route_inventory_has_no_unclassified_routes(self):
        from app.main import app

        classified = {
            "/",
            "/privacy",
            "/data-deletion",
            "/manifest.webmanifest",
            "/service-worker.js",
            "/.well-known/assetlinks.json",
            "/health",
            "/icons",
            "/static/{asset_path:path}",
            "/scan/{key}",
            "/scan/manual",
            "/scan/photo",
            "/feedback/correction",
            "/internal/beta/feedback-summary",
            "/license/challenge",
            "/license/session",
            "/license/status",
            "/license/logout",
        }
        discovered = {route.path for route in app.routes if getattr(route, "include_in_schema", True)}
        self.assertTrue(classified.issubset(discovered))
        self.assertFalse((discovered - classified) - {"/openapi.json", "/docs", "/docs/oauth2-redirect", "/redoc"})


class ConfigurationHardeningTests(unittest.TestCase):
    def test_weak_and_placeholder_secrets_are_rejected(self):
        for value in ["secret", "changeme", "short"]:
            with self.subTest(value=value):
                with self.assertRaises(RuntimeError):
                    validate_license_secret(value)
        validate_license_secret(SECRET)

    def test_dangerous_lockdown_without_enforcement_is_rejected(self):
        with patch.dict(os.environ, _env(APP_ACCESS_LOCKDOWN_ENABLED="true", PLAY_INTEGRITY_ENFORCEMENT_ENABLED="false"), clear=False):
            with self.assertRaises(RuntimeError):
                validate_runtime_settings(get_settings())

    def test_production_enforcement_requires_redis(self):
        with patch.dict(os.environ, _env(APP_ENV="production", PLAY_INTEGRITY_ENFORCEMENT_ENABLED="true", APP_ACCESS_LOCKDOWN_ENABLED="false", LICENSE_STATE_BACKEND="memory"), clear=False):
            with self.assertRaises(RuntimeError):
                validate_runtime_settings(get_settings())

    def test_flags_false_preserve_current_behavior(self):
        with patch.dict(os.environ, _env(APP_ACCESS_LOCKDOWN_ENABLED="false", PLAY_INTEGRITY_ENFORCEMENT_ENABLED="false", LICENSE_SESSION_SECRET=""), clear=False):
            validate_runtime_settings(get_settings())
            import app.main as main

            importlib.reload(main)
            client = TestClient(main.app, base_url="https://noesisfood.app")
            self.assertIn("NoesisFood", client.get("/").text)
