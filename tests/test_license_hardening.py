import asyncio
import importlib
import os
import time
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from app.config import get_settings, validate_license_secret, validate_runtime_settings
from app.license_rate_limit import MemoryLicenseRateLimiter
from app.license_state import ChallengeConsumed, ChallengeRecord, MemoryLicenseStateStore
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

    def test_malformed_and_oversized_requests_do_not_call_google(self):
        with patch.dict(os.environ, _env(LICENSE_SESSION_BODY_MAX_BYTES="32"), clear=False):
            client = self._client()
            self.assertEqual(client.post("/license/session", content=b"{bad").status_code, 400)
            self.assertEqual(client.post("/license/session", data="x" * 128).status_code, 413)
            self.assertEqual(CountingVerifier.calls, 0)

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
