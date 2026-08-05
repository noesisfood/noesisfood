import importlib
import json
import os
import subprocess
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

from app.config import get_settings
from app.licensing import (
    COOKIE_NAME,
    LicenseError,
    create_challenge,
    create_session_cookie,
    verify_challenge,
    verify_decoded_integrity,
    verify_session_cookie,
)


SECRET = "test-license-secret-with-enough-entropy-0123456789abcdef"


def _env(**extra):
    base = {
        "APP_ACCESS_LOCKDOWN_ENABLED": "false",
        "PLAY_INTEGRITY_ENFORCEMENT_ENABLED": "true",
        "PLAY_INTEGRITY_PACKAGE_NAME": "com.noesisfood.app",
        "PLAY_INTEGRITY_MIN_VERSION_CODE": "9",
        "LICENSE_SESSION_SECRET": SECRET,
        "LICENSE_STATE_BACKEND": "memory",
        "LICENSE_SESSION_TTL_SECONDS": "900",
        "PUBLIC_BASE_URL": "https://noesisfood.app",
        "CORS_ALLOWED_ORIGINS": "https://noesisfood.app",
    }
    base.update(extra)
    return base


def _decoded(request_hash, **overrides):
    now_ms = int(time.time() * 1000)
    data = {
        "requestDetails": {
            "requestPackageName": "com.noesisfood.app",
            "requestHash": request_hash,
            "timestampMillis": now_ms,
        },
        "appIntegrity": {
            "packageName": "com.noesisfood.app",
            "versionCode": 9,
            "appRecognitionVerdict": "PLAY_RECOGNIZED",
        },
        "accountDetails": {"appLicensingVerdict": "LICENSED"},
    }
    for path, value in overrides.items():
        section, key = path.split("__", 1)
        data[section][key] = value
    return data


class FakeVerifier:
    decoded = None
    error = None
    calls = 0

    async def decode(self, integrity_token):
        self.__class__.calls += 1
        if self.error:
            raise self.error
        if integrity_token == "malformed":
            raise LicenseError("malformed token")
        return self.decoded


class LicensingTests(unittest.TestCase):
    def test_valid_licensed_verdict(self):
        with patch.dict(os.environ, _env(), clear=False):
            settings = get_settings()
            challenge = create_challenge(settings=settings)
            verify_decoded_integrity(_decoded(challenge.request_hash), challenge.request_hash, settings=settings)

    def test_rejects_unlicensed_and_unevaluated_verdicts(self):
        with patch.dict(os.environ, _env(), clear=False):
            settings = get_settings()
            challenge = create_challenge(settings=settings)
            for verdict in ("UNLICENSED", "UNEVALUATED"):
                with self.subTest(verdict=verdict):
                    with self.assertRaises(LicenseError):
                        verify_decoded_integrity(
                            _decoded(challenge.request_hash, accountDetails__appLicensingVerdict=verdict),
                            challenge.request_hash,
                            settings=settings,
                        )

    def test_rejects_wrong_package_request_hash_version_recognition_and_stale_timestamp(self):
        with patch.dict(os.environ, _env(), clear=False):
            settings = get_settings()
            challenge = create_challenge(settings=settings)
            cases = [
                _decoded(challenge.request_hash, requestDetails__requestPackageName="wrong"),
                _decoded("wronghash"),
                _decoded(challenge.request_hash, appIntegrity__packageName="wrong"),
                _decoded(challenge.request_hash, appIntegrity__versionCode=8),
                _decoded(challenge.request_hash, appIntegrity__appRecognitionVerdict="UNRECOGNIZED_VERSION"),
                _decoded(challenge.request_hash, requestDetails__timestampMillis=1),
            ]
            for decoded in cases:
                with self.subTest(decoded=decoded):
                    with self.assertRaises(LicenseError):
                        verify_decoded_integrity(decoded, challenge.request_hash, settings=settings)

    def test_expired_and_modified_challenge_fail(self):
        with patch.dict(os.environ, _env(LICENSE_CHALLENGE_TTL_SECONDS="1"), clear=False):
            settings = get_settings()
            challenge = create_challenge(settings=settings, now=100)
            with self.assertRaises(LicenseError):
                verify_challenge(challenge.challenge_token, settings=settings, now=102)
            with self.assertRaises(LicenseError):
                verify_challenge(challenge.challenge_token + "x", settings=settings, now=100)

    def test_valid_expired_and_modified_session_cookie(self):
        with patch.dict(os.environ, _env(), clear=False):
            settings = get_settings()
            cookie, _exp = create_session_cookie(_decoded("a" * 43), settings=settings, now=100)
            self.assertEqual(verify_session_cookie(cookie, settings=settings, now=101)["vc"], 9)
            with self.assertRaises(LicenseError):
                verify_session_cookie(cookie, settings=settings, now=1001)
            with self.assertRaises(LicenseError):
                verify_session_cookie(cookie + "x", settings=settings, now=101)

    def _client(self):
        from app.main import app
        from app.api.routes.license import get_integrity_verifier
        from app.license_rate_limit import MemoryLicenseRateLimiter, get_license_rate_limiter
        from app.license_state import MemoryLicenseStateStore, get_license_state_store

        FakeVerifier.error = None
        FakeVerifier.calls = 0
        store = MemoryLicenseStateStore()
        limiter = MemoryLicenseRateLimiter()
        app.dependency_overrides[get_integrity_verifier] = lambda: FakeVerifier()
        app.dependency_overrides[get_license_state_store] = lambda: store
        app.dependency_overrides[get_license_rate_limiter] = lambda: limiter
        app.state.license_state_store_override = store
        self.addCleanup(lambda: [app.dependency_overrides.pop(dep, None) for dep in [
            get_integrity_verifier,
            get_license_state_store,
            get_license_rate_limiter,
        ]])
        self.addCleanup(lambda: hasattr(app.state, "license_state_store_override") and delattr(app.state, "license_state_store_override"))
        return TestClient(app, base_url="https://noesisfood.app"), app, [
            get_integrity_verifier,
            get_license_state_store,
            get_license_rate_limiter,
        ]

    def test_session_endpoint_issues_secure_cookie_and_protected_routes_require_it(self):
        with patch.dict(os.environ, _env(APP_ACCESS_LOCKDOWN_ENABLED="true"), clear=False):
            client, app, deps = self._client()
            challenge = client.get("/license/challenge").json()
            FakeVerifier.decoded = _decoded(challenge["request_hash"])
            response = client.post(
                "/license/session",
                json={"integrity_token": "ok", "challenge_token": challenge["challenge_token"]},
            )
            self.assertEqual(response.status_code, 200)
            cookie = response.cookies.get(COOKIE_NAME)
            self.assertTrue(cookie)
            self.assertIn("httponly", response.headers["set-cookie"].lower())
            self.assertIn("secure", response.headers["set-cookie"].lower())
            self.assertEqual(client.get("/scan/5201005073111").status_code, 200)
            for dep in deps:
                app.dependency_overrides.pop(dep, None)

    def test_malformed_token_and_verifier_error_are_rejected(self):
        with patch.dict(os.environ, _env(), clear=False):
            client, app, deps = self._client()
            challenge = client.get("/license/challenge").json()
            FakeVerifier.decoded = _decoded(challenge["request_hash"])
            self.assertEqual(
                client.post(
                    "/license/session",
                    json={"integrity_token": "malformed", "challenge_token": challenge["challenge_token"]},
                ).status_code,
                403,
            )
            challenge = client.get("/license/challenge").json()
            FakeVerifier.error = LicenseError("network")
            self.assertEqual(
                client.post(
                    "/license/session",
                    json={"integrity_token": "ok", "challenge_token": challenge["challenge_token"]},
                ).status_code,
                403,
            )
            FakeVerifier.error = None
            for dep in deps:
                app.dependency_overrides.pop(dep, None)

    def test_feature_flags_off_preserve_browser_app_access_and_enforcement_off_blocks_session_creation(self):
        with patch.dict(os.environ, _env(APP_ACCESS_LOCKDOWN_ENABLED="false", PLAY_INTEGRITY_ENFORCEMENT_ENABLED="false"), clear=False):
            client, app, deps = self._client()
            self.assertEqual(client.get("/").status_code, 200)
            self.assertIn("NoesisFood", client.get("/").text)
            challenge = client.get("/license/challenge").json()
            self.assertEqual(
                client.post(
                    "/license/session",
                    json={"integrity_token": "ok", "challenge_token": challenge["challenge_token"]},
                ).status_code,
                503,
            )
            for dep in deps:
                app.dependency_overrides.pop(dep, None)

    def test_lockdown_browser_gets_landing_and_private_apis_are_unauthorized(self):
        with patch.dict(os.environ, _env(APP_ACCESS_LOCKDOWN_ENABLED="true"), clear=False):
            client, _app, _dep = self._client()
            self.assertIn("Buy on Google Play", client.get("/").text)
            protected = [
                ("GET", "/scan/5201005073111"),
                ("POST", "/scan/manual"),
                ("POST", "/scan/photo"),
                ("POST", "/feedback/correction"),
            ]
            for method, path in protected:
                with self.subTest(path=path):
                    self.assertEqual(client.request(method, path, json={}).status_code, 401)

    def test_public_routes_remain_accessible_under_lockdown(self):
        with patch.dict(os.environ, _env(APP_ACCESS_LOCKDOWN_ENABLED="true"), clear=False):
            client, _app, _dep = self._client()
            for path in ["/privacy", "/data-deletion", "/.well-known/assetlinks.json", "/manifest.webmanifest", "/health", "/license/challenge"]:
                with self.subTest(path=path):
                    self.assertEqual(client.get(path).status_code, 200)

    def test_cors_uses_explicit_origin_with_credentials(self):
        with patch.dict(os.environ, _env(CORS_ALLOWED_ORIGINS="https://noesisfood.app,https://local.test"), clear=False):
            import app.main as main

            importlib.reload(main)
            client = TestClient(main.app, base_url="https://noesisfood.app")
            response = client.options(
                "/license/status",
                headers={
                    "Origin": "https://noesisfood.app",
                    "Access-Control-Request-Method": "GET",
                },
            )
            self.assertEqual(response.headers.get("access-control-allow-origin"), "https://noesisfood.app")
            self.assertNotEqual(response.headers.get("access-control-allow-origin"), "*")


class LicensingStaticTests(unittest.TestCase):
    def _landing_head(self):
        content = Path("app/frontend/landing.html").read_text(encoding="utf-8")
        return content[content.index("<head>"):content.index("</head>")]

    def _landing_early_catcher_script(self):
        head = self._landing_head()
        start = head.index("<script>") + len("<script>")
        end = head.index("</script>", start)
        return head[start:end]

    def _run_landing_handler(self, events, fetch_ok=True, json_ok=True):
        harness = f"""
const script = {json.dumps(self._landing_early_catcher_script())};
const events = {json.dumps(events)};
const fetches = [];
const statuses = [];
let reloaded = false;
const window = {{
  listeners: {{}},
  location: {{
    reload() {{
      reloaded = true;
    }},
  }},
  addEventListener(name, callback) {{
    this.listeners[name] = this.listeners[name] || [];
    this.listeners[name].push(callback);
  }},
  postMessage() {{
    throw new Error("window.postMessage must not be called");
  }},
}};
const document = {{
  querySelector(selector) {{
    if (selector !== "[data-license-status]") return null;
    return {{
      setAttribute(name, value) {{
        statuses.push([name, value]);
      }},
    }};
  }},
}};
const localStorage = {{
  getItem() {{ return null; }},
  setItem() {{}},
}};
const sessionStorage = {{
  getItem() {{ return null; }},
  setItem() {{
    throw new Error("sessionStorage must not be used");
  }},
}};
const indexedDB = {{
  open() {{
    throw new Error("IndexedDB must not be used");
  }},
}};
global.fetch = async function (url, options) {{
  fetches.push({{
    url,
    credentials: options && options.credentials,
    method: options && options.method,
    bodyKeys: Object.keys(JSON.parse(options.body)).sort(),
    contentType: options && options.headers && options.headers["Content-Type"],
    accept: options && options.headers && options.headers.Accept,
  }});
  return {{
    ok: {json.dumps(fetch_ok)},
    json: async () => {{
      if (!{json.dumps(json_ok)}) throw new Error("bad json");
      return {{ ok: true }};
    }},
  }};
}};
global.window = window;
global.document = document;
global.localStorage = localStorage;
global.sessionStorage = sessionStorage;
global.indexedDB = indexedDB;
eval(script);
function makePort(label) {{
  return {{
    label,
    onmessage: null,
    addEventListener(name, callback) {{
      if (name === "message") this.onmessage = callback;
    }},
    postMessage() {{}},
    start() {{}},
    deliver(data) {{
      if (typeof this.onmessage === "function") {{
        this.onmessage({{ type: "message", data, ports: [], origin: "" }});
      }}
    }},
  }};
}}
for (const event of events) {{
  const ports = event.omitPorts
    ? undefined
    : (event.invalidPort ? [{{}}] : (event.withPort ? [makePort(event.portLabel || "port")] : []));
  for (const callback of window.listeners.message || []) {{
    callback({{
      type: "message",
      origin: event.origin,
      data: event.data,
      ports,
      source: null,
    }});
  }}
  if (ports && ports[0] && typeof ports[0].deliver === "function") {{
    const nativeMessages = event.nativeMessages || (
      Object.prototype.hasOwnProperty.call(event, "nativeData") ? [event.nativeData] : []
    );
    for (const nativeData of nativeMessages) ports[0].deliver(nativeData);
  }}
}}
setTimeout(() => {{
  console.log(JSON.stringify({{
    fetches,
    statuses,
    reloaded,
    globalKeys: Object.keys(window).sort(),
  }}));
}}, 0);
"""
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", suffix=".js", delete=False) as temp:
            temp.write(harness)
            temp_path = temp.name
        try:
            result = subprocess.run(
                ["node", temp_path],
                check=True,
                capture_output=True,
                text=True,
            )
        finally:
            Path(temp_path).unlink(missing_ok=True)
        return json.loads(result.stdout)

    def _native_license_message(self, **overrides):
        data = {
            "type": "noesisfood.license.integrityToken",
            "version": 1,
            "integrityToken": "integrity-token",
            "challengeToken": "abc_DEF-123.XYZ_789-a",
        }
        data.update(overrides)
        return json.dumps(data)

    def _native_first_twa_delivery(self, native_data, **overrides):
        event = {
            "origin": "android-app://noesisfood.app",
            "data": "",
            "withPort": True,
            "nativeData": native_data,
        }
        event.update(overrides)
        return event

    def test_landing_contains_el_en_de_fr_and_browser_purchase_flow(self):
        content = Path("app/frontend/landing.html").read_text(encoding="utf-8")
        for text in ["Buy on Google Play", "Αγορά στο Google Play", "Bei Google Play kaufen", "Acheter sur Google Play"]:
            self.assertIn(text, content)
        self.assertIn("https://play.google.com/store/apps/details?id=com.noesisfood.app", content)
        self.assertNotIn("/scan/photo", content)
        self.assertNotIn('<script src="/static/license-bootstrap.js" defer></script>', content)

    def test_landing_installs_native_first_session_handler_in_head(self):
        content = Path("app/frontend/landing.html").read_text(encoding="utf-8")
        head = self._landing_head()
        self.assertLess(head.index('trustedOrigin = "android-app://noesisfood.app"'), head.index("<title>"))
        self.assertIn('window.addEventListener("message", function (event)', head)
        self.assertIn("event.origin !== trustedOrigin", head)
        self.assertNotIn('event.origin !== "https://noesisfood.app"', head)
        self.assertIn('responseType = "noesisfood.license.integrityToken"', head)
        self.assertNotIn("noesisfood.license.channelReady", head)
        self.assertIn("data.version !== 1", head)
        self.assertIn("parseMessage(event.data)", head)
        self.assertIn("!event.ports || event.ports.length < 1", head)
        self.assertIn('typeof port.postMessage !== "function"', head)
        self.assertIn("port.onmessage = handleNativeMessage", head)
        self.assertIn('typeof port.start === "function"', head)
        self.assertIn('fetch("/license/session"', head)
        self.assertIn('credentials: "include"', head)
        self.assertIn("integrity_token", head)
        self.assertIn("challenge_token", head)
        self.assertNotIn("/license/challenge", head)
        self.assertNotIn("window.postMessage(", head)
        self.assertNotIn("localStorage.setItem", head)
        self.assertNotIn("sessionStorage", head)
        self.assertNotIn("indexedDB", head)

    def test_landing_accepts_only_exact_android_app_origin(self):
        accepted = self._run_landing_handler([
            self._native_first_twa_delivery(self._native_license_message())
        ])
        self.assertEqual(len(accepted["fetches"]), 1)

        rejected_origins = [
            "https://noesisfood.app",
            "null",
            "",
            "android-app://com.noesisfood.app",
            "android-app://noesisfood.app.evil",
        ]
        for origin in rejected_origins:
            with self.subTest(origin=origin):
                rejected = self._run_landing_handler([
                    self._native_first_twa_delivery(self._native_license_message(), origin=origin)
                ])
                self.assertEqual(rejected["fetches"], [])

    def test_real_native_first_twa_port_message_posts_session(self):
        result = self._run_landing_handler([
            self._native_first_twa_delivery(self._native_license_message())
        ])
        self.assertEqual(len(result["fetches"]), 1)
        self.assertEqual(result["fetches"][0]["url"], "/license/session")

    def test_landing_rejects_wrong_type_version_malformed_json_and_missing_channel(self):
        rejected_messages = [
            "",
            "ordinary string",
            "{",
            json.dumps({"type": "wrong", "version": 1, "integrityToken": "x", "challengeToken": "a.b"}),
            self._native_license_message(version=2),
        ]
        for data in rejected_messages:
            with self.subTest(data=data):
                result = self._run_landing_handler([
                    self._native_first_twa_delivery(data)
                ])
                self.assertEqual(result["fetches"], [])

        no_port = self._run_landing_handler([
            self._native_first_twa_delivery(self._native_license_message(), withPort=False)
        ])
        self.assertEqual(no_port["fetches"], [])
        missing_ports = self._run_landing_handler([
            self._native_first_twa_delivery(self._native_license_message(), omitPorts=True)
        ])
        self.assertEqual(missing_ports["fetches"], [])
        invalid_port = self._run_landing_handler([
            self._native_first_twa_delivery(self._native_license_message(), invalidPort=True)
        ])
        self.assertEqual(invalid_port["fetches"], [])

    def test_landing_rejects_malformed_empty_oversized_and_invalid_tokens(self):
        long_token = "x" * 9000
        invalid_cases = [
            {"integrityToken": ""},
            {"integrityToken": long_token},
            {"challengeToken": ""},
            {"challengeToken": "one"},
            {"challengeToken": ".right"},
            {"challengeToken": "left."},
            {"challengeToken": "a.b.c"},
            {"challengeToken": "a b.c"},
            {"challengeToken": "a+b.c"},
        ]
        for override in invalid_cases:
            with self.subTest(override=override):
                result = self._run_landing_handler([
                    self._native_first_twa_delivery(self._native_license_message(**override))
                ])
                self.assertEqual(result["fetches"], [])

    def test_landing_posts_session_with_snake_case_credentials_include_and_reloads(self):
        result = self._run_landing_handler([
            self._native_first_twa_delivery(self._native_license_message())
        ])
        self.assertEqual(len(result["fetches"]), 1)
        self.assertEqual(result["fetches"][0]["url"], "/license/session")
        self.assertEqual(result["fetches"][0]["method"], "POST")
        self.assertEqual(result["fetches"][0]["credentials"], "include")
        self.assertEqual(result["fetches"][0]["bodyKeys"], ["challenge_token", "integrity_token"])
        self.assertEqual(result["fetches"][0]["contentType"], "application/json")
        self.assertEqual(result["fetches"][0]["accept"], "application/json")
        self.assertTrue(result["reloaded"])

    def test_landing_starts_only_one_session_request_even_after_failure(self):
        event = self._native_first_twa_delivery(self._native_license_message())
        event["nativeMessages"] = [self._native_license_message(), self._native_license_message()]
        result = self._run_landing_handler([
            event,
        ], fetch_ok=False)
        self.assertEqual(len(result["fetches"]), 1)
        self.assertFalse(result["reloaded"])

    def test_landing_failed_session_or_response_parse_does_not_reload(self):
        denied = self._run_landing_handler([
            self._native_first_twa_delivery(self._native_license_message())
        ], fetch_ok=False)
        self.assertEqual(len(denied["fetches"]), 1)
        self.assertFalse(denied["reloaded"])

        malformed_success = self._run_landing_handler([
            self._native_first_twa_delivery(self._native_license_message())
        ], json_ok=False)
        self.assertEqual(len(malformed_success["fetches"]), 1)
        self.assertFalse(malformed_success["reloaded"])

    def test_landing_normal_browser_remains_passive(self):
        result = self._run_landing_handler([
            self._native_first_twa_delivery(self._native_license_message(), origin="https://noesisfood.app")
        ])
        self.assertEqual(result["fetches"], [])

    def test_landing_handler_keeps_tokens_out_of_global_storage_and_dom(self):
        content = Path("app/frontend/landing.html").read_text(encoding="utf-8")
        head = self._landing_head()
        self.assertNotIn("__NOESISFOOD_LICENSE_PORT_STATE__", head)
        self.assertNotIn("localStorage.setItem", head)
        self.assertNotIn("sessionStorage", head)
        self.assertNotIn("indexedDB", head)
        self.assertNotIn("innerHTML", head)
        self.assertNotIn("textContent = integrityToken", head)
        self.assertNotIn("textContent = challengeToken", head)
        self.assertNotIn("location.href", head)
        self.assertNotIn("history.pushState", head)
        self.assertNotIn("history.replaceState", head)
        self.assertNotIn("/license/challenge", content)
        self.assertNotIn("noesisfood.license.challenge", content)
        self.assertNotIn("window.postMessage(", content)

    def test_service_worker_cache_migration_and_private_cache_exclusions(self):
        content = Path("app/frontend/service-worker.js").read_text(encoding="utf-8")
        self.assertIn('noesisfood-shell-v3', content)
        self.assertNotIn('"/",', content)
        for path in ["/scan", "/scan/photo", "/feedback/correction", "/license"]:
            self.assertIn(path, content)
        self.assertIn("caches.delete", content)

    def test_assetlinks_preserves_fingerprints_and_relations(self):
        content = Path("app/frontend/.well-known/assetlinks.json").read_text(encoding="utf-8")
        self.assertIn("delegate_permission/common.handle_all_urls", content)
        self.assertIn("delegate_permission/common.use_as_origin", content)
        self.assertIn("AF:A2:CC:DA:B9:DD:41:24:17:6D:70:58:00:8F:41:52:52:91:71:11:7A:25:D1:61:2E:C6:A4:EA:34:A2:A7:B9", content)
        self.assertIn("9D:BA:01:6F:DE:67:0D:E2:65:D4:B2:08:BF:B6:16:5A:8F:07:1C:00:14:CA:A6:6A:0D:80:8C:B0:FF:94:6A:EB", content)

    def test_private_app_is_noindexed_and_legal_links_remain(self):
        index = Path("app/frontend/index.html").read_text(encoding="utf-8")
        self.assertIn('name="robots" content="noindex,nofollow,noarchive"', index)
        landing = Path("app/frontend/landing.html").read_text(encoding="utf-8")
        self.assertIn('href="/privacy"', landing)
        self.assertIn('href="/data-deletion"', landing)
