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

    def _run_landing_early_catcher(self, events):
        harness = f"""
const script = {json.dumps(self._landing_early_catcher_script())};
const events = {json.dumps(events)};
const dispatched = [];
const window = {{
  listeners: {{}},
  addEventListener(name, callback) {{
    this.listeners[name] = this.listeners[name] || [];
    this.listeners[name].push(callback);
  }},
  dispatchEvent(event) {{
    dispatched.push(event.type);
  }},
  postMessage() {{
    throw new Error("window.postMessage must not be called");
  }},
  fetch() {{
    throw new Error("fetch must not be called");
  }},
}};
class CustomEvent {{
  constructor(type) {{
    this.type = type;
  }}
}}
global.window = window;
global.CustomEvent = CustomEvent;
eval(script);
function makePort(label) {{
  return {{
    label,
    postMessage() {{}},
    start() {{}},
  }};
}}
for (const event of events) {{
  const ports = event.omitPorts ? undefined : (event.withPort ? [makePort(event.portLabel || "port")] : []);
  for (const callback of window.listeners.message || []) {{
    callback({{
      origin: event.origin,
      data: event.data,
      ports,
      source: null,
    }});
  }}
}}
const state = window.__NOESISFOOD_LICENSE_PORT_STATE__;
console.log(JSON.stringify({{
  hasPort: Boolean(state && state.port),
  portLabel: state && state.port ? state.port.label : null,
  consumed: Boolean(state && state.consumed),
  stateKeys: state ? Object.keys(state).sort() : [],
  dispatched,
}}));
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

    def _run_bootstrap_harness(self, challenge, call_consume=True):
        content = Path("app/frontend/license-bootstrap.js").read_text(encoding="utf-8")
        harness = f"""
const script = {json.dumps(content)};
const challenge = {json.dumps(challenge)};
const callConsume = {json.dumps(call_consume)};
const sent = [];
const statuses = [];
const port = {{
  postMessage(message) {{
    const parsed = JSON.parse(message);
    sent.push({{
      type: parsed.type,
      version: parsed.version,
      requestHashLength: String(parsed.requestHash || "").length,
      challengeTokenSegmentCount: String(parsed.challengeToken || "").split(".").length,
      expiresAtType: typeof parsed.expiresAt,
    }});
  }},
  start() {{
    sent.push({{ started: true }});
  }},
}};
const window = {{
  __NOESISFOOD_LICENSE_PORT_STATE__: {{ port, consumed: false }},
  listeners: {{}},
  location: {{ reload() {{ throw new Error("reload must not be called"); }} }},
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
const navigator = {{ serviceWorker: {{ getRegistrations: async () => [] }} }};
const caches = {{ keys: async () => [] }};
global.window = window;
global.document = document;
global.navigator = navigator;
global.caches = caches;
global.fetch = async function (url) {{
  if (url !== "/license/challenge") throw new Error("unexpected fetch");
  return {{
    ok: true,
    json: async () => challenge,
  }};
}};
eval(script);
if (callConsume) {{
  window.NoesisFoodLicenseBootstrap._test.consumeRetainedNativePort();
}}
setTimeout(() => {{
  console.log(JSON.stringify({{
    valid: window.NoesisFoodLicenseBootstrap._test.isValidChallenge(challenge),
    sent,
    statuses,
    consumed: window.__NOESISFOOD_LICENSE_PORT_STATE__.consumed,
    retainedPort: Boolean(window.__NOESISFOOD_LICENSE_PORT_STATE__.port),
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

    def _valid_bootstrap_challenge(self, **overrides):
        data = {
            "request_hash": "A" * 43,
            "challenge_token": "abc_DEF-123.XYZ_789-a",
            "expires_at": int(time.time()) + 300,
        }
        data.update(overrides)
        return data

    def test_landing_contains_el_en_de_fr_and_browser_purchase_flow(self):
        content = Path("app/frontend/landing.html").read_text(encoding="utf-8")
        for text in ["Buy on Google Play", "Αγορά στο Google Play", "Bei Google Play kaufen", "Acheter sur Google Play"]:
            self.assertIn(text, content)
        self.assertIn("https://play.google.com/store/apps/details?id=com.noesisfood.app", content)
        self.assertNotIn("/scan/photo", content)
        self.assertIn('<script src="/static/license-bootstrap.js" defer></script>', content)

    def test_landing_installs_early_twa_port_catcher_in_head(self):
        content = Path("app/frontend/landing.html").read_text(encoding="utf-8")
        head = self._landing_head()
        bootstrap_index = content.index('<script src="/static/license-bootstrap.js" defer></script>')
        self.assertLess(content.index("__NOESISFOOD_LICENSE_PORT_STATE__"), bootstrap_index)
        self.assertLess(head.index("__NOESISFOOD_LICENSE_PORT_STATE__"), head.index("<title>"))
        self.assertIn('window.addEventListener("message", function (event)', head)
        self.assertIn('event.origin !== "android-app://noesisfood.app"', head)
        self.assertNotIn('event.origin !== "https://noesisfood.app"', head)
        self.assertNotIn("parseMessageData", head)
        self.assertNotIn('data.type !== "noesisfood.license.channelReady"', head)
        self.assertNotIn("data.version !== 1", head)
        self.assertNotIn("event.data", head)
        self.assertIn("!event.ports || event.ports.length < 1", head)
        self.assertIn("var port = event.ports[0]", head)
        self.assertIn("state.port || state.consumed", head)
        self.assertIn("state.port = port", head)
        self.assertIn('window.dispatchEvent(new CustomEvent(readyEventName))', head)
        self.assertNotIn("/license/challenge", head)
        self.assertNotIn("window.postMessage(", head)
        self.assertNotIn("startsWith", head)
        self.assertNotIn("includes", head)
        self.assertNotIn("match(", head)
        self.assertNotIn("RegExp", head)

    def test_landing_early_catcher_accepts_only_exact_twa_app_origin(self):
        accepted = self._run_landing_early_catcher([
            {"origin": "android-app://noesisfood.app", "data": "", "withPort": True, "portLabel": "accepted"}
        ])
        self.assertTrue(accepted["hasPort"])
        self.assertEqual(accepted["portLabel"], "accepted")
        self.assertEqual(accepted["dispatched"], ["noesisfood:license-port-ready"])

        rejected_origins = [
            "https://noesisfood.app",
            "null",
            "",
            "android-app://com.noesisfood.app",
            "android-app://noesisfood.app.evil",
        ]
        for origin in rejected_origins:
            with self.subTest(origin=origin):
                rejected = self._run_landing_early_catcher([
                    {"origin": origin, "data": "", "withPort": True, "portLabel": "rejected"}
                ])
                self.assertFalse(rejected["hasPort"])
                self.assertEqual(rejected["dispatched"], [])

    def test_landing_early_catcher_accepts_valid_port_transfer_with_ignored_payload(self):
        cases = [
            "",
            "ordinary string",
            "{",
            {"unexpected": "object"},
        ]
        for data in cases:
            with self.subTest(data=data):
                result = self._run_landing_early_catcher([
                    {"origin": "android-app://noesisfood.app", "data": data, "withPort": True, "portLabel": "accepted"}
                ])
                self.assertTrue(result["hasPort"])
                self.assertEqual(result["portLabel"], "accepted")
                self.assertEqual(result["stateKeys"], ["consumed", "port"])
                self.assertEqual(result["dispatched"], ["noesisfood:license-port-ready"])

    def test_landing_early_catcher_rejects_events_without_transferred_port(self):
        for data in ["", "ordinary string", "{", {"unexpected": "object"}]:
            with self.subTest(data=data):
                result = self._run_landing_early_catcher([
                    {"origin": "android-app://noesisfood.app", "data": data, "withPort": False}
                ])
                self.assertFalse(result["hasPort"])
                self.assertEqual(result["dispatched"], [])
        missing_ports = self._run_landing_early_catcher([
            {"origin": "android-app://noesisfood.app", "data": "", "omitPorts": True}
        ])
        self.assertFalse(missing_ports["hasPort"])
        self.assertEqual(missing_ports["dispatched"], [])

    def test_landing_early_catcher_retains_first_valid_port_and_ignores_duplicates(self):
        result = self._run_landing_early_catcher([
            {"origin": "android-app://noesisfood.app", "data": "", "withPort": True, "portLabel": "first"},
            {"origin": "android-app://noesisfood.app", "data": '{"type":"wrong","version":1}', "withPort": True, "portLabel": "second"},
        ])
        self.assertTrue(result["hasPort"])
        self.assertEqual(result["portLabel"], "first")
        self.assertEqual(result["dispatched"], ["noesisfood:license-port-ready"])

    def test_landing_early_catcher_is_passive_for_normal_browser_messages(self):
        result = self._run_landing_early_catcher([
            {"origin": "https://noesisfood.app", "data": {"type": "noesisfood.license.channelReady", "version": 1}, "withPort": True}
        ])
        self.assertFalse(result["hasPort"])
        self.assertEqual(result["dispatched"], [])

    def test_landing_catcher_keeps_only_non_secret_port_state(self):
        content = Path("app/frontend/landing.html").read_text(encoding="utf-8")
        head = self._landing_head()
        self.assertIn("{ port: null, consumed: false }", head)
        self.assertNotIn("event.data", head)
        for forbidden in ["challengeToken", "integrityToken", "cookie", "credential", "requestHash"]:
            self.assertNotIn(forbidden, head)

    def test_bootstrap_rejects_invalid_origin_and_posts_session_request(self):
        content = Path("app/frontend/license-bootstrap.js").read_text(encoding="utf-8")
        self.assertIn('origin === TARGET_ORIGIN', content)
        self.assertIn('"/license/session"', content)
        self.assertIn("no-store", content)

    def test_bootstrap_uses_twa_message_port_contract(self):
        content = Path("app/frontend/license-bootstrap.js").read_text(encoding="utf-8")
        self.assertIn('const CHANNEL_READY_TYPE = "noesisfood.license.channelReady"', content)
        self.assertIn('const PORT_STATE_NAME = "__NOESISFOOD_LICENSE_PORT_STATE__"', content)
        self.assertIn('const PORT_READY_EVENT = "noesisfood:license-port-ready"', content)
        self.assertIn("consumeRetainedNativePort();", content)
        self.assertIn("window.addEventListener(PORT_READY_EVENT, consumeRetainedNativePort)", content)
        self.assertIn("const port = state.port", content)
        self.assertIn('typeof port.postMessage !== "function"', content)
        self.assertIn("state.consumed = true", content)
        self.assertIn("state.port = null", content)
        self.assertIn("nativePort = port", content)
        self.assertIn("nativePort.onmessage = handleNativePortMessage", content)
        self.assertIn('if (typeof nativePort.start === "function") nativePort.start();', content)
        self.assertIn("nativePort.postMessage(JSON.stringify({", content)
        self.assertIn("function handleNativePortMessage(event)", content)
        self.assertNotIn('window.addEventListener("message"', content)
        self.assertNotIn("window.postMessage(", content)

    def test_bootstrap_accepts_only_backend_style_two_segment_challenge_tokens(self):
        accepted = self._run_bootstrap_harness(self._valid_bootstrap_challenge(), call_consume=False)
        self.assertTrue(accepted["valid"])

        rejected_tokens = {
            "previous_three_segment_jws": "abc.DEF.ghi",
            "one_segment": "abc",
            "empty_left_segment": ".abc",
            "empty_right_segment": "abc.",
            "extra_dot": "abc.def.ghi",
            "whitespace": "abc def.ghi",
            "invalid_characters": "abc+def.ghi",
        }
        for name, token in rejected_tokens.items():
            with self.subTest(name=name):
                result = self._run_bootstrap_harness(
                    self._valid_bootstrap_challenge(challenge_token=token),
                    call_consume=False,
                )
                self.assertFalse(result["valid"])

    def test_bootstrap_challenge_validation_keeps_request_hash_and_expires_at_checks(self):
        invalid_cases = {
            "short_request_hash": {"request_hash": "A" * 42},
            "long_request_hash": {"request_hash": "A" * 44},
            "invalid_request_hash_character": {"request_hash": ("A" * 42) + "."},
            "non_numeric_expires_at": {"expires_at": "not-a-number"},
            "expired_expires_at": {"expires_at": int(time.time()) - 1},
        }
        for name, override in invalid_cases.items():
            with self.subTest(name=name):
                result = self._run_bootstrap_harness(
                    self._valid_bootstrap_challenge(**override),
                    call_consume=False,
                )
                self.assertFalse(result["valid"])

    def test_bootstrap_valid_challenge_reaches_message_port_send_path(self):
        result = self._run_bootstrap_harness(self._valid_bootstrap_challenge())
        self.assertTrue(result["valid"])
        self.assertTrue(result["consumed"])
        self.assertFalse(result["retainedPort"])
        self.assertEqual(result["statuses"], [["data-license-status", "starting"], ["data-license-status", "waiting"]])
        self.assertEqual(result["sent"][0], {"started": True})
        self.assertEqual(len(result["sent"]), 2)
        self.assertEqual(result["sent"][1]["type"], "noesisfood.license.challenge")
        self.assertEqual(result["sent"][1]["version"], 1)
        self.assertEqual(result["sent"][1]["requestHashLength"], 43)
        self.assertEqual(result["sent"][1]["challengeTokenSegmentCount"], 2)
        self.assertEqual(result["sent"][1]["expiresAtType"], "number")

    def test_bootstrap_later_port_messages_require_exact_type_and_version(self):
        content = Path("app/frontend/license-bootstrap.js").read_text(encoding="utf-8")
        handler = content[content.index("function handleNativePortMessage(event)"):content.index("async function beginNativeLicensing")]
        self.assertIn("const data = parseMessageData(event.data);", handler)
        self.assertIn("if (!data || data.version !== 1) return;", handler)
        self.assertIn("if (data.type === ERROR_TYPE)", handler)
        self.assertIn("if (data.type !== RESPONSE_TYPE) return;", handler)
        self.assertIn("challengeToken !== pendingChallengeToken", handler)
        self.assertIn("createSession(integrityToken, challengeToken)", handler)

    def test_bootstrap_consumes_retained_port_at_most_once(self):
        content = Path("app/frontend/license-bootstrap.js").read_text(encoding="utf-8")
        consume_block = content[content.index("function consumeRetainedNativePort()"):content.index("consumeRetainedNativePort();")]
        self.assertIn("state.consumed || nativeLicensingStarted", consume_block)
        self.assertIn("state.consumed = true", consume_block)
        self.assertIn("state.port = null", consume_block)
        self.assertIn("nativeLicensingStarted = true", consume_block)
        self.assertIn("beginNativeLicensing(port).catch", consume_block)

    def test_bootstrap_does_not_fetch_challenge_before_native_port_exists(self):
        content = Path("app/frontend/license-bootstrap.js").read_text(encoding="utf-8")
        load_block = content[content.index('window.addEventListener("load"'):]
        start_block = content[content.index("async start()"):content.index("_test:")]
        self.assertNotIn("requestChallenge()", load_block)
        self.assertNotIn("requestChallenge()", start_block)
        self.assertIn("const challenge = await requestChallenge();", content)
        self.assertLess(content.index("nativePort = port"), content.index("const challenge = await requestChallenge();"))
        self.assertLess(content.index("nativePort.onmessage = handleNativePortMessage"), content.index("const challenge = await requestChallenge();"))
        self.assertLess(content.index('if (typeof nativePort.start === "function") nativePort.start();'), content.index("const challenge = await requestChallenge();"))

    def test_bootstrap_successful_session_reloads_root_with_cookie(self):
        content = Path("app/frontend/license-bootstrap.js").read_text(encoding="utf-8")
        self.assertIn('credentials: "same-origin"', content)
        self.assertIn('if (!response.ok) throw new Error("session_denied");', content)
        self.assertIn("window.location.reload();", content)

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
