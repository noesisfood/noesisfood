import asyncio
import hashlib
import importlib
import json
import os
import time
import unittest
from dataclasses import replace
from unittest.mock import patch
from uuid import UUID

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


class LicenseDependencyContractTests(unittest.TestCase):
    @staticmethod
    def _session_route():
        from fastapi.routing import APIRoute

        from app.main import app

        return next(
            route
            for route in app.routes
            if isinstance(route, APIRoute) and route.path == "/license/session" and "POST" in route.methods
        )

    @staticmethod
    def _dependency_body_params(dependant):
        body_params = []
        pending = list(dependant.dependencies)
        while pending:
            dependency = pending.pop()
            body_params.extend(dependency.body_params)
            pending.extend(dependency.dependencies)
        return body_params

    @staticmethod
    def _non_enforcing_memory_settings():
        return replace(
            get_settings(),
            app_env="development",
            app_access_lockdown_enabled=False,
            play_integrity_enforcement_enabled=False,
            license_state_backend="memory",
        )

    def test_session_route_has_no_dependency_generated_body_contract(self):
        from app.main import app

        route = self._session_route()
        self.assertEqual(route.dependant.body_params, [])
        self.assertEqual(self._dependency_body_params(route.dependant), [])
        self.assertIsNone(route.body_field)

        operation = app.openapi()["paths"]["/license/session"]["post"]
        self.assertNotIn("requestBody", operation)
        self.assertNotIn("Settings", json.dumps(operation, sort_keys=True))

    def test_valid_license_object_reaches_route_guard_with_real_store_and_limiter(self):
        from app.license_rate_limit import get_license_rate_limiter
        from app.license_state import get_license_state_store
        from app.main import app

        original_overrides = dict(app.dependency_overrides)
        try:
            app.dependency_overrides.clear()
            app.dependency_overrides[get_settings] = self._non_enforcing_memory_settings
            self.assertNotIn(get_license_state_store, app.dependency_overrides)
            self.assertNotIn(get_license_rate_limiter, app.dependency_overrides)

            with TestClient(app, base_url="https://noesisfood.app") as client:
                response = client.post(
                    "/license/session",
                    json={"integrity_token": "placeholder", "challenge_token": "placeholder"},
                )

            self.assertEqual(response.status_code, 503)
            self.assertEqual(response.json(), {"detail": "Play Integrity enforcement is disabled"})
        finally:
            app.dependency_overrides.clear()
            app.dependency_overrides.update(original_overrides)

    def test_provider_selection_preserves_direct_calls_and_singletons(self):
        import app.license_rate_limit as rate_limit_module
        import app.license_state as state_module

        settings = self._non_enforcing_memory_settings()
        self.assertIs(state_module.get_license_state_store(settings), state_module._memory_store)
        self.assertIs(state_module.get_license_state_store(settings), state_module._memory_store)
        self.assertIs(rate_limit_module.get_license_rate_limiter(settings), rate_limit_module._memory_limiter)
        self.assertIs(rate_limit_module.get_license_rate_limiter(settings), rate_limit_module._memory_limiter)

        with patch.object(state_module, "get_settings", return_value=settings):
            self.assertIs(state_module.get_license_state_store(), state_module._memory_store)
        with patch.object(rate_limit_module, "get_settings", return_value=settings):
            self.assertIs(rate_limit_module.get_license_rate_limiter(), rate_limit_module._memory_limiter)

        redis_settings = replace(settings, license_state_backend="redis", redis_url="redis://placeholder")
        state_marker = object()
        with (
            patch.object(state_module, "_redis_store", None),
            patch.object(state_module, "RedisLicenseStateStore", return_value=state_marker) as state_constructor,
        ):
            self.assertIs(state_module.get_license_state_store(redis_settings), state_marker)
            self.assertIs(state_module.get_license_state_store(redis_settings), state_marker)
            state_constructor.assert_called_once_with("redis://placeholder")

        limiter_marker = object()
        with (
            patch.object(rate_limit_module, "_redis_limiter", None),
            patch.object(rate_limit_module, "RedisLicenseRateLimiter", return_value=limiter_marker) as limiter_constructor,
        ):
            self.assertIs(rate_limit_module.get_license_rate_limiter(redis_settings), limiter_marker)
            self.assertIs(rate_limit_module.get_license_rate_limiter(redis_settings), limiter_marker)
            limiter_constructor.assert_called_once_with("redis://placeholder")

    def test_zero_argument_provider_overrides_still_apply(self):
        from app.license_rate_limit import get_license_rate_limiter
        from app.license_state import get_license_state_store
        from app.main import app

        calls = []
        original_overrides = dict(app.dependency_overrides)
        try:
            app.dependency_overrides.clear()
            app.dependency_overrides[get_settings] = self._non_enforcing_memory_settings
            app.dependency_overrides[get_license_state_store] = lambda: calls.append("store") or object()
            app.dependency_overrides[get_license_rate_limiter] = lambda: calls.append("limiter") or object()

            with TestClient(app, base_url="https://noesisfood.app") as client:
                response = client.post(
                    "/license/session",
                    json={"integrity_token": "placeholder", "challenge_token": "placeholder"},
                )

            self.assertEqual(response.status_code, 503)
            self.assertCountEqual(calls, ["store", "limiter"])
        finally:
            app.dependency_overrides.clear()
            app.dependency_overrides.update(original_overrides)


class LicenseIngressDiagnosticTests(unittest.TestCase):
    @staticmethod
    def _render_log_call(call):
        template, *args = call.args
        return template % tuple(args) if args else template

    def _run_diagnostic(
        self,
        messages,
        *,
        headers=None,
        method="POST",
        path="/license/session",
        body_limit=16384,
        downstream=None,
        scope_state=None,
    ):
        import app.main as main

        original_messages = list(messages)
        forwarded = []
        receive_index = 0

        async def receive():
            nonlocal receive_index
            message = original_messages[receive_index]
            receive_index += 1
            return message

        async def default_downstream(_scope, wrapped_receive, _send):
            while True:
                message = await wrapped_receive()
                forwarded.append(message)
                if message.get("type") != "http.request" or not message.get("more_body", False):
                    return

        async def send(_message):
            return None

        scope = {
            "type": "http",
            "method": method,
            "path": path,
            "headers": headers or [],
            "client": ("203.0.113.10", 4321),
        }
        if scope_state is not None:
            scope["state"] = scope_state
        settings = replace(get_settings(), license_session_body_max_bytes=body_limit)
        with patch.object(main, "get_settings", return_value=settings), patch.object(main.logger, "warning") as warning:
            asyncio.run(main.LicenseIngressDiagnostics(downstream or default_downstream)(scope, receive, send))
        logs = [self._render_log_call(call) for call in warning.call_args_list]
        return scope, forwarded, original_messages, logs

    def _ingress_fields(self, logs):
        records = [message for message in logs if message.startswith("event=license_session_ingress_v2 ")]
        self.assertEqual(len(records), 1)
        return dict(part.split("=", 1) for part in records[0].split())

    def test_valid_object_reports_structure_keys_and_exact_hash(self):
        body = b'{"integrity_token":"token-placeholder","challenge_token":"challenge-placeholder"}'
        _scope, forwarded, messages, logs = self._run_diagnostic(
            [{"type": "http.request", "body": body, "more_body": False}],
            headers=[(b"content-type", b"application/json"), (b"content-length", str(len(body)).encode())],
        )
        fields = self._ingress_fields(logs)
        UUID(fields["request_id"])
        self.assertEqual(fields["raw_body_bytes"], str(len(body)))
        self.assertEqual(fields["body_empty"], "false")
        self.assertEqual(fields["first_non_whitespace_category"], "object_open")
        self.assertEqual(fields["last_non_whitespace_category"], "object_close")
        self.assertEqual(fields["utf8_decode"], "success")
        self.assertEqual(fields["json_parse"], "success")
        self.assertEqual(fields["json_top_level_type"], "object")
        self.assertEqual(fields["has_integrity_token_key"], "true")
        self.assertEqual(fields["has_challenge_token_key"], "true")
        self.assertEqual(fields["raw_body_sha256"], hashlib.sha256(body).hexdigest())
        self.assertIs(forwarded[0], messages[0])

    def test_double_encoded_object_reports_json_string_without_key_flags(self):
        inner = json.dumps(
            {"integrity_token": "token-placeholder", "challenge_token": "challenge-placeholder"},
            separators=(",", ":"),
        )
        body = json.dumps(inner, separators=(",", ":")).encode()
        _scope, _forwarded, _messages, logs = self._run_diagnostic(
            [{"type": "http.request", "body": body, "more_body": False}]
        )
        fields = self._ingress_fields(logs)
        self.assertEqual(fields["first_non_whitespace_category"], "quote")
        self.assertEqual(fields["last_non_whitespace_category"], "quote")
        self.assertEqual(fields["json_parse"], "success")
        self.assertEqual(fields["json_top_level_type"], "string")
        self.assertEqual(fields["has_integrity_token_key"], "not_applicable")
        self.assertEqual(fields["has_challenge_token_key"], "not_applicable")
        self.assertEqual(fields["content_type"], "absent")
        self.assertEqual(fields["content_encoding"], "absent")
        self.assertEqual(fields["content_length"], "absent")

    def test_truncated_empty_invalid_utf8_backtick_and_backslash_prefixes(self):
        cases = [
            ("truncated", b'{"integrity_token":"placeholder"', "object_open", "quote", "success", "failure"),
            ("empty", b"", "none", "none", "success", "failure"),
            ("invalid_utf8", b"\xff\xfe", "non_ascii", "non_ascii", "failure", "not_attempted"),
            ("backtick", b'`{"integrity_token":"placeholder"}', "backtick", "object_close", "success", "failure"),
            ("backslash_n", b'\\n{"integrity_token":"placeholder"}', "backslash", "object_close", "success", "failure"),
        ]
        for name, body, first, last, utf8_state, json_state in cases:
            with self.subTest(name=name):
                _scope, _forwarded, _messages, logs = self._run_diagnostic(
                    [{"type": "http.request", "body": body, "more_body": False}]
                )
                fields = self._ingress_fields(logs)
                self.assertEqual(fields["raw_body_bytes"], str(len(body)))
                self.assertEqual(fields["body_empty"], "true" if not body else "false")
                self.assertEqual(fields["first_non_whitespace_category"], first)
                self.assertEqual(fields["last_non_whitespace_category"], last)
                self.assertEqual(fields["utf8_decode"], utf8_state)
                self.assertEqual(fields["json_parse"], json_state)
                self.assertEqual(fields["json_top_level_type"], "not_available")
                self.assertEqual(fields["raw_body_sha256"], hashlib.sha256(body).hexdigest())

    def test_headers_are_allowlisted_normalized_bounded_and_classified(self):
        body = b"{}"
        headers = [
            (b"content-type", b" Application/JSON ; charset=UTF-8 "),
            (b"content-encoding", b" GZIP "),
            (b"content-length", b"2"),
        ]
        _scope, _forwarded, _messages, logs = self._run_diagnostic(
            [{"type": "http.request", "body": body, "more_body": False}], headers=headers
        )
        fields = self._ingress_fields(logs)
        self.assertEqual(fields["content_type"], "application/json")
        self.assertEqual(fields["content_encoding"], "gzip")
        self.assertEqual(fields["content_length"], "integer")

        long_header = b"private-marker-" + (b"x" * 200)
        _scope, _forwarded, _messages, logs = self._run_diagnostic(
            [{"type": "http.request", "body": body, "more_body": False}],
            headers=[
                (b"content-type", long_header),
                (b"content-encoding", long_header),
                (b"content-length", b"invalid"),
            ],
        )
        fields = self._ingress_fields(logs)
        self.assertEqual(fields["content_type"], "other")
        self.assertEqual(fields["content_encoding"], "other")
        self.assertEqual(fields["content_length"], "invalid")
        self.assertNotIn("private-marker", logs[0])

    def test_split_messages_are_forwarded_unchanged_counted_hashed_and_logged_once(self):
        body_parts = [b'{"integrity_token":"token-', b'placeholder","challenge_token":"challenge-placeholder"}']
        messages = [
            {"type": "http.request", "body": body_parts[0], "more_body": True},
            {"type": "http.request", "body": body_parts[1], "more_body": False},
        ]
        body = b"".join(body_parts)
        _scope, forwarded, originals, logs = self._run_diagnostic(messages)
        fields = self._ingress_fields(logs)
        self.assertEqual(len([message for message in logs if message.startswith("event=license_session_ingress_v2 ")]), 1)
        self.assertEqual(fields["raw_body_bytes"], str(len(body)))
        self.assertEqual(fields["raw_body_sha256"], hashlib.sha256(body).hexdigest())
        self.assertEqual(forwarded, originals)
        for forwarded_message, original_message in zip(forwarded, originals):
            self.assertIs(forwarded_message, original_message)

    def test_body_over_buffer_limit_is_not_decoded_or_parsed(self):
        body = b'{"integrity_token":"token-placeholder","challenge_token":"challenge-placeholder"}'
        _scope, forwarded, messages, logs = self._run_diagnostic(
            [{"type": "http.request", "body": body, "more_body": False}], body_limit=8
        )
        fields = self._ingress_fields(logs)
        self.assertEqual(fields["raw_body_bytes"], str(len(body)))
        self.assertEqual(fields["raw_body_sha256"], hashlib.sha256(body).hexdigest())
        self.assertEqual(fields["utf8_decode"], "not_attempted")
        self.assertEqual(fields["json_parse"], "not_attempted")
        self.assertEqual(fields["json_top_level_type"], "not_available")
        self.assertEqual(fields["has_integrity_token_key"], "not_applicable")
        self.assertEqual(fields["has_challenge_token_key"], "not_applicable")
        self.assertIs(forwarded[0], messages[0])

    def test_log_excludes_body_values_and_unapproved_request_metadata(self):
        body = (
            b'{"integrity_token":"integrity-value-never-log",'
            b'"challenge_token":"challenge-value-never-log","private_marker":"body-text-never-log"}'
        )
        headers = [
            (b"content-type", b"application/json"),
            (b"cookie", b"cookie-value-never-log"),
            (b"authorization", b"authorization-value-never-log"),
            (b"user-agent", b"user-agent-value-never-log"),
            (b"x-request-id", b"inbound-request-id-never-trust"),
        ]
        _scope, _forwarded, _messages, logs = self._run_diagnostic(
            [{"type": "http.request", "body": body, "more_body": False}], headers=headers
        )
        record = next(message for message in logs if message.startswith("event=license_session_ingress_v2 "))
        UUID(dict(part.split("=", 1) for part in record.split())["request_id"])
        for forbidden in (
            "integrity-value-never-log",
            "challenge-value-never-log",
            "body-text-never-log",
            "private_marker",
            "cookie-value-never-log",
            "authorization-value-never-log",
            "user-agent-value-never-log",
            "inbound-request-id-never-trust",
            "203.0.113.10",
        ):
            for message in logs:
                self.assertNotIn(forbidden, message)

    def test_validation_record_uses_same_server_request_id(self):
        import app.main as main

        async def downstream(scope, wrapped_receive, _send):
            await wrapped_receive()
            request = Request(scope, wrapped_receive)
            await main.validation_exception_handler(request, RequestValidationError([]))

        _scope, _forwarded, _messages, logs = self._run_diagnostic(
            [{"type": "http.request", "body": b"{}", "more_body": False}], downstream=downstream
        )
        ingress_fields = self._ingress_fields(logs)
        validation_records = [message for message in logs if message.startswith("event=license_validation_error_v1 ")]
        self.assertEqual(len(validation_records), 1)
        validation_fields = dict(part.split("=", 1) for part in validation_records[0].split())
        self.assertEqual(validation_fields["request_id"], ingress_fields["request_id"])

    def test_non_matching_method_and_path_emit_no_v2_record(self):
        for method, path in (("GET", "/license/session"), ("POST", "/license/status")):
            with self.subTest(method=method, path=path):
                _scope, _forwarded, _messages, logs = self._run_diagnostic(
                    [{"type": "http.request", "body": b"{}", "more_body": False}],
                    method=method,
                    path=path,
                )
                self.assertFalse(any(message.startswith("event=license_session_ingress_v2 ") for message in logs))

    def test_logger_failure_returns_original_message_and_preserves_downstream_response(self):
        import app.main as main

        message = {"type": "http.request", "body": b"{}", "more_body": False}
        received = []
        sent = []

        async def receive():
            return message

        async def downstream(_scope, wrapped_receive, send):
            received.append(await wrapped_receive())
            await send({"type": "http.response.start", "status": 204, "headers": []})
            await send({"type": "http.response.body", "body": b""})

        async def send(response_message):
            sent.append(response_message)

        scope = {"type": "http", "method": "POST", "path": "/license/session", "headers": []}
        settings = replace(get_settings(), license_session_body_max_bytes=16384)
        with (
            patch.object(main, "get_settings", return_value=settings),
            patch.object(main.logger, "warning", side_effect=RuntimeError),
        ):
            asyncio.run(main.LicenseIngressDiagnostics(downstream)(scope, receive, send))

        self.assertEqual(len(received), 1)
        self.assertIs(received[0], message)
        self.assertEqual(sent[0]["status"], 204)
        self.assertEqual(sent[-1]["body"], b"")

    def test_structural_helper_failure_returns_original_message(self):
        import app.main as main

        message = {"type": "http.request", "body": b"{}", "more_body": False}
        with patch.object(main, "_body_edge_category", side_effect=RuntimeError):
            _scope, forwarded, originals, logs = self._run_diagnostic([message])
        self.assertEqual(len(forwarded), 1)
        self.assertIs(forwarded[0], originals[0])
        self.assertFalse(any(record.startswith("event=license_session_ingress_v2 ") for record in logs))

    def test_uuid_and_scope_state_failures_bypass_diagnostics(self):
        import app.main as main

        message = {"type": "http.request", "body": b"{}", "more_body": False}
        validation_statuses = []

        async def validation_downstream(scope, wrapped_receive, _send):
            await wrapped_receive()
            response = await main.validation_exception_handler(
                Request(scope, wrapped_receive),
                RequestValidationError([]),
            )
            validation_statuses.append(response.status_code)

        with patch.object(main.uuid, "uuid4", side_effect=RuntimeError):
            _scope, forwarded, originals, logs = self._run_diagnostic(
                [message],
                downstream=validation_downstream,
            )
        self.assertEqual(validation_statuses, [400])
        validation_record = next(record for record in logs if record.startswith("event=license_validation_error_v1 "))
        self.assertIn("request_id=unavailable", validation_record)

        class RejectingState(dict):
            def __setitem__(self, _key, _value):
                raise RuntimeError

        _scope, forwarded, originals, logs = self._run_diagnostic(
            [message],
            scope_state=RejectingState(),
        )
        self.assertIs(forwarded[0], originals[0])
        self.assertFalse(any(record.startswith("event=license_session_ingress_v2 ") for record in logs))

    def test_diagnostic_failure_disables_retries_and_forwards_later_chunks(self):
        import app.main as main

        class FailingHash:
            def __init__(self):
                self.update_calls = 0

            def update(self, _body):
                self.update_calls += 1
                raise RuntimeError

            def hexdigest(self):
                raise AssertionError("disabled diagnostics must not emit")

        messages = [
            {"type": "http.request", "body": b"{", "more_body": True},
            {"type": "http.request", "body": b"}", "more_body": False},
        ]
        failing_hash = FailingHash()
        with patch.object(main.hashlib, "sha256", return_value=failing_hash):
            _scope, forwarded, originals, logs = self._run_diagnostic(messages)
        self.assertEqual(failing_hash.update_calls, 1)
        self.assertEqual(forwarded, originals)
        for forwarded_message, original_message in zip(forwarded, originals):
            self.assertIs(forwarded_message, original_message)
        self.assertFalse(any(record.startswith("event=license_session_ingress_v2 ") for record in logs))

    def test_original_receive_exception_is_not_swallowed(self):
        import app.main as main

        class ReceiveFailure(Exception):
            pass

        async def receive():
            raise ReceiveFailure

        async def downstream(_scope, wrapped_receive, _send):
            await wrapped_receive()

        async def send(_message):
            return None

        scope = {"type": "http", "method": "POST", "path": "/license/session", "headers": []}
        settings = replace(get_settings(), license_session_body_max_bytes=16384)
        with patch.object(main, "get_settings", return_value=settings):
            with self.assertRaises(ReceiveFailure):
                asyncio.run(main.LicenseIngressDiagnostics(downstream)(scope, receive, send))

    def test_downstream_application_exception_is_not_swallowed(self):
        import app.main as main

        class DownstreamFailure(Exception):
            pass

        async def receive():
            return {"type": "http.request", "body": b"", "more_body": False}

        async def downstream(_scope, _wrapped_receive, _send):
            raise DownstreamFailure

        async def send(_message):
            return None

        scope = {"type": "http", "method": "POST", "path": "/license/session", "headers": []}
        settings = replace(get_settings(), license_session_body_max_bytes=16384)
        with patch.object(main, "get_settings", return_value=settings):
            with self.assertRaises(DownstreamFailure):
                asyncio.run(main.LicenseIngressDiagnostics(downstream)(scope, receive, send))

    def test_validation_diagnostic_failures_keep_bounded_response(self):
        import app.main as main

        async def receive():
            return {"type": "http.request", "body": b"", "more_body": False}

        request = Request(
            {
                "type": "http",
                "method": "POST",
                "scheme": "https",
                "path": "/license/session",
                "raw_path": b"/license/session",
                "query_string": b"",
                "headers": [],
                "server": ("test", 443),
                "root_path": "",
            },
            receive,
        )
        for failure_target in ("request_id", "logger"):
            with self.subTest(failure_target=failure_target):
                request_id_patch = patch.object(main, "_license_request_id", side_effect=RuntimeError)
                logger_patch = patch.object(main.logger, "warning", side_effect=RuntimeError)
                active_patch = request_id_patch if failure_target == "request_id" else logger_patch
                with active_patch:
                    response = asyncio.run(main.validation_exception_handler(request, RequestValidationError([])))
                self.assertEqual(response.status_code, 400)


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
        self.assertEqual(len(captured.records), 2)
        self.assertEqual(captured.records[0].getMessage(), "license_session_bad_request reason=request_validation_error")
        self.assertEqual(
            captured.records[1].getMessage(),
            "event=license_validation_error_v1 request_id=unavailable type=other location=other",
        )

    def test_ingress_diagnostics_forwards_split_messages_unchanged(self):
        from app.main import LicenseIngressDiagnostics

        messages = [
            {"type": "http.request", "body": b'{"integrity_token":"', "more_body": True},
            {"type": "http.request", "body": b"x", "more_body": False},
        ]
        received = []

        async def receive():
            return messages[len(received)]

        async def downstream(scope, wrapped_receive, send):
            received.append(await wrapped_receive())
            received.append(await wrapped_receive())

        async def send(_message):
            return None

        scope = {
            "type": "http",
            "method": "POST",
            "path": "/license/session",
            "headers": [(b"content-type", b"application/json"), (b"content-length", b"21")],
        }
        with self.assertLogs("noesisfood.license", level="WARNING") as captured:
            asyncio.run(LicenseIngressDiagnostics(downstream)(scope, receive, send))

        self.assertEqual(received, messages)
        self.assertEqual(len(captured.records), 1)
        message = captured.records[0].getMessage()
        fields = dict(part.split("=", 1) for part in message.split())
        UUID(fields["request_id"])
        body = b"".join(item["body"] for item in messages)
        self.assertEqual(fields["event"], "license_session_ingress_v2")
        self.assertEqual(fields["raw_body_bytes"], str(len(body)))
        self.assertEqual(fields["first_non_whitespace_category"], "object_open")
        self.assertEqual(fields["last_non_whitespace_category"], "letter")
        self.assertEqual(fields["utf8_decode"], "success")
        self.assertEqual(fields["json_parse"], "failure")
        self.assertEqual(fields["raw_body_sha256"], hashlib.sha256(body).hexdigest())
        self.assertEqual(fields["content_type"], "application/json")
        self.assertEqual(fields["content_encoding"], "absent")
        self.assertEqual(fields["content_length"], "integer")
        self.assertNotIn('"integrity_token"', message)
        self.assertNotIn('"x"', message)

    def test_runtime_route_shape_marker_is_bounded(self):
        import app.main as main

        with self.assertLogs("noesisfood.license", level="WARNING") as captured:
            asyncio.run(main.log_license_runtime_shape())
        self.assertEqual(len(captured.records), 1)
        message = captured.records[0].getMessage()
        self.assertIn("license_runtime_shape_v1", message)
        self.assertIn("body_field_present=no", message)
        self.assertIn("body_parameter_count=zero", message)
        self.assertIn("dependency_body_parameter_count=zero", message)

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
                conflict_records = [record for record in captured.records if record.getMessage().startswith("license_session_conflict ")]
                self.assertEqual(len(conflict_records), 1)
                self.assertEqual(conflict_records[0].getMessage(), f"license_session_conflict reason={reason}")
                self.assertNotIn("integrity_token", conflict_records[0].getMessage())
                self.assertNotIn("challenge_token", conflict_records[0].getMessage())
                self.assertNotIn(challenge["challenge_token"], conflict_records[0].getMessage())

    def test_malformed_and_oversized_requests_do_not_call_google(self):
        with patch.dict(os.environ, _env(LICENSE_SESSION_BODY_MAX_BYTES="32"), clear=False):
            client = self._client()
            with self.assertLogs("noesisfood.license", level="WARNING") as captured:
                malformed = client.post("/license/session", content=b"{bad")
            self.assertEqual(malformed.status_code, 400)
            self.assertEqual(malformed.json(), {"detail": "Malformed license request"})
            self.assertEqual(len(captured.records), 2)
            ingress_fields = dict(part.split("=", 1) for part in captured.records[0].getMessage().split())
            UUID(ingress_fields["request_id"])
            self.assertEqual(ingress_fields["event"], "license_session_ingress_v2")
            self.assertEqual(ingress_fields["raw_body_bytes"], "4")
            self.assertEqual(ingress_fields["first_non_whitespace_category"], "object_open")
            self.assertEqual(ingress_fields["last_non_whitespace_category"], "letter")
            self.assertEqual(ingress_fields["utf8_decode"], "success")
            self.assertEqual(ingress_fields["json_parse"], "failure")
            self.assertEqual(ingress_fields["raw_body_sha256"], hashlib.sha256(b"{bad").hexdigest())
            self.assertEqual(ingress_fields["content_type"], "absent")
            self.assertEqual(ingress_fields["content_encoding"], "absent")
            self.assertEqual(ingress_fields["content_length"], "integer")
            self.assertEqual(captured.records[1].getMessage(), "license_session_bad_request reason=malformed_json_body")
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
            integrity_records = [record for record in captured_integrity.records if record.getMessage().startswith("license_session_bad_request ")]
            self.assertEqual(len(integrity_records), 1)
            self.assertEqual(integrity_records[0].getMessage(), "license_session_bad_request reason=missing_integrity_token")

            challenge = self._challenge(client)
            with self.assertLogs("noesisfood.license", level="WARNING") as captured_challenge:
                missing_challenge = client.post(
                    "/license/session",
                    json={"integrity_token": "ok"},
                )
            self.assertEqual(missing_challenge.status_code, 400)
            self.assertEqual(missing_challenge.json(), {"detail": "Malformed license request"})
            challenge_records = [record for record in captured_challenge.records if record.getMessage().startswith("license_session_bad_request ")]
            self.assertEqual(len(challenge_records), 1)
            self.assertEqual(challenge_records[0].getMessage(), "license_session_bad_request reason=missing_challenge_token")

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
