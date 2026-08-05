import http.client
import json
import os
import socket
import threading
import time
import unittest

import uvicorn

from app.api.routes.license import get_integrity_verifier
from app.license_rate_limit import MemoryLicenseRateLimiter, get_license_rate_limiter
from app.license_state import MemoryLicenseStateStore, get_license_state_store


class _Verifier:
    def __init__(self):
        self.request_hash = ""

    async def decode(self, _token):
        return {
            "requestDetails": {
                "requestPackageName": "com.noesisfood.app",
                "requestHash": self.request_hash,
                "timestampMillis": int(time.time() * 1000),
            },
            "appIntegrity": {
                "packageName": "com.noesisfood.app",
                "versionCode": 9,
                "appRecognitionVerdict": "PLAY_RECOGNIZED",
            },
            "accountDetails": {"appLicensingVerdict": "LICENSED"},
        }


class LicenseIngressUvicornTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ.update(
            {
                "APP_ACCESS_LOCKDOWN_ENABLED": "false",
                "PLAY_INTEGRITY_ENFORCEMENT_ENABLED": "true",
                "PLAY_INTEGRITY_PACKAGE_NAME": "com.noesisfood.app",
                "PLAY_INTEGRITY_MIN_VERSION_CODE": "9",
                "LICENSE_SESSION_SECRET": "test-license-secret-with-enough-entropy-0123456789abcdef",
                "LICENSE_STATE_BACKEND": "memory",
                "LICENSE_CHALLENGE_RATE_LIMIT_PER_MINUTE": "100",
                "LICENSE_SESSION_RATE_LIMIT_PER_MINUTE": "100",
                "LICENSE_SESSION_BODY_MAX_BYTES": "16384",
                "PLAY_INTEGRITY_MAX_CONCURRENT_DECODES": "5",
                "INVALID_TOKEN_DENY_TTL_SECONDS": "60",
            }
        )
        from app.main import app

        cls.app = app
        cls.verifier = _Verifier()
        cls.app.dependency_overrides[get_integrity_verifier] = lambda: cls.verifier
        cls.app.dependency_overrides[get_license_state_store] = lambda: cls.store
        cls.app.dependency_overrides[get_license_rate_limiter] = lambda: cls.limiter
        cls.port = cls._free_port()
        cls.server = uvicorn.Server(
            uvicorn.Config(cls.app, host="127.0.0.1", port=cls.port, log_level="critical", access_log=False)
        )
        cls.thread = threading.Thread(target=cls.server.run, daemon=True)
        cls.thread.start()
        for _ in range(100):
            try:
                with socket.create_connection(("127.0.0.1", cls.port), timeout=0.1):
                    return
            except OSError:
                time.sleep(0.05)
        raise RuntimeError("uvicorn did not start")

    @classmethod
    def tearDownClass(cls):
        cls.server.should_exit = True
        cls.thread.join(timeout=5)
        cls.app.dependency_overrides.clear()

    def setUp(self):
        type(self).store = MemoryLicenseStateStore()
        type(self).limiter = MemoryLicenseRateLimiter()

    @staticmethod
    def _free_port():
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            return sock.getsockname()[1]

    def _challenge(self):
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=3)
        connection.request("GET", "/license/challenge")
        response = connection.getresponse()
        payload = json.loads(response.read())
        connection.close()
        self.assertEqual(response.status, 200)
        self.verifier.request_hash = payload["request_hash"]
        return payload["challenge_token"]

    def _post(self, body, framing):
        sock = socket.create_connection(("127.0.0.1", self.port), timeout=3)
        if framing == "chunked":
            request = (
                b"POST /license/session HTTP/1.1\r\nHost: 127.0.0.1\r\n"
                b"Content-Type: application/json\r\nTransfer-Encoding: chunked\r\n"
                b"Connection: close\r\n\r\n"
                + (f"{len(body):x}\r\n".encode() + body + b"\r\n0\r\n\r\n")
            )
            sock.sendall(request)
        elif framing == "split":
            header = (
                f"POST /license/session HTTP/1.1\r\nHost: 127.0.0.1\r\n"
                f"Content-Type: application/json\r\nContent-Length: {len(body)}\r\n"
                "Connection: close\r\n\r\n"
            ).encode()
            midpoint = max(1, len(body) // 2)
            sock.sendall(header + body[:midpoint])
            time.sleep(0.01)
            sock.sendall(body[midpoint:])
        else:
            sock.sendall(
                (
                    f"POST /license/session HTTP/1.1\r\nHost: 127.0.0.1\r\n"
                    f"Content-Type: application/json\r\nContent-Length: {len(body)}\r\n"
                    "Connection: close\r\n\r\n"
                ).encode()
                + body
            )
        header = b""
        while b"\r\n\r\n" not in header:
            chunk = sock.recv(1)
            if not chunk:
                break
            header += chunk
        sock.close()
        return int(header.split(b" ", 2)[1])

    def test_real_uvicorn_framing_preserves_production_json(self):
        for framing in ("content_length", "split", "chunked"):
            with self.subTest(framing=framing):
                challenge = self._challenge()
                body = json.dumps(
                    {"integrity_token": "x" * 128, "challenge_token": challenge},
                    separators=(",", ":"),
                ).encode()
                self.assertEqual(self._post(body, framing), 200)

    def test_real_uvicorn_malformed_json_keeps_generic_response(self):
        self.assertEqual(self._post(b"{bad", "content_length"), 400)


if __name__ == "__main__":
    unittest.main()
