import base64
import hashlib
import hmac
import json
import secrets
import time
from dataclasses import dataclass
from typing import Any, Protocol

import httpx
from fastapi import Cookie, Depends, HTTPException, Request

from app.config import Settings, get_settings


COOKIE_NAME = "nf_license_session"


class LicenseError(Exception):
    pass


class VerifierUnavailable(LicenseError):
    pass


class IntegrityVerifier(Protocol):
    async def decode(self, integrity_token: str) -> dict[str, Any]:
        ...


@dataclass(frozen=True)
class LicenseChallenge:
    challenge_id: str
    issued_at: int
    expires_at: int
    request_hash: str
    challenge_token: str


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64url_decode(data: str) -> bytes:
    padding = "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode((data + padding).encode("ascii"))


def _sign(payload: dict[str, Any], secret: str) -> str:
    if not secret:
        raise VerifierUnavailable("LICENSE_SESSION_SECRET is not configured")
    body = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    encoded = _b64url(body)
    signature = hmac.new(secret.encode("utf-8"), encoded.encode("ascii"), hashlib.sha256).digest()
    return f"{encoded}.{_b64url(signature)}"


def _unsign(token: str, secret: str) -> dict[str, Any]:
    if not secret:
        raise LicenseError("missing signing secret")
    try:
        encoded, provided = token.split(".", 1)
        expected = _b64url(hmac.new(secret.encode("utf-8"), encoded.encode("ascii"), hashlib.sha256).digest())
        if not hmac.compare_digest(expected, provided):
            raise LicenseError("bad signature")
        return json.loads(_b64url_decode(encoded).decode("utf-8"))
    except LicenseError:
        raise
    except Exception as exc:
        raise LicenseError("malformed signed token") from exc


def _request_hash(challenge_id: str, issued_at: int) -> str:
    digest = hashlib.sha256(f"{challenge_id}.{issued_at}".encode("ascii")).digest()
    return _b64url(digest)


def create_challenge(settings: Settings | None = None, now: int | None = None) -> LicenseChallenge:
    settings = settings or get_settings()
    current = int(now or time.time())
    challenge_id = _b64url(secrets.token_bytes(32))
    request_hash = _request_hash(challenge_id, current)
    payload = {
        "typ": "nf_license_challenge",
        "cid": challenge_id,
        "iat": current,
        "exp": current + settings.license_challenge_ttl_seconds,
        "rh": request_hash,
    }
    return LicenseChallenge(
        challenge_id=challenge_id,
        issued_at=payload["iat"],
        expires_at=payload["exp"],
        request_hash=request_hash,
        challenge_token=_sign(payload, settings.license_session_secret),
    )


def verify_challenge(challenge_token: str, settings: Settings | None = None, now: int | None = None) -> dict[str, Any]:
    settings = settings or get_settings()
    current = int(now or time.time())
    payload = _unsign(challenge_token, settings.license_session_secret)
    if payload.get("typ") != "nf_license_challenge":
        raise LicenseError("wrong challenge token type")
    if int(payload.get("exp") or 0) < current:
        raise LicenseError("expired challenge")
    expected_hash = _request_hash(str(payload.get("cid") or ""), int(payload.get("iat") or 0))
    if not hmac.compare_digest(expected_hash, str(payload.get("rh") or "")):
        raise LicenseError("challenge hash mismatch")
    return payload


def create_session_cookie(decoded: dict[str, Any], settings: Settings | None = None, now: int | None = None) -> tuple[str, int]:
    settings = settings or get_settings()
    current = int(now or time.time())
    app_integrity = decoded.get("appIntegrity") if isinstance(decoded.get("appIntegrity"), dict) else {}
    payload = {
        "typ": "nf_license_session",
        "jti": _b64url(secrets.token_bytes(16)),
        "iat": current,
        "exp": current + settings.license_session_ttl_seconds,
        "pkg": settings.play_integrity_package_name,
        "vc": int(app_integrity.get("versionCode") or 0),
    }
    return _sign(payload, settings.license_session_secret), payload["exp"]


def verify_session_cookie(cookie_value: str | None, settings: Settings | None = None, now: int | None = None) -> dict[str, Any]:
    if not cookie_value:
        raise LicenseError("missing session cookie")
    settings = settings or get_settings()
    current = int(now or time.time())
    payload = _unsign(cookie_value, settings.license_session_secret)
    if payload.get("typ") != "nf_license_session":
        raise LicenseError("wrong session token type")
    if int(payload.get("exp") or 0) < current:
        raise LicenseError("expired session")
    if payload.get("pkg") != settings.play_integrity_package_name:
        raise LicenseError("wrong package session")
    if int(payload.get("vc") or 0) < settings.play_integrity_min_version_code:
        raise LicenseError("old app session")
    return payload


def verify_decoded_integrity(
    decoded: dict[str, Any],
    expected_request_hash: str,
    settings: Settings | None = None,
    now_ms: int | None = None,
) -> None:
    settings = settings or get_settings()
    current_ms = int(now_ms if now_ms is not None else time.time() * 1000)
    request_details = decoded.get("requestDetails") if isinstance(decoded.get("requestDetails"), dict) else {}
    app_integrity = decoded.get("appIntegrity") if isinstance(decoded.get("appIntegrity"), dict) else {}
    account_details = decoded.get("accountDetails") if isinstance(decoded.get("accountDetails"), dict) else {}

    if request_details.get("requestPackageName") != settings.play_integrity_package_name:
        raise LicenseError("wrong request package")
    if not hmac.compare_digest(str(request_details.get("requestHash") or ""), expected_request_hash):
        raise LicenseError("wrong request hash")
    timestamp_ms = int(request_details.get("timestampMillis") or 0)
    if timestamp_ms <= 0 or abs(current_ms - timestamp_ms) > settings.play_integrity_token_max_age_seconds * 1000:
        raise LicenseError("stale integrity token")
    if app_integrity.get("packageName") != settings.play_integrity_package_name:
        raise LicenseError("wrong app package")
    if int(app_integrity.get("versionCode") or 0) < settings.play_integrity_min_version_code:
        raise LicenseError("app version too old")
    if app_integrity.get("appRecognitionVerdict") != "PLAY_RECOGNIZED":
        raise LicenseError("app not play recognized")
    if account_details.get("appLicensingVerdict") != "LICENSED":
        raise LicenseError("app not licensed")


class GooglePlayIntegrityVerifier:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()

    async def decode(self, integrity_token: str) -> dict[str, Any]:
        access_token = self._access_token()
        url = (
            "https://playintegrity.googleapis.com/v1/"
            f"{self.settings.play_integrity_package_name}:decodeIntegrityToken"
        )
        timeout = httpx.Timeout(connect=3.0, read=7.0, write=3.0, pool=2.0)
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.post(
                url,
                headers={"Authorization": f"Bearer {access_token}"},
                json={"integrity_token": integrity_token},
            )
        if response.status_code >= 400:
            raise VerifierUnavailable("Google Play Integrity decode failed")
        data = response.json()
        decoded = data.get("tokenPayloadExternal")
        if not isinstance(decoded, dict):
            raise LicenseError("malformed decode response")
        return decoded

    def _access_token(self) -> str:
        try:
            import google.auth
            from google.auth.transport.requests import Request as GoogleAuthRequest
        except Exception as exc:
            raise VerifierUnavailable("google-auth is not installed") from exc
        credentials, _project = google.auth.default(scopes=["https://www.googleapis.com/auth/playintegrity"])
        credentials.refresh(GoogleAuthRequest())
        return credentials.token


def get_integrity_verifier() -> IntegrityVerifier:
    return GooglePlayIntegrityVerifier()


async def require_licensed_session(
    request: Request,
    nf_license_session: str | None = Cookie(default=None, alias=COOKIE_NAME),
    settings: Settings = Depends(get_settings),
) -> None:
    if not settings.app_access_lockdown_enabled:
        return
    try:
        payload = verify_session_cookie(nf_license_session, settings=settings)
        from app.license_state import get_license_state_store

        store = getattr(request.app.state, "license_state_store_override", None) or get_license_state_store(settings)
        if await store.is_session_revoked(str(payload.get("jti") or "")):
            raise LicenseError("revoked session")
    except LicenseError:
        raise HTTPException(status_code=401, detail="Licensed Android session required")


def request_has_licensed_session(request: Request, settings: Settings | None = None) -> bool:
    settings = settings or get_settings()
    try:
        verify_session_cookie(request.cookies.get(COOKIE_NAME), settings=settings)
        return True
    except LicenseError:
        return False


async def request_has_active_licensed_session(request: Request, settings: Settings | None = None) -> bool:
    settings = settings or get_settings()
    try:
        payload = verify_session_cookie(request.cookies.get(COOKIE_NAME), settings=settings)
        from app.license_state import get_license_state_store

        store = getattr(request.app.state, "license_state_store_override", None) or get_license_state_store(settings)
        return not await store.is_session_revoked(str(payload.get("jti") or ""))
    except LicenseError:
        return False
