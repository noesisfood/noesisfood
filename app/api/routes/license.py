import json

from fastapi import APIRouter, Depends, HTTPException, Request, Response

from app.config import Settings, get_settings
from app.license_rate_limit import (
    LicenseRateLimiter,
    RateLimitExceeded,
    client_identifier,
    get_license_rate_limiter,
    token_digest,
)
from app.license_state import (
    ChallengeConsumed,
    ChallengeRecord,
    ChallengeUnavailable,
    LicenseStateStore,
    get_license_state_store,
)
from app.licensing import (
    COOKIE_NAME,
    IntegrityVerifier,
    LicenseError,
    VerifierUnavailable,
    create_challenge,
    create_session_cookie,
    get_integrity_verifier,
    verify_challenge,
    verify_decoded_integrity,
    verify_session_cookie,
)


router = APIRouter(prefix="/license", tags=["license"])


@router.get("/challenge")
async def challenge(
    request: Request,
    response: Response,
    settings: Settings = Depends(get_settings),
    store: LicenseStateStore = Depends(get_license_state_store),
    limiter: LicenseRateLimiter = Depends(get_license_rate_limiter),
):
    try:
        response.headers["Cache-Control"] = "no-store"
        client_id = client_identifier(request, settings)
        await limiter.check_client_rate(
            "challenge",
            client_id,
            settings.license_challenge_rate_limit_per_minute,
        )
        item = create_challenge(settings=settings)
        await store.store_challenge(
            ChallengeRecord(
                challenge_id=item.challenge_id,
                request_hash=item.request_hash,
                issued_at=item.issued_at,
                expires_at=item.expires_at,
                max_attempts=settings.license_challenge_max_attempts,
            )
        )
    except RateLimitExceeded:
        raise HTTPException(status_code=429, detail="Too many license challenges")
    except VerifierUnavailable:
        raise HTTPException(status_code=503, detail="Licensing is not configured")
    except Exception:
        raise HTTPException(status_code=503, detail="License state is unavailable")
    return {
        "challenge_id": item.challenge_id,
        "issued_at": item.issued_at,
        "expires_at": item.expires_at,
        "request_hash": item.request_hash,
        "challenge_token": item.challenge_token,
    }


@router.post("/session")
async def session(
    request: Request,
    response: Response,
    settings: Settings = Depends(get_settings),
    verifier: IntegrityVerifier = Depends(get_integrity_verifier),
    store: LicenseStateStore = Depends(get_license_state_store),
    limiter: LicenseRateLimiter = Depends(get_license_rate_limiter),
):
    if not settings.play_integrity_enforcement_enabled:
        raise HTTPException(status_code=503, detail="Play Integrity enforcement is disabled")
    content_length = request.headers.get("content-length")
    if content_length:
        try:
            if int(content_length) > settings.license_session_body_max_bytes:
                raise HTTPException(status_code=413, detail="License request too large")
        except ValueError:
            raise HTTPException(status_code=400, detail="Malformed license request")
    try:
        response.headers["Cache-Control"] = "no-store"
        client_id = client_identifier(request, settings)
        await limiter.check_client_rate(
            "session",
            client_id,
            settings.license_session_rate_limit_per_minute,
        )
        body = await request.body()
        if len(body) > settings.license_session_body_max_bytes:
            raise HTTPException(status_code=413, detail="License request too large")
        try:
            raw = json.loads(body.decode("utf-8"))
        except Exception:
            raise HTTPException(status_code=400, detail="Malformed license request")
        integrity_token = str(raw.get("integrity_token") or "").strip()
        challenge_token = str(raw.get("challenge_token") or "").strip()
        if not integrity_token or not challenge_token:
            raise HTTPException(status_code=400, detail="Malformed license request")
        digest = token_digest(integrity_token)
        if await limiter.is_invalid_token_denied(digest):
            raise HTTPException(status_code=403, detail="License verification failed")
        challenge_payload = verify_challenge(challenge_token, settings=settings)
        record = await store.consume_challenge(str(challenge_payload.get("cid") or ""))
        if (
            record.request_hash != str(challenge_payload.get("rh") or "")
            or record.issued_at != int(challenge_payload.get("iat") or 0)
            or record.expires_at != int(challenge_payload.get("exp") or 0)
        ):
            raise LicenseError("stored challenge mismatch")
        slot = await limiter.acquire_decode_slot(settings.play_integrity_max_concurrent_decodes)
        try:
            decoded = await verifier.decode(integrity_token)
        finally:
            await limiter.release_decode_slot(slot)
        verify_decoded_integrity(decoded, str(challenge_payload.get("rh") or ""), settings=settings)
        cookie_value, expires_at = create_session_cookie(decoded, settings=settings)
    except HTTPException:
        raise
    except RateLimitExceeded:
        raise HTTPException(status_code=429, detail="Too many license requests")
    except (ChallengeConsumed, ChallengeUnavailable):
        raise HTTPException(status_code=409, detail="License challenge is no longer valid")
    except VerifierUnavailable:
        raise HTTPException(status_code=503, detail="Play Integrity verifier unavailable")
    except LicenseError:
        if "digest" in locals():
            try:
                await limiter.mark_invalid_token(digest, settings.invalid_token_deny_ttl_seconds)
            except Exception:
                pass
        raise HTTPException(status_code=403, detail="License verification failed")
    except Exception:
        raise HTTPException(status_code=503, detail="License state is unavailable")

    response.set_cookie(
        COOKIE_NAME,
        cookie_value,
        max_age=settings.license_session_ttl_seconds,
        expires=expires_at,
        secure=True,
        httponly=True,
        samesite="lax",
        path="/",
    )
    return {"ok": True, "expires_at": expires_at}


@router.get("/status")
async def status(
    request: Request,
    response: Response,
    settings: Settings = Depends(get_settings),
    limiter: LicenseRateLimiter = Depends(get_license_rate_limiter),
):
    response.headers["Cache-Control"] = "no-store"
    try:
        await limiter.check_client_rate("status", client_identifier(request, settings), 60)
    except RateLimitExceeded:
        raise HTTPException(status_code=429, detail="Too many license status requests")
    return {
        "app_access_lockdown_enabled": settings.app_access_lockdown_enabled,
        "play_integrity_enforcement_enabled": settings.play_integrity_enforcement_enabled,
        "package_name": settings.play_integrity_package_name,
        "min_version_code": settings.play_integrity_min_version_code,
    }


@router.post("/logout")
async def logout(
    request: Request,
    response: Response,
    settings: Settings = Depends(get_settings),
    store: LicenseStateStore = Depends(get_license_state_store),
    limiter: LicenseRateLimiter = Depends(get_license_rate_limiter),
):
    response.headers["Cache-Control"] = "no-store"
    try:
        await limiter.check_client_rate("logout", client_identifier(request, settings), 20)
    except RateLimitExceeded:
        raise HTTPException(status_code=429, detail="Too many license logout requests")
    cookie = request.cookies.get(COOKIE_NAME)
    if cookie:
        try:
            payload = verify_session_cookie(cookie, settings=settings)
            await store.revoke_session(str(payload.get("jti") or ""), int(payload.get("exp") or 0))
        except LicenseError:
            pass
    response.delete_cookie(COOKIE_NAME, path="/", secure=True, httponly=True, samesite="lax")
    return {"ok": True}
