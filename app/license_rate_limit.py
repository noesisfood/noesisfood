import asyncio
import hashlib
import time
from dataclasses import dataclass
from typing import Annotated, Protocol

from fastapi import Depends, Request

from app.config import Settings, get_settings
from app.licensing import VerifierUnavailable


class RateLimitExceeded(Exception):
    pass


@dataclass(frozen=True)
class DecodeSlot:
    key: str
    acquired: bool


class LicenseRateLimiter(Protocol):
    async def check_client_rate(self, bucket: str, client_id: str, limit: int, window_seconds: int = 60) -> None:
        ...

    async def is_invalid_token_denied(self, token_digest: str) -> bool:
        ...

    async def mark_invalid_token(self, token_digest: str, ttl_seconds: int) -> None:
        ...

    async def acquire_decode_slot(self, limit: int) -> DecodeSlot:
        ...

    async def release_decode_slot(self, slot: DecodeSlot) -> None:
        ...


def token_digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def client_identifier(request: Request, settings: Settings | None = None) -> str:
    settings = settings or get_settings()
    host = request.client.host if request.client else "unknown"
    if settings.trusted_proxy_hops > 0:
        forwarded = request.headers.get("x-forwarded-for", "")
        parts = [part.strip() for part in forwarded.split(",") if part.strip()]
        if parts:
            host = parts[-settings.trusted_proxy_hops]
    ua = request.headers.get("user-agent", "")
    return hashlib.sha256(f"{host}|{ua[:120]}".encode("utf-8")).hexdigest()


class MemoryLicenseRateLimiter:
    def __init__(self) -> None:
        self._counts: dict[str, tuple[int, int]] = {}
        self._invalid_tokens: dict[str, int] = {}
        self._decode_inflight = 0
        self._lock = asyncio.Lock()

    async def check_client_rate(self, bucket: str, client_id: str, limit: int, window_seconds: int = 60) -> None:
        async with self._lock:
            self._cleanup_locked()
            now = int(time.time())
            window = now // window_seconds
            key = f"{bucket}:{client_id}:{window}"
            count, expires = self._counts.get(key, (0, now + window_seconds))
            if count >= limit:
                raise RateLimitExceeded("rate limit exceeded")
            self._counts[key] = (count + 1, expires)

    async def is_invalid_token_denied(self, token_digest_value: str) -> bool:
        async with self._lock:
            self._cleanup_locked()
            return token_digest_value in self._invalid_tokens

    async def mark_invalid_token(self, token_digest_value: str, ttl_seconds: int) -> None:
        async with self._lock:
            self._cleanup_locked()
            self._invalid_tokens[token_digest_value] = int(time.time()) + max(1, ttl_seconds)

    async def acquire_decode_slot(self, limit: int) -> DecodeSlot:
        async with self._lock:
            if self._decode_inflight >= limit:
                raise RateLimitExceeded("decode concurrency exceeded")
            self._decode_inflight += 1
            return DecodeSlot("memory", True)

    async def release_decode_slot(self, slot: DecodeSlot) -> None:
        if not slot.acquired:
            return
        async with self._lock:
            self._decode_inflight = max(0, self._decode_inflight - 1)

    def _cleanup_locked(self) -> None:
        now = int(time.time())
        self._counts = {key: value for key, value in self._counts.items() if value[1] >= now}
        self._invalid_tokens = {key: exp for key, exp in self._invalid_tokens.items() if exp >= now}


class RedisLicenseRateLimiter:
    def __init__(self, redis_url: str, prefix: str = "nf:license") -> None:
        if not redis_url:
            raise VerifierUnavailable("REDIS_URL is required")
        try:
            import redis.asyncio as redis
        except Exception as exc:
            raise VerifierUnavailable("redis dependency is not installed") from exc
        self._redis = redis.from_url(redis_url, decode_responses=True)
        self._prefix = prefix

    async def check_client_rate(self, bucket: str, client_id: str, limit: int, window_seconds: int = 60) -> None:
        now = int(time.time())
        window = now // window_seconds
        key = f"{self._prefix}:rate:{bucket}:{client_id}:{window}"
        count = await self._redis.incr(key)
        if count == 1:
            await self._redis.expire(key, window_seconds + 2)
        if int(count) > limit:
            raise RateLimitExceeded("rate limit exceeded")

    async def is_invalid_token_denied(self, token_digest_value: str) -> bool:
        return bool(await self._redis.exists(self._invalid_key(token_digest_value)))

    async def mark_invalid_token(self, token_digest_value: str, ttl_seconds: int) -> None:
        await self._redis.set(self._invalid_key(token_digest_value), "1", ex=max(1, ttl_seconds))

    async def acquire_decode_slot(self, limit: int) -> DecodeSlot:
        key = f"{self._prefix}:decode:inflight"
        count = int(await self._redis.incr(key))
        if count == 1:
            await self._redis.expire(key, 30)
        if count > limit:
            await self._redis.decr(key)
            raise RateLimitExceeded("decode concurrency exceeded")
        return DecodeSlot(key, True)

    async def release_decode_slot(self, slot: DecodeSlot) -> None:
        if slot.acquired:
            await self._redis.decr(slot.key)

    def _invalid_key(self, token_digest_value: str) -> str:
        return f"{self._prefix}:invalid_token:{token_digest_value}"


_memory_limiter = MemoryLicenseRateLimiter()
_redis_limiter: RedisLicenseRateLimiter | None = None


def get_license_rate_limiter(
    settings: Annotated[Settings | None, Depends(get_settings)] = None,
) -> LicenseRateLimiter:
    global _redis_limiter
    settings = settings or get_settings()
    if settings.license_state_backend == "memory":
        if settings.is_production and (settings.play_integrity_enforcement_enabled or settings.app_access_lockdown_enabled):
            raise VerifierUnavailable("production rate limiting requires Redis")
        return _memory_limiter
    if settings.license_state_backend == "redis":
        if _redis_limiter is None:
            _redis_limiter = RedisLicenseRateLimiter(settings.redis_url)
        return _redis_limiter
    raise VerifierUnavailable("unsupported LICENSE_STATE_BACKEND")
