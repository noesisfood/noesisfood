import asyncio
import json
import time
from dataclasses import dataclass
from typing import Any, Protocol

from app.config import Settings, get_settings
from app.licensing import LicenseError, VerifierUnavailable


class ChallengeConsumed(LicenseError):
    pass


class ChallengeUnavailable(LicenseError):
    pass


@dataclass(frozen=True)
class ChallengeRecord:
    challenge_id: str
    request_hash: str
    issued_at: int
    expires_at: int
    max_attempts: int


class LicenseStateStore(Protocol):
    async def store_challenge(self, record: ChallengeRecord) -> None:
        ...

    async def consume_challenge(self, challenge_id: str) -> ChallengeRecord:
        ...

    async def revoke_session(self, session_id: str, expires_at: int) -> None:
        ...

    async def is_session_revoked(self, session_id: str) -> bool:
        ...


class MemoryLicenseStateStore:
    def __init__(self, max_challenges: int = 2048) -> None:
        self._max_challenges = max_challenges
        self._challenges: dict[str, ChallengeRecord] = {}
        self._revoked_sessions: dict[str, int] = {}
        self._lock = asyncio.Lock()

    async def store_challenge(self, record: ChallengeRecord) -> None:
        async with self._lock:
            self._cleanup_locked()
            if len(self._challenges) >= self._max_challenges:
                oldest = min(self._challenges.values(), key=lambda item: item.expires_at)
                self._challenges.pop(oldest.challenge_id, None)
            self._challenges[record.challenge_id] = record

    async def consume_challenge(self, challenge_id: str) -> ChallengeRecord:
        async with self._lock:
            self._cleanup_locked()
            record = self._challenges.pop(challenge_id, None)
            if record is None:
                raise ChallengeConsumed("challenge missing or already consumed")
            if record.expires_at < int(time.time()):
                raise ChallengeUnavailable("challenge expired")
            return record

    async def revoke_session(self, session_id: str, expires_at: int) -> None:
        async with self._lock:
            self._cleanup_locked()
            self._revoked_sessions[session_id] = expires_at

    async def is_session_revoked(self, session_id: str) -> bool:
        async with self._lock:
            self._cleanup_locked()
            return session_id in self._revoked_sessions

    def _cleanup_locked(self) -> None:
        now = int(time.time())
        self._challenges = {key: item for key, item in self._challenges.items() if item.expires_at >= now}
        self._revoked_sessions = {key: exp for key, exp in self._revoked_sessions.items() if exp >= now}


class RedisLicenseStateStore:
    def __init__(self, redis_url: str, prefix: str = "nf:license") -> None:
        if not redis_url:
            raise VerifierUnavailable("REDIS_URL is required")
        try:
            import redis.asyncio as redis
        except Exception as exc:
            raise VerifierUnavailable("redis dependency is not installed") from exc
        self._redis = redis.from_url(redis_url, decode_responses=True)
        self._prefix = prefix

    async def store_challenge(self, record: ChallengeRecord) -> None:
        ttl = max(1, record.expires_at - int(time.time()))
        payload = json.dumps(
            {
                "challenge_id": record.challenge_id,
                "request_hash": record.request_hash,
                "issued_at": record.issued_at,
                "expires_at": record.expires_at,
                "max_attempts": record.max_attempts,
            },
            separators=(",", ":"),
            sort_keys=True,
        )
        ok = await self._redis.set(self._challenge_key(record.challenge_id), payload, ex=ttl, nx=True)
        if not ok:
            raise VerifierUnavailable("challenge storage collision")

    async def consume_challenge(self, challenge_id: str) -> ChallengeRecord:
        key = self._challenge_key(challenge_id)
        try:
            payload = await self._redis.getdel(key)
        except Exception:
            payload = await self._redis.eval("local v=redis.call('GET', KEYS[1]); if v then redis.call('DEL', KEYS[1]); end; return v", 1, key)
        if not payload:
            raise ChallengeConsumed("challenge missing or already consumed")
        try:
            data = json.loads(payload)
            record = ChallengeRecord(
                challenge_id=str(data["challenge_id"]),
                request_hash=str(data["request_hash"]),
                issued_at=int(data["issued_at"]),
                expires_at=int(data["expires_at"]),
                max_attempts=int(data.get("max_attempts") or 1),
            )
        except Exception as exc:
            raise ChallengeUnavailable("stored challenge malformed") from exc
        if record.expires_at < int(time.time()):
            raise ChallengeUnavailable("challenge expired")
        return record

    async def revoke_session(self, session_id: str, expires_at: int) -> None:
        ttl = max(1, expires_at - int(time.time()))
        await self._redis.set(self._session_key(session_id), "1", ex=ttl)

    async def is_session_revoked(self, session_id: str) -> bool:
        return bool(await self._redis.exists(self._session_key(session_id)))

    def _challenge_key(self, challenge_id: str) -> str:
        return f"{self._prefix}:challenge:{challenge_id}"

    def _session_key(self, session_id: str) -> str:
        return f"{self._prefix}:revoked_session:{session_id}"


_memory_store = MemoryLicenseStateStore()
_redis_store: RedisLicenseStateStore | None = None


def get_license_state_store(settings: Settings | None = None) -> LicenseStateStore:
    global _redis_store
    settings = settings or get_settings()
    if settings.license_state_backend == "memory":
        if settings.is_production and (settings.play_integrity_enforcement_enabled or settings.app_access_lockdown_enabled):
            raise VerifierUnavailable("production licensing requires Redis state")
        return _memory_store
    if settings.license_state_backend == "redis":
        if _redis_store is None:
            _redis_store = RedisLicenseStateStore(settings.redis_url)
        return _redis_store
    raise VerifierUnavailable("unsupported LICENSE_STATE_BACKEND")
