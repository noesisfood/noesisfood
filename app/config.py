import os
from dataclasses import dataclass


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        return int(raw.strip())
    except ValueError:
        return default


def _env_list(name: str, default: str) -> list[str]:
    raw = os.environ.get(name, default)
    return [item.strip() for item in raw.split(",") if item.strip()]


@dataclass(frozen=True)
class Settings:
    app_env: str
    app_access_lockdown_enabled: bool
    play_integrity_enforcement_enabled: bool
    play_integrity_package_name: str
    play_integrity_min_version_code: int
    play_integrity_cloud_project_number: str
    license_session_secret: str
    license_session_ttl_seconds: int
    license_challenge_ttl_seconds: int
    license_challenge_max_attempts: int
    play_integrity_token_max_age_seconds: int
    public_base_url: str
    cors_allowed_origins: list[str]
    license_state_backend: str
    redis_url: str
    license_challenge_rate_limit_per_minute: int
    license_session_rate_limit_per_minute: int
    license_session_body_max_bytes: int
    play_integrity_max_concurrent_decodes: int
    invalid_token_deny_ttl_seconds: int
    trusted_proxy_hops: int

    @property
    def is_production(self) -> bool:
        return self.app_env == "production" or _env_bool("RENDER", False)


def get_settings() -> Settings:
    return Settings(
        app_env=os.environ.get("APP_ENV", os.environ.get("ENVIRONMENT", "development")).strip().lower()
        or "development",
        app_access_lockdown_enabled=_env_bool("APP_ACCESS_LOCKDOWN_ENABLED", False),
        play_integrity_enforcement_enabled=_env_bool("PLAY_INTEGRITY_ENFORCEMENT_ENABLED", False),
        play_integrity_package_name=os.environ.get("PLAY_INTEGRITY_PACKAGE_NAME", "com.noesisfood.app").strip()
        or "com.noesisfood.app",
        play_integrity_min_version_code=_env_int("PLAY_INTEGRITY_MIN_VERSION_CODE", 9),
        play_integrity_cloud_project_number=os.environ.get("PLAY_INTEGRITY_CLOUD_PROJECT_NUMBER", "").strip(),
        license_session_secret=os.environ.get("LICENSE_SESSION_SECRET", "").strip(),
        license_session_ttl_seconds=_env_int("LICENSE_SESSION_TTL_SECONDS", 900),
        license_challenge_ttl_seconds=_env_int("LICENSE_CHALLENGE_TTL_SECONDS", 120),
        license_challenge_max_attempts=_env_int("LICENSE_CHALLENGE_MAX_ATTEMPTS", 1),
        play_integrity_token_max_age_seconds=_env_int("PLAY_INTEGRITY_TOKEN_MAX_AGE_SECONDS", 120),
        public_base_url=os.environ.get("PUBLIC_BASE_URL", "https://noesisfood.app").strip()
        or "https://noesisfood.app",
        cors_allowed_origins=_env_list("CORS_ALLOWED_ORIGINS", "https://noesisfood.app"),
        license_state_backend=os.environ.get("LICENSE_STATE_BACKEND", "memory").strip().lower() or "memory",
        redis_url=os.environ.get("REDIS_URL", "").strip(),
        license_challenge_rate_limit_per_minute=_env_int("LICENSE_CHALLENGE_RATE_LIMIT_PER_MINUTE", 10),
        license_session_rate_limit_per_minute=_env_int("LICENSE_SESSION_RATE_LIMIT_PER_MINUTE", 5),
        license_session_body_max_bytes=_env_int("LICENSE_SESSION_BODY_MAX_BYTES", 16384),
        play_integrity_max_concurrent_decodes=_env_int("PLAY_INTEGRITY_MAX_CONCURRENT_DECODES", 5),
        invalid_token_deny_ttl_seconds=_env_int("INVALID_TOKEN_DENY_TTL_SECONDS", 60),
        trusted_proxy_hops=_env_int("TRUSTED_PROXY_HOPS", 1),
    )


def validate_runtime_settings(settings: Settings) -> None:
    if settings.app_access_lockdown_enabled and not settings.play_integrity_enforcement_enabled:
        raise RuntimeError("APP_ACCESS_LOCKDOWN_ENABLED=true requires PLAY_INTEGRITY_ENFORCEMENT_ENABLED=true")
    if not settings.play_integrity_enforcement_enabled and not settings.app_access_lockdown_enabled:
        return
    validate_license_secret(settings.license_session_secret)
    if settings.license_challenge_max_attempts < 1:
        raise RuntimeError("LICENSE_CHALLENGE_MAX_ATTEMPTS must be at least 1")
    if settings.is_production and settings.license_state_backend != "redis":
        raise RuntimeError("Production Play Integrity enforcement requires LICENSE_STATE_BACKEND=redis")
    if settings.license_state_backend == "redis" and not settings.redis_url:
        raise RuntimeError("LICENSE_STATE_BACKEND=redis requires REDIS_URL")


def validate_license_secret(secret: str) -> None:
    value = str(secret or "").strip()
    placeholders = {
        "changeme",
        "change-me",
        "secret",
        "test",
        "password",
        "license_session_secret",
        "replace-me",
        "your-secret-here",
    }
    if value.lower() in placeholders or len(value) < 43:
        raise RuntimeError("LICENSE_SESSION_SECRET must be a strong random secret")
