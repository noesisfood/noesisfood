# app/main.py

import hashlib
import json
import logging
import re
import uuid
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.exceptions import RequestValidationError
from fastapi.exception_handlers import request_validation_exception_handler
from fastapi.responses import FileResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles

from app.api.routes.license import router as license_router
from app.api.routes.scan import router as scan_router
from app.config import get_settings, validate_runtime_settings
from app.licensing import request_has_active_licensed_session

app = FastAPI(title="NoesisFood API", version="0.3.1")
logger = logging.getLogger("noesisfood.license")


_LICENSE_REQUEST_ID_STATE_KEY = "noesisfood_license_request_id"
_ASCII_WHITESPACE = frozenset(b" \t\r\n\f\v")


def _body_edge_category(value: int | None) -> str:
    if value is None:
        return "none"
    categories = {
        ord("{"): "object_open",
        ord("}"): "object_close",
        ord("["): "array_open",
        ord("]"): "array_close",
        ord('"'): "quote",
        ord("`"): "backtick",
        ord("\\"): "backslash",
        ord(":"): "colon",
        ord(","): "comma",
        ord("-"): "minus",
    }
    if value in categories:
        return categories[value]
    if ord("a") <= value <= ord("z") or ord("A") <= value <= ord("Z"):
        return "letter"
    if ord("0") <= value <= ord("9"):
        return "digit"
    if value < 128:
        return "other_ascii"
    return "non_ascii"


def _content_type_category(headers: dict[bytes, bytes]) -> str:
    raw = headers.get(b"content-type")
    if raw is None:
        return "absent"
    try:
        value = raw.decode("ascii").strip().lower()
    except UnicodeDecodeError:
        return "invalid"
    if not value:
        return "invalid"
    return "application/json" if value.split(";", 1)[0].strip() == "application/json" else "other"


def _content_encoding_category(headers: dict[bytes, bytes]) -> str:
    raw = headers.get(b"content-encoding")
    if raw is None:
        return "absent"
    try:
        value = raw.decode("ascii").strip().lower()
    except UnicodeDecodeError:
        return "invalid"
    if value in {"identity", "gzip", "deflate", "br"}:
        return value
    return "invalid" if not value else "other"


def _content_length_category(headers: dict[bytes, bytes]) -> str:
    raw = headers.get(b"content-length")
    if raw is None:
        return "absent"
    try:
        value = raw.decode("ascii").strip()
    except UnicodeDecodeError:
        return "invalid"
    return "integer" if re.fullmatch(r"[0-9]+", value) else "invalid"


def _json_top_level_type(value) -> str:
    if isinstance(value, dict):
        return "object"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    if isinstance(value, bool):
        return "boolean"
    if value is None:
        return "null"
    if isinstance(value, (int, float)):
        return "number"
    return "not_available"


def _license_request_id(scope) -> str:
    state = scope.get("state")
    candidate = state.get(_LICENSE_REQUEST_ID_STATE_KEY) if isinstance(state, dict) else None
    if not isinstance(candidate, str):
        return "unavailable"
    try:
        return str(uuid.UUID(candidate))
    except (ValueError, AttributeError):
        return "unavailable"


def _shape_bucket(count: int) -> str:
    if count == 0:
        return "zero"
    if count == 1:
        return "one"
    return "multiple"


class LicenseIngressDiagnostics:
    """Transparent ASGI receive classification for the license session path."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope.get("type") != "http" or scope.get("method") != "POST" or scope.get("path") != "/license/session":
            await self.app(scope, receive, send)
            return

        try:
            headers = {key.lower(): value for key, value in scope.get("headers", [])}
            request_id = str(uuid.uuid4())
            state = scope.get("state")
            if not isinstance(state, dict):
                state = {}
                scope["state"] = state
            state[_LICENSE_REQUEST_ID_STATE_KEY] = request_id

            total_size = 0
            retained = bytearray()
            retained_truncated = False
            first_non_whitespace = None
            last_non_whitespace = None
            raw_body_hash = hashlib.sha256()
            finalized = False
            max_bytes = max(0, get_settings().license_session_body_max_bytes)
        except Exception:
            await self.app(scope, receive, send)
            return

        diagnostic_failed = False

        def emit():
            nonlocal finalized
            if finalized:
                return
            finalized = True
            json_top_level_type = "not_available"
            has_integrity_token_key = "not_applicable"
            has_challenge_token_key = "not_applicable"
            if retained_truncated:
                utf8_state = "not_attempted"
                json_state = "not_attempted"
            else:
                try:
                    decoded = bytes(retained).decode("utf-8")
                    utf8_state = "success"
                    try:
                        parsed = json.loads(decoded)
                        json_state = "success"
                        json_top_level_type = _json_top_level_type(parsed)
                        if isinstance(parsed, dict):
                            has_integrity_token_key = "true" if "integrity_token" in parsed else "false"
                            has_challenge_token_key = "true" if "challenge_token" in parsed else "false"
                    except Exception:
                        json_state = "failure"
                except UnicodeDecodeError:
                    utf8_state = "failure"
                    json_state = "not_attempted"
            logger.warning(
                "event=license_session_ingress_v2 request_id=%s raw_body_bytes=%s body_empty=%s first_non_whitespace_category=%s last_non_whitespace_category=%s utf8_decode=%s json_parse=%s json_top_level_type=%s has_integrity_token_key=%s has_challenge_token_key=%s raw_body_sha256=%s content_type=%s content_encoding=%s content_length=%s",
                request_id,
                total_size,
                "true" if total_size == 0 else "false",
                _body_edge_category(first_non_whitespace),
                _body_edge_category(last_non_whitespace),
                utf8_state,
                json_state,
                json_top_level_type,
                has_integrity_token_key,
                has_challenge_token_key,
                raw_body_hash.hexdigest(),
                _content_type_category(headers),
                _content_encoding_category(headers),
                _content_length_category(headers),
            )

        async def diagnostic_receive():
            nonlocal diagnostic_failed, total_size, retained_truncated, first_non_whitespace, last_non_whitespace
            message = await receive()
            if diagnostic_failed:
                return message
            try:
                if finalized or message.get("type") != "http.request":
                    return message
                body = message.get("body", b"")
                if body:
                    total_size += len(body)
                    raw_body_hash.update(body)
                    if first_non_whitespace is None:
                        first_non_whitespace = next((value for value in body if value not in _ASCII_WHITESPACE), None)
                    chunk_last_non_whitespace = next(
                        (value for value in reversed(body) if value not in _ASCII_WHITESPACE),
                        None,
                    )
                    if chunk_last_non_whitespace is not None:
                        last_non_whitespace = chunk_last_non_whitespace
                    remaining = max_bytes - len(retained)
                    if remaining > 0:
                        retained.extend(body[:remaining])
                    if total_size > max_bytes:
                        retained_truncated = True
                if not message.get("more_body", False):
                    emit()
            except Exception:
                diagnostic_failed = True
            return message

        await self.app(scope, diagnostic_receive, send)

settings = get_settings()
validate_runtime_settings(settings)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def license_session_body_limit_middleware(request: Request, call_next):
    if request.url.path == "/license/session":
        content_length = request.headers.get("content-length")
        if content_length:
            try:
                if int(content_length) > get_settings().license_session_body_max_bytes:
                    return PlainTextResponse("License request too large", status_code=413, headers=_cache_headers(public=False))
            except ValueError:
                logger.warning("license_session_bad_request reason=malformed_content_length_middleware")
                return PlainTextResponse("Malformed license request", status_code=400, headers=_cache_headers(public=False))
    return await call_next(request)


app.add_middleware(LicenseIngressDiagnostics)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    if request.url.path == "/license/session":
        try:
            logger.warning("license_session_bad_request reason=request_validation_error")
            errors = exc.errors()
            error_type = "other"
            location = "other"
            if errors:
                candidate_type = errors[0].get("type")
                if isinstance(candidate_type, str) and re.fullmatch(r"[a-z0-9_.-]{1,64}", candidate_type):
                    error_type = candidate_type
                candidate_location = (errors[0].get("loc") or [None])[0]
                if candidate_location in {"body", "header", "query", "path"}:
                    location = candidate_location
            logger.warning(
                "event=license_validation_error_v1 request_id=%s type=%s location=%s",
                _license_request_id(request.scope),
                error_type,
                location,
            )
        except Exception:
            pass
        return PlainTextResponse("Malformed license request", status_code=400, headers=_cache_headers(public=False))
    return await request_validation_exception_handler(request, exc)

# ---- Paths ----
APP_DIR = Path(__file__).resolve().parent          # app/
FRONTEND_DIR = APP_DIR / "frontend"                # app/frontend/
INDEX_FILE = FRONTEND_DIR / "index.html"
LANDING_FILE = FRONTEND_DIR / "landing.html"
PRIVACY_FILE = FRONTEND_DIR / "privacy.html"
DATA_DELETION_FILE = FRONTEND_DIR / "data-deletion.html"
MANIFEST_FILE = FRONTEND_DIR / "manifest.webmanifest"
SERVICE_WORKER_FILE = FRONTEND_DIR / "service-worker.js"
ICONS_DIR = FRONTEND_DIR / "icons"
WELL_KNOWN_DIR = FRONTEND_DIR / ".well-known"
ASSETLINKS_FILE = WELL_KNOWN_DIR / "assetlinks.json"

app.mount("/icons", StaticFiles(directory=str(ICONS_DIR)), name="icons")

# Include API routes
app.include_router(license_router)
app.include_router(scan_router)


@app.on_event("startup")
async def log_license_runtime_shape():
    route = next(
        (
            route
            for route in app.routes
            if getattr(route, "path", None) == "/license/session"
            and getattr(route, "methods", set()) == {"POST"}
        ),
        None,
    )
    if route is None:
        logger.warning(
            "license_runtime_shape_v1 endpoint=missing body_field_present=no body_parameter_count=zero dependency_body_parameter_count=zero"
        )
        return
    dependant = route.dependant
    dependency_body_count = sum(len(getattr(dep, "body_params", [])) for dep in dependant.dependencies)
    endpoint = getattr(route.endpoint, "__module__", "unknown") + "." + getattr(route.endpoint, "__qualname__", "unknown")
    logger.warning(
        "license_runtime_shape_v1 endpoint=%s body_field_present=%s body_parameter_count=%s dependency_body_parameter_count=%s",
        endpoint,
        "yes" if getattr(dependant, "body_params", []) else "no",
        _shape_bucket(len(getattr(dependant, "body_params", []))),
        _shape_bucket(dependency_body_count),
    )


@app.get("/")
async def serve_ui(request: Request):
    settings = get_settings()
    if settings.app_access_lockdown_enabled and not await request_has_active_licensed_session(request, settings=settings):
        return _html_response(LANDING_FILE, public=True)
    return _html_response(INDEX_FILE, public=False)


@app.get("/privacy")
async def serve_privacy():
    return _html_response(PRIVACY_FILE, public=True)


@app.get("/data-deletion")
async def serve_data_deletion():
    return _html_response(DATA_DELETION_FILE, public=True)


@app.get("/manifest.webmanifest")
async def serve_manifest():
    return FileResponse(str(MANIFEST_FILE), media_type="application/manifest+json", headers=_cache_headers(public=True))


@app.get("/service-worker.js")
async def serve_service_worker(request: Request):
    settings = get_settings()
    if settings.app_access_lockdown_enabled and not await request_has_active_licensed_session(request, settings=settings):
        return PlainTextResponse(
            'self.addEventListener("install",e=>self.skipWaiting());'
            'self.addEventListener("activate",e=>{e.waitUntil(caches.keys().then(keys=>Promise.all(keys.map(k=>/^noesisfood-shell-v[0-9]+$/.test(k)?caches.delete(k):false))).then(()=>self.registration.unregister()).then(()=>self.clients.claim()))});',
            media_type="application/javascript",
            headers=_cache_headers(public=False),
        )
    return FileResponse(str(SERVICE_WORKER_FILE), media_type="application/javascript", headers=_cache_headers(public=False))


@app.get("/.well-known/assetlinks.json")
async def serve_assetlinks():
    return FileResponse(str(ASSETLINKS_FILE), media_type="application/json", headers=_cache_headers(public=True))


@app.get("/health")
async def health():
    # Keep a JSON health endpoint for monitoring
    return {"status": "NoesisFood API running"}


PUBLIC_STATIC_FILES = {
    "license-bootstrap.js": "application/javascript",
    "brand/noesisfood-logo-source.png": "image/png",
    "brand/noesisfood-full-logo-source.png": "image/png",
}


@app.get("/static/{asset_path:path}")
async def serve_static_asset(asset_path: str, request: Request):
    normalized = "/".join(part for part in asset_path.split("/") if part not in {"", ".", ".."})
    if normalized in PUBLIC_STATIC_FILES:
        return FileResponse(
            str(FRONTEND_DIR / normalized),
            media_type=PUBLIC_STATIC_FILES[normalized],
            headers=_cache_headers(public=True),
        )
    settings = get_settings()
    if settings.app_access_lockdown_enabled and not await request_has_active_licensed_session(request, settings=settings):
        if normalized.endswith(".html"):
            return _html_response(LANDING_FILE, public=True)
        return PlainTextResponse("Not found", status_code=404, headers=_cache_headers(public=False))
    target = FRONTEND_DIR / normalized
    if not target.is_file() or FRONTEND_DIR not in target.resolve().parents:
        return PlainTextResponse("Not found", status_code=404, headers=_cache_headers(public=False))
    return FileResponse(str(target), headers=_cache_headers(public=False))


def _html_response(path: Path, public: bool) -> FileResponse:
    return FileResponse(str(path), media_type="text/html", headers=_cache_headers(public=public))


def _cache_headers(public: bool) -> dict[str, str]:
    if public:
        return {"Cache-Control": "public, max-age=300"}
    return {"Cache-Control": "no-store"}
