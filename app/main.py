# app/main.py

import logging
import json
import re
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


def _size_bucket(size: int) -> str:
    if size == 0:
        return "zero"
    if size <= 256:
        return "1_to_256"
    if size <= 1024:
        return "257_to_1024"
    return "over_1024"


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

        headers = {key.lower(): value for key, value in scope.get("headers", [])}
        content_type = headers.get(b"content-type", b"").split(b";", 1)[0].lower()
        if content_type == b"application/json":
            content_type_class = "application_json"
        elif b"content-type" not in headers:
            content_type_class = "missing"
        else:
            content_type_class = "other"
        if b"content-length" in headers:
            framing_class = "content_length"
        elif b"transfer-encoding" in headers and b"chunked" in headers[b"transfer-encoding"].lower():
            framing_class = "chunked"
        elif b"transfer-encoding" in headers:
            framing_class = "other"
        else:
            framing_class = "none"

        chunk_count = 0
        total_size = 0
        retained = bytearray()
        retained_truncated = False
        more_body_count = 0
        more_body_invalid = False
        finalized = False
        max_bytes = get_settings().license_session_body_max_bytes

        def emit():
            nonlocal finalized
            if finalized:
                return
            finalized = True
            if retained_truncated:
                utf8_state = "valid"
                json_state = "invalid"
            elif not retained:
                utf8_state = "valid"
                json_state = "empty"
            else:
                try:
                    decoded = bytes(retained).decode("utf-8")
                    utf8_state = "valid"
                    try:
                        parsed = json.loads(decoded)
                        json_state = "valid_object" if isinstance(parsed, dict) else "valid_non_object"
                    except Exception:
                        json_state = "invalid"
                except UnicodeDecodeError:
                    utf8_state = "invalid"
                    json_state = "invalid"
            logger.warning(
                "event=license_ingress_body_v1 chunk_count=%s total_size_bucket=%s more_body_sequence=%s utf8_state=%s json_state=%s content_type_class=%s framing_class=%s",
                _shape_bucket(chunk_count),
                _size_bucket(total_size),
                "invalid" if more_body_invalid else ("single" if more_body_count <= 1 else "multiple"),
                utf8_state,
                json_state,
                content_type_class,
                framing_class,
            )

        async def diagnostic_receive():
            nonlocal chunk_count, total_size, more_body_count, more_body_invalid, retained_truncated
            message = await receive()
            if message.get("type") != "http.request":
                more_body_invalid = True
                emit()
                return message
            body = message.get("body", b"")
            if body:
                chunk_count += 1
                total_size += len(body)
                if len(retained) < max_bytes:
                    retained.extend(body[: max_bytes - len(retained)])
                if len(retained) >= max_bytes and total_size > max_bytes:
                    retained_truncated = True
            if not isinstance(message.get("more_body", False), bool):
                more_body_invalid = True
            more_body_count += 1
            if not message.get("more_body", False):
                emit()
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
        logger.warning("event=license_validation_error_v1 type=%s location=%s", error_type, location)
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
