# app/main.py

import logging
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


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    if request.url.path == "/license/session":
        logger.warning("license_session_bad_request reason=request_validation_error")
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
