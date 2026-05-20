# app/main.py

from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from app.api.routes.scan import router as scan_router

app = FastAPI(title="NoesisFood API", version="0.3.1")

# CORS (dev friendly)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---- Paths ----
APP_DIR = Path(__file__).resolve().parent          # app/
FRONTEND_DIR = APP_DIR / "frontend"                # app/frontend/
INDEX_FILE = FRONTEND_DIR / "index.html"
MANIFEST_FILE = FRONTEND_DIR / "manifest.webmanifest"
SERVICE_WORKER_FILE = FRONTEND_DIR / "service-worker.js"
ICONS_DIR = FRONTEND_DIR / "icons"
WELL_KNOWN_DIR = FRONTEND_DIR / ".well-known"
ASSETLINKS_FILE = WELL_KNOWN_DIR / "assetlinks.json"

# Serve static files (if you have css/js/images later)
# Accessed like /static/...
app.mount("/static", StaticFiles(directory=str(FRONTEND_DIR)), name="static")
app.mount("/icons", StaticFiles(directory=str(ICONS_DIR)), name="icons")

# Include API routes
app.include_router(scan_router)


@app.get("/")
async def serve_ui():
    # Serve the UI instead of JSON
    return FileResponse(str(INDEX_FILE))


@app.get("/manifest.webmanifest")
async def serve_manifest():
    return FileResponse(str(MANIFEST_FILE), media_type="application/manifest+json")


@app.get("/service-worker.js")
async def serve_service_worker():
    return FileResponse(str(SERVICE_WORKER_FILE), media_type="application/javascript")


@app.get("/.well-known/assetlinks.json")
async def serve_assetlinks():
    return FileResponse(str(ASSETLINKS_FILE), media_type="application/json")


@app.get("/health")
async def health():
    # Keep a JSON health endpoint for monitoring
    return {"status": "NoesisFood API running"}
