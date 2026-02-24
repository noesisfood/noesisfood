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

# Serve static files (if you have css/js/images later)
# Accessed like /static/...
app.mount("/static", StaticFiles(directory=str(FRONTEND_DIR)), name="static")

# Include API routes
app.include_router(scan_router)


@app.get("/")
async def serve_ui():
    # Serve the UI instead of JSON
    return FileResponse(str(INDEX_FILE))


@app.get("/health")
async def health():
    # Keep a JSON health endpoint for monitoring
    return {"status": "NoesisFood API running"}