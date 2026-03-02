# app/api/routes/scan.py

import logging
from fastapi import APIRouter
from fastapi.responses import JSONResponse

from app.services.scanner_service import scan_product

logger = logging.getLogger("noesisfood.scan")

router = APIRouter()

@router.get("/scan/{key}")
async def scan_endpoint(key: str):
    try:
        data = await scan_product(key)
        # if service returns {"error": "..."} keep it as JSON with 404-ish semantics
        if isinstance(data, dict) and data.get("error"):
            return JSONResponse(status_code=404, content=data)
        return data
    except Exception as e:
        # This prints full traceback in Render logs
        logger.exception("Scan failed for key=%s", key)
        # Return a safe JSON error so frontend doesn't just show blank 500
        return JSONResponse(
            status_code=500,
            content={"error": "Internal Server Error", "detail": str(e)},
        )