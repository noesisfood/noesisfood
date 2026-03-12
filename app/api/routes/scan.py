# app/api/routes/scan.py

import logging
from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from app.services.scanner_service import scan_product

logger = logging.getLogger("noesisfood.scan")

router = APIRouter()


def _error_status(data: dict) -> int:
    code = str(data.get("error_code") or "").upper()
    if isinstance(data.get("status_code"), int):
        return int(data["status_code"])
    if code == "INVALID_BARCODE":
        return 400
    if code == "PRODUCT_NOT_FOUND":
        return 404
    if code == "MISSING_KEY_DATA":
        return 422
    if code == "ANALYSIS_UNAVAILABLE":
        return 422
    return 400

@router.get("/scan/{key}")
async def scan_endpoint(key: str, lang: str = Query("en")):
    try:
        lang = lang if lang in {"el", "en", "de", "fr"} else "en"
        data = await scan_product(key, lang=lang)
        # if service returns {"error": "..."} keep it as JSON with 404-ish semantics
        if isinstance(data, dict) and data.get("error"):
            return JSONResponse(status_code=_error_status(data), content=data)
        return data
    except Exception as e:
        # This prints full traceback in Render logs
        logger.exception("Scan failed for key=%s", key)
        # Return a safe JSON error so frontend doesn't just show blank 500
        return JSONResponse(
            status_code=422,
            content={
                "error": "This product could not be analyzed.",
                "error_code": "ANALYSIS_UNAVAILABLE",
            },
        )
