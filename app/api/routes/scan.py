# app/api/routes/scan.py

import logging
import os
import time
from fastapi import APIRouter, Body, Header, Query
from fastapi.responses import JSONResponse

from app.services.correction_feedback_service import submit_correction_feedback
from app.services.internal_beta_review_service import get_internal_beta_review_summary
from app.services.monitoring_service import log_event
from app.services.scanner_service import _decode_image_data_url, analyze_manual_product, analyze_photo_product, scan_product

logger = logging.getLogger("noesisfood.scan")

router = APIRouter()


def _error_status(data: dict) -> int:
    code = str(data.get("error_code") or "").upper()
    if code == "ANALYSIS_UNAVAILABLE":
        return 503
    if code == "PHOTO_EXTRACTION_UNAVAILABLE":
        return 503
    if code == "PHOTO_EXTRACTION_FAILED":
        return 409
    if code == "PHOTO_PARSING_FAILED":
        return 409
    if isinstance(data.get("status_code"), int):
        return int(data["status_code"])
    if code == "INVALID_BARCODE":
        return 400
    if code == "PRODUCT_NOT_FOUND":
        return 404
    if code == "MISSING_KEY_DATA":
        return 422
    return 400


def _photo_payload_summary(payload: dict) -> dict:
    body = payload if isinstance(payload, dict) else {}
    ingredient_data_url = str(body.get("ingredient_image_data_url") or "").strip()
    nutrition_data_url = str(body.get("nutrition_image_data_url") or "").strip()
    existing_analysis = body.get("existing_analysis") if isinstance(body.get("existing_analysis"), dict) else {}
    return {
        "lang": None,
        "ingredient_upload_present": bool(ingredient_data_url),
        "ingredient_crop_applied": bool(body.get("ingredient_crop_applied")),
        "ingredient_image_chars": len(ingredient_data_url),
        "ingredient_image_bytes_present": bool(_decode_image_data_url(ingredient_data_url)),
        "nutrition_upload_present": bool(nutrition_data_url),
        "nutrition_crop_applied": bool(body.get("nutrition_crop_applied")),
        "nutrition_image_chars": len(nutrition_data_url),
        "nutrition_image_bytes_present": bool(_decode_image_data_url(nutrition_data_url)),
        "existing_key": str(body.get("existing_key") or existing_analysis.get("key") or ""),
    }


def _beta_review_token_valid(header_value: str | None) -> tuple[bool, bool]:
    configured = str(os.environ.get("BETA_REVIEW_TOKEN") or "").strip()
    if not configured:
        return False, False
    provided = str(header_value or "").strip()
    return provided == configured, True

@router.get("/scan/{key}")
async def scan_endpoint(key: str, lang: str = Query("en")):
    started_at = time.perf_counter()
    lang = lang if lang in {"el", "en", "de", "fr"} else "en"
    log_event(logger, "scan_started", source="barcode", key=str(key or "").strip(), lang=lang)
    try:
        data = await scan_product(key, lang=lang)
        if isinstance(data, dict):
            meta = data.get("meta")
            if not isinstance(meta, dict):
                meta = {}
                data["meta"] = meta
            perf = meta.get("performance")
            if not isinstance(perf, dict):
                perf = {}
                meta["performance"] = perf
            perf["route_total_ms"] = int(round((time.perf_counter() - started_at) * 1000.0))
            logger.info(
                "scan endpoint key=%s status=%s resolution_state=%s render_allowed=%s reason=%s",
                key,
                "error" if data.get("error") else "ok",
                data.get("scan_resolution_state") or meta.get("scan_resolution_state"),
                data.get("final_render_allowed") if "final_render_allowed" in data else meta.get("final_render_allowed"),
                data.get("final_render_reason") or meta.get("final_render_reason"),
            )
        # if service returns {"error": "..."} keep it as JSON with 404-ish semantics
        if isinstance(data, dict) and data.get("error"):
            log_event(
                logger,
                "scan_failed",
                source="barcode",
                key=str(key or "").strip(),
                lang=lang,
                error_code=str(data.get("error_code") or ""),
                status_code=_error_status(data),
            )
            return JSONResponse(status_code=_error_status(data), content=data)
        if isinstance(data, dict):
            log_event(
                logger,
                "scan_completed",
                source="barcode",
                key=str(key or "").strip(),
                lang=lang,
                analysis_state=str(data.get("analysis_state") or ""),
                analysis_confidence=str(data.get("analysis_confidence") or ""),
                route_total_ms=int(round((time.perf_counter() - started_at) * 1000.0)),
            )
        return data
    except Exception as e:
        # This prints full traceback in Render logs
        logger.exception("Scan failed for key=%s", key)
        log_event(
            logger,
            "scan_failed",
            source="barcode",
            key=str(key or "").strip(),
            lang=lang,
            error_code="ANALYSIS_UNAVAILABLE",
            status_code=422,
        )
        # Return a safe JSON error so frontend doesn't just show blank 500
        return JSONResponse(
            status_code=422,
            content={
                "error": "This product could not be analyzed.",
                "error_code": "ANALYSIS_UNAVAILABLE",
            },
        )


@router.post("/scan/manual")
async def scan_manual_endpoint(payload: dict = Body(default={}), lang: str = Query("en")):
    lang = lang if lang in {"el", "en", "de", "fr"} else "en"
    log_event(
        logger,
        "scan_started",
        source="manual",
        lang=lang,
        corrected_in_session=bool((payload or {}).get("corrected_in_session")),
    )
    try:
        data = await analyze_manual_product(payload or {}, lang=lang)
        if isinstance(data, dict) and data.get("error"):
            log_event(
                logger,
                "scan_failed",
                source="manual",
                lang=lang,
                error_code=str(data.get("error_code") or ""),
                status_code=_error_status(data),
                corrected_in_session=bool((payload or {}).get("corrected_in_session")),
            )
            return JSONResponse(status_code=_error_status(data), content=data)
        if isinstance(data, dict):
            log_event(
                logger,
                "scan_completed",
                source="manual",
                lang=lang,
                analysis_state=str(data.get("analysis_state") or ""),
                analysis_confidence=str(data.get("analysis_confidence") or ""),
                corrected_in_session=bool(data.get("corrected_in_session")),
            )
        return data
    except Exception:
        logger.exception("Manual scan failed")
        log_event(
            logger,
            "scan_failed",
            source="manual",
            lang=lang,
            error_code="ANALYSIS_UNAVAILABLE",
            status_code=422,
            corrected_in_session=bool((payload or {}).get("corrected_in_session")),
        )
        return JSONResponse(
            status_code=422,
            content={
                "error": "This product could not be analyzed.",
                "error_code": "ANALYSIS_UNAVAILABLE",
            },
        )


@router.post("/scan/photo")
async def scan_photo_endpoint(payload: dict = Body(default={}), lang: str = Query("en")):
    lang = lang if lang in {"el", "en", "de", "fr"} else "en"
    log_event(logger, "scan_started", source="photo", lang=lang)
    try:
        payload_summary = _photo_payload_summary(payload or {})
        payload_summary["lang"] = lang
        data = await analyze_photo_product(payload or {}, lang=lang)
        if isinstance(data, dict) and data.get("error"):
            status = _error_status(data)
            log_event(
                logger,
                "scan_failed",
                source="photo",
                lang=lang,
                error_code=str(data.get("error_code") or data.get("error", {}).get("code") or ""),
                status_code=status,
            )
            logger.warning(
                "photo route error status=%s error_code=%s error=%s lookup_state=%s analysis_state=%s lookup_missing_fields=%s payload=%s photo_debug=%s",
                status,
                data.get("error_code") or data.get("error", {}).get("code"),
                data.get("error") if not isinstance(data.get("error"), dict) else data.get("error", {}).get("message"),
                data.get("lookup_state"),
                data.get("analysis_state"),
                data.get("lookup_missing_fields"),
                payload_summary,
                data.get("photo_extraction_debug") or data.get("photo_extraction", {}).get("debug"),
            )
            return JSONResponse(status_code=status, content=data)
        if isinstance(data, dict):
            log_event(
                logger,
                "scan_completed",
                source="photo",
                lang=lang,
                analysis_state=str(data.get("analysis_state") or ""),
                analysis_confidence=str(data.get("analysis_confidence") or ""),
                ocr_confidence=str(((data.get("photo_extraction") or {}).get("confidence")) or ""),
            )
        return data
    except Exception:
        logger.exception("Photo scan failed payload=%s", _photo_payload_summary(payload or {}))
        log_event(
            logger,
            "scan_failed",
            source="photo",
            lang=lang,
            error_code="PHOTO_EXTRACTION_FAILED",
            status_code=503,
        )
        return JSONResponse(
            status_code=503,
            content={
                "error": "This product could not be analyzed.",
                "error_code": "PHOTO_EXTRACTION_FAILED",
            },
        )


@router.post("/feedback/correction")
async def correction_feedback_endpoint(payload: dict = Body(default={}), lang: str = Query("en")):
    lang = lang if lang in {"el", "en", "de", "fr"} else "en"
    try:
        data = submit_correction_feedback(payload or {}, lang=lang)
        if isinstance(data, dict) and not data.get("ok"):
            log_event(
                logger,
                "feedback_submission_failed",
                lang=lang,
                error_code=str(data.get("error_code") or ""),
                status_code=int(data.get("status_code") or 422),
            )
            return JSONResponse(status_code=int(data.get("status_code") or 422), content=data)
        log_event(logger, "feedback_submission_completed", lang=lang)
        return data
    except Exception:
        logger.exception("Correction feedback submission failed")
        log_event(logger, "feedback_submission_failed", lang=lang, error_code="FEEDBACK_UNAVAILABLE", status_code=503)
        return JSONResponse(
            status_code=503,
            content={
                "ok": False,
                "error": "Correction feedback could not be submitted.",
                "error_code": "FEEDBACK_UNAVAILABLE",
            },
        )


@router.get("/internal/beta/feedback-summary")
async def internal_beta_feedback_summary_endpoint(
    x_beta_review_token: str | None = Header(default=None),
):
    authorized, configured = _beta_review_token_valid(x_beta_review_token)
    if not configured:
        return JSONResponse(status_code=404, content={"detail": "Not found"})
    if not authorized:
        return JSONResponse(status_code=403, content={"detail": "Forbidden"})
    return get_internal_beta_review_summary()
