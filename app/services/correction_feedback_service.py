import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


SUPPORTED_LANGS = {"el", "en", "de", "fr"}
ALLOWED_SOURCE_TYPES = {"barcode", "photo", "manual"}
NUTRITION_FIELDS = (
    "energy_kcal",
    "fat_g",
    "carb_g",
    "sugar_g",
    "salt_g",
    "sat_fat_g",
    "protein_g",
    "serving_size",
    "unit",
)
MAX_TEXT_LEN = 200
MAX_REASONS = 5
MAX_REASON_LEN = 180
MAX_PAYLOAD_BYTES = 12000
DUPLICATE_WINDOW_MINUTES = 10
FEEDBACK_STORE_PATH = Path(__file__).resolve().parents[2] / "data" / "correction_feedback" / "correction_feedback.jsonl"

VALUE_LIMITS = {
    "energy_kcal": (0.0, 900.0),
    "fat_g": (0.0, 100.0),
    "carb_g": (0.0, 100.0),
    "sugar_g": (0.0, 100.0),
    "salt_g": (0.0, 25.0),
    "sat_fat_g": (0.0, 100.0),
    "protein_g": (0.0, 100.0),
    "serving_size": (0.0, 5000.0),
}


def _trim_text(value: Any, limit: int = MAX_TEXT_LEN) -> Optional[str]:
    text = str(value or "").strip()
    if not text:
        return None
    if len(text) > limit:
        text = text[:limit].strip()
    return text or None


def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text.replace(",", "."))
    except Exception:
        return None


def _normalize_nutrition(payload: Any) -> Dict[str, Any]:
    source = payload if isinstance(payload, dict) else {}
    normalized: Dict[str, Any] = {
        "unit": str(source.get("unit") or "g").strip().lower() or "g",
        "energy_kcal": _to_float(source.get("energy_kcal")),
        "fat_g": _to_float(source.get("fat_g")),
        "carb_g": _to_float(source.get("carb_g") if source.get("carb_g") is not None else source.get("carbs_g")),
        "sugar_g": _to_float(source.get("sugar_g")),
        "salt_g": _to_float(source.get("salt_g")),
        "sat_fat_g": _to_float(source.get("sat_fat_g") if source.get("sat_fat_g") is not None else source.get("saturated_fat_g")),
        "protein_g": _to_float(source.get("protein_g")),
        "serving_size": _to_float(source.get("serving_size")),
    }
    normalized["unit"] = "ml" if normalized["unit"] == "ml" else "g"
    return normalized


def _normalize_confidence_reasons(value: Any) -> List[str]:
    items = value if isinstance(value, list) else []
    output: List[str] = []
    for item in items:
        text = _trim_text(item, MAX_REASON_LEN)
        if not text or text in output:
            continue
        output.append(text)
        if len(output) >= MAX_REASONS:
            break
    return output


def _meaningful_changes(original: Dict[str, Any], corrected: Dict[str, Any]) -> List[str]:
    changed: List[str] = []
    for field in NUTRITION_FIELDS:
        if field == "unit":
            if str(original.get(field) or "g") != str(corrected.get(field) or "g"):
                changed.append(field)
            continue
        left = original.get(field)
        right = corrected.get(field)
        if left is None and right is None:
            continue
        if left is None or right is None:
            changed.append(field)
            continue
        if abs(float(left) - float(right)) > 1e-9:
            changed.append(field)
    return changed


def _validate_nutrition_ranges(nutrition: Dict[str, Any]) -> Optional[str]:
    for field, bounds in VALUE_LIMITS.items():
        value = nutrition.get(field)
        if value is None:
            continue
        low, high = bounds
        if value < low or value > high:
            return f"Unrealistic nutrition value for {field}."
    return None


def _canonical_hash(product: Dict[str, Any], corrected: Dict[str, Any]) -> str:
    digest_input = {
        "barcode": product.get("barcode"),
        "key": product.get("key"),
        "name": product.get("name"),
        "corrected": corrected,
    }
    payload = json.dumps(digest_input, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _recent_duplicate_exists(store_path: Path, submission_hash: str, submitted_at: datetime) -> bool:
    if not store_path.exists():
        return False
    cutoff = submitted_at - timedelta(minutes=DUPLICATE_WINDOW_MINUTES)
    try:
        with store_path.open("r", encoding="utf-8") as handle:
            lines = handle.readlines()[-200:]
    except Exception:
        return False
    for line in reversed(lines):
        line = line.strip()
        if not line:
            continue
        try:
            item = json.loads(line)
        except Exception:
            continue
        if str(item.get("submission_hash") or "") != submission_hash:
            continue
        try:
            created_at = datetime.fromisoformat(str(item.get("timestamp")).replace("Z", "+00:00"))
        except Exception:
            created_at = None
        if created_at and created_at >= cutoff:
            return True
    return False


def _ok(message: str, status_code: int = 200, **extra: Any) -> Dict[str, Any]:
    response = {"ok": True, "message": message, "status_code": status_code}
    response.update(extra)
    return response


def _err(code: str, message: str, status_code: int = 422) -> Dict[str, Any]:
    return {"ok": False, "error_code": code, "error": message, "status_code": status_code}


def submit_correction_feedback(payload: Dict[str, Any], lang: str = "en", store_path: Optional[Path] = None) -> Dict[str, Any]:
    body = payload if isinstance(payload, dict) else {}
    lang = lang if lang in SUPPORTED_LANGS else "en"
    raw_json = json.dumps(body, ensure_ascii=False, separators=(",", ":"))
    if len(raw_json.encode("utf-8")) > MAX_PAYLOAD_BYTES:
        return _err("PAYLOAD_TOO_LARGE", "Feedback payload is too large.", 413)

    if not bool(body.get("corrected_in_session")):
        return _err("CORRECTION_REQUIRED", "Only corrected session results can be submitted as feedback.")

    product = body.get("product") if isinstance(body.get("product"), dict) else {}
    product_block = {
        "name": _trim_text(product.get("name") or body.get("name")),
        "brand": _trim_text(product.get("brand") or body.get("brand")),
        "barcode": _trim_text(product.get("barcode") or body.get("barcode"), 80),
        "key": _trim_text(product.get("key") or body.get("key"), 120),
    }
    if not any(product_block.values()):
        return _err("PRODUCT_ID_REQUIRED", "At least one product identifier is required.")

    corrected = _normalize_nutrition(body.get("corrected_nutrition_per_100"))
    original = _normalize_nutrition(body.get("original_nutrition_per_100"))
    changed_fields = _meaningful_changes(original, corrected)
    if not changed_fields:
        return _err("NO_CHANGED_FIELDS", "At least one nutrition value must be changed.")

    range_error = _validate_nutrition_ranges(corrected)
    if range_error:
        return _err("UNREALISTIC_VALUES", range_error)

    source_type = str(body.get("source_type") or "").strip().lower()
    if source_type not in ALLOWED_SOURCE_TYPES:
        source_type = "manual"

    confidence_reasons = _normalize_confidence_reasons(body.get("confidence_reasons"))
    analysis_confidence = str(body.get("analysis_confidence") or "").strip().lower()
    if analysis_confidence not in {"high", "medium", "low"}:
        analysis_confidence = "low"

    submitted_at = datetime.now(timezone.utc)
    timestamp = submitted_at.isoformat().replace("+00:00", "Z")
    record = {
        "product": product_block,
        "source_type": source_type,
        "lang": lang,
        "timestamp": timestamp,
        "original_nutrition_per_100": original,
        "corrected_nutrition_per_100": corrected,
        "analysis_confidence": analysis_confidence,
        "confidence_reasons": confidence_reasons,
        "corrected_in_session": True,
        "changed_fields": changed_fields,
    }
    submission_hash = _canonical_hash(product_block, corrected)
    record["submission_hash"] = submission_hash

    target = store_path or FEEDBACK_STORE_PATH
    if _recent_duplicate_exists(target, submission_hash, submitted_at):
        return _err("DUPLICATE_FEEDBACK", "A similar correction feedback was already submitted recently.", 409)

    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=False) + "\n")

    return _ok("Thank you for helping improve nutrition accuracy.", feedback_saved=True)
