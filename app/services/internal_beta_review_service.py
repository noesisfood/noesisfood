from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from app.services.correction_feedback_service import FEEDBACK_STORE_PATH
from app.services.monitoring_service import get_beta_monitoring_summary


NUTRITION_FIELDS = (
    "energy_kcal",
    "fat_g",
    "carb_g",
    "sugar_g",
    "salt_g",
    "sat_fat_g",
    "protein_g",
    "serving_size",
)


def _to_float(value: Any) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(str(value).strip().replace(",", "."))
    except Exception:
        return None


def _normalize_nutrition(payload: Any) -> Dict[str, Any]:
    source = payload if isinstance(payload, dict) else {}
    return {
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


def _changed_fields(record: Dict[str, Any]) -> List[str]:
    explicit = record.get("changed_fields")
    if isinstance(explicit, list) and explicit:
        return [str(item) for item in explicit if str(item).strip()]
    original = _normalize_nutrition(record.get("original_nutrition_per_100"))
    corrected = _normalize_nutrition(record.get("corrected_nutrition_per_100"))
    changed: List[str] = []
    for field in ("unit",) + NUTRITION_FIELDS:
        left = original.get(field)
        right = corrected.get(field)
        if left is None and right is None:
            continue
        if left is None or right is None:
            changed.append(field)
            continue
        if field == "unit":
            if str(left) != str(right):
                changed.append(field)
            continue
        if abs(float(left) - float(right)) > 1e-9:
            changed.append(field)
    return changed


def _parse_timestamp(value: Any) -> datetime:
    raw = str(value or "").strip()
    if not raw:
        return datetime.min
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except Exception:
        return datetime.min


def _product_summary(product: Dict[str, Any]) -> Dict[str, Optional[str]]:
    barcode = str(product.get("barcode") or "").strip() or None
    key = str(product.get("key") or "").strip() or None
    name = str(product.get("name") or "").strip() or None
    brand = str(product.get("brand") or "").strip() or None
    identity = barcode or key or name
    return {
        "product_key": identity,
        "barcode": barcode,
        "key": key,
        "name": name,
        "brand": brand,
    }


def _noise_flags(record: Dict[str, Any], changed_fields: List[str]) -> List[str]:
    product = record.get("product") if isinstance(record.get("product"), dict) else {}
    flags: List[str] = []
    if not bool(record.get("corrected_in_session")):
        flags.append("not_corrected_session")
    if not changed_fields:
        flags.append("no_changed_fields")
    if not any(str(product.get(field) or "").strip() for field in ("barcode", "key", "name")):
        flags.append("missing_product_identifier")
    return flags


def _load_feedback_records(store_path: Path) -> Tuple[List[Dict[str, Any]], int]:
    records: List[Dict[str, Any]] = []
    invalid_count = 0
    if not store_path.exists():
        return records, invalid_count
    with store_path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line:
                continue
            try:
                item = json.loads(line)
            except Exception:
                invalid_count += 1
                continue
            if not isinstance(item, dict):
                invalid_count += 1
                continue
            records.append(item)
    return records, invalid_count


def get_feedback_summary(store_path: Optional[Path] = None, recent_limit: int = 10) -> Dict[str, Any]:
    target = store_path or FEEDBACK_STORE_PATH
    records, invalid_count = _load_feedback_records(target)
    by_product: Counter[str] = Counter()
    field_counter: Counter[str] = Counter()
    product_meta: Dict[str, Dict[str, Optional[str]]] = {}
    recent_feedback: List[Dict[str, Any]] = []
    noisy_count = 0

    sorted_records = sorted(records, key=lambda item: _parse_timestamp(item.get("timestamp")), reverse=True)

    for record in sorted_records:
        product = record.get("product") if isinstance(record.get("product"), dict) else {}
        product_info = _product_summary(product)
        product_key = str(product_info.get("product_key") or "")
        changed_fields = _changed_fields(record)
        noise_flags = _noise_flags(record, changed_fields)
        if noise_flags:
            noisy_count += 1
        if product_key:
            by_product[product_key] += 1
            product_meta[product_key] = product_info
        for field in changed_fields:
            field_counter[field] += 1
        if len(recent_feedback) < max(1, int(recent_limit)):
            recent_feedback.append(
                {
                    "timestamp": str(record.get("timestamp") or ""),
                    "product_key": product_info.get("product_key"),
                    "barcode": product_info.get("barcode"),
                    "name": product_info.get("name"),
                    "brand": product_info.get("brand"),
                    "source_type": str(record.get("source_type") or "").strip().lower() or "manual",
                    "changed_fields": changed_fields,
                    "analysis_confidence": str(record.get("analysis_confidence") or "").strip().lower() or "low",
                }
            )

    submissions_by_product = [
        {**product_meta.get(product_key, {"product_key": product_key}), "count": count}
        for product_key, count in by_product.most_common()
    ]
    repeated_product_reports = [item for item in submissions_by_product if int(item.get("count") or 0) > 1]
    most_frequently_corrected_fields = [
        {"field": field, "count": count}
        for field, count in field_counter.most_common()
    ]
    return {
        "total_feedback_submissions": len(records),
        "invalid_or_noisy_record_count": int(invalid_count + noisy_count),
        "submissions_by_product": submissions_by_product,
        "most_frequently_corrected_fields": most_frequently_corrected_fields,
        "repeated_product_reports": repeated_product_reports,
        "recent_feedback": recent_feedback,
    }


def get_internal_beta_review_summary(store_path: Optional[Path] = None, recent_limit: int = 10) -> Dict[str, Any]:
    return {
        "feedback": get_feedback_summary(store_path=store_path, recent_limit=recent_limit),
        "monitoring": get_beta_monitoring_summary(),
    }
