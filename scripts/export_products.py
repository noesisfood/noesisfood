#!/usr/bin/env python3
import argparse
import csv
import json
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple


REQUIRED_COLUMNS = [
    "key",
    "barcode",
    "name",
    "brand",
    "serving_size_value",
    "serving_size_unit",
    "nutrients_energy_kcal_100",
    "nutrients_sugar_g_100",
    "nutrients_salt_g_100",
    "nutrients_saturated_fat_g_100",
    "ingredients_text",
    "ingredients_language",
    "ingredients_source_language",
    "additives",
    "review_note",
    "review_date",
    "review_confidence",
]

OPTIONAL_COLUMNS = [
    "nutrients_protein_g_100",
]

ALLOWED_SERVING_UNITS = {"g", "ml"}
E_NUMBER_RE = re.compile(r"^E\d{3}[A-Za-z]?$")
BARCODE_RE = re.compile(r"^\d{8,14}$")
LANG_RE = re.compile(r"^[a-z]{2}$")


class ValidationError(Exception):
    pass


def parse_float(value: str, field: str, row_idx: int, *, min_value=None, required=True) -> Optional[float]:
    raw = (value or "").strip()
    if raw == "":
        if required:
            raise ValidationError(f"row {row_idx}: '{field}' is required")
        return None
    try:
        number = float(raw)
    except ValueError as exc:
        raise ValidationError(f"row {row_idx}: '{field}' must be a number, got '{value}'") from exc
    if min_value is not None and number < min_value:
        raise ValidationError(f"row {row_idx}: '{field}' must be >= {min_value}, got {number}")
    return number


def parse_date(value: str, field: str, row_idx: int) -> str:
    raw = (value or "").strip()
    if not raw:
        raise ValidationError(f"row {row_idx}: '{field}' is required")
    try:
        datetime.strptime(raw, "%Y-%m-%d")
    except ValueError as exc:
        raise ValidationError(f"row {row_idx}: '{field}' must be YYYY-MM-DD, got '{value}'") from exc
    return raw


def parse_additives(value: str, row_idx: int) -> List[str]:
    raw = (value or "").strip()
    if not raw:
        return []
    parts = re.split(r"[;,]", raw)
    cleaned = []
    for part in parts:
        token = part.strip().upper()
        if not token:
            continue
        if not E_NUMBER_RE.match(token):
            raise ValidationError(f"row {row_idx}: additive '{token}' is not a valid E-number")
        cleaned.append(token)
    return sorted(set(cleaned))


def require_str(value: str, field: str, row_idx: int) -> str:
    raw = (value or "").strip()
    if not raw:
        raise ValidationError(f"row {row_idx}: '{field}' is required")
    return raw


def parse_lang(value: str, field: str, row_idx: int) -> str:
    raw = require_str(value, field, row_idx).lower()
    if not LANG_RE.match(raw):
        raise ValidationError(f"row {row_idx}: '{field}' must be a 2-letter ISO code, got '{value}'")
    return raw


def parse_row(row: Dict[str, str], row_idx: int) -> Dict[str, Any]:
    key = require_str(row.get("key"), "key", row_idx)

    barcode = require_str(row.get("barcode"), "barcode", row_idx)
    if not BARCODE_RE.match(barcode):
        raise ValidationError(f"row {row_idx}: 'barcode' must be 8-14 digits, got '{barcode}'")

    serving_unit = require_str(row.get("serving_size_unit"), "serving_size_unit", row_idx).lower()
    if serving_unit not in ALLOWED_SERVING_UNITS:
        raise ValidationError(
            f"row {row_idx}: 'serving_size_unit' must be one of {sorted(ALLOWED_SERVING_UNITS)}, got '{serving_unit}'"
        )

    review_conf = parse_float(
        row.get("review_confidence"),
        "review_confidence",
        row_idx,
        min_value=0.0,
        required=True,
    )
    if review_conf is None:
        raise ValidationError(f"row {row_idx}: 'review_confidence' is required")
    if review_conf > 1.0:
        raise ValidationError(f"row {row_idx}: 'review_confidence' must be <= 1.0, got {review_conf}")

    product: Dict[str, Any] = {
        "key": key,
        "barcode": barcode,
        "name": require_str(row.get("name"), "name", row_idx),
        "brand": require_str(row.get("brand"), "brand", row_idx),
        "serving_size": {
            "value": parse_float(
                row.get("serving_size_value"),
                "serving_size_value",
                row_idx,
                min_value=0.0,
                required=True,
            ),
            "unit": serving_unit,
        },
        "nutrients_per_100": {
            "energy_kcal": parse_float(
                row.get("nutrients_energy_kcal_100"),
                "nutrients_energy_kcal_100",
                row_idx,
                min_value=0.0,
                required=True,
            ),
            "sugar_g": parse_float(
                row.get("nutrients_sugar_g_100"),
                "nutrients_sugar_g_100",
                row_idx,
                min_value=0.0,
                required=True,
            ),
            "salt_g": parse_float(
                row.get("nutrients_salt_g_100"),
                "nutrients_salt_g_100",
                row_idx,
                min_value=0.0,
                required=True,
            ),
            "saturated_fat_g": parse_float(
                row.get("nutrients_saturated_fat_g_100"),
                "nutrients_saturated_fat_g_100",
                row_idx,
                min_value=0.0,
                required=True,
            ),
        },
        "ingredients": {
            "text": require_str(row.get("ingredients_text"), "ingredients_text", row_idx),
            "language": parse_lang(row.get("ingredients_language"), "ingredients_language", row_idx),
            "source_language": parse_lang(row.get("ingredients_source_language"), "ingredients_source_language", row_idx),
        },
        "additives": parse_additives(row.get("additives"), row_idx),
        "review": {
            "note": require_str(row.get("review_note"), "review_note", row_idx),
            "date": parse_date(row.get("review_date"), "review_date", row_idx),
            "confidence": float(review_conf),
        },
    }

    protein = parse_float(
        row.get("nutrients_protein_g_100"),
        "nutrients_protein_g_100",
        row_idx,
        min_value=0.0,
        required=False,
    )
    if protein is not None:
        product["nutrients_per_100"]["protein_g"] = protein

    return product


def validate_columns(fieldnames: Optional[List[str]]) -> Tuple[List[str], List[str]]:
    if fieldnames is None:
        raise ValidationError("CSV has no header row")

    fields = [f.strip() for f in fieldnames if f and f.strip()]
    required = set(REQUIRED_COLUMNS)
    optional = set(OPTIONAL_COLUMNS)
    allowed = required | optional

    missing = [c for c in REQUIRED_COLUMNS if c not in fields]
    unknown = [c for c in fields if c not in allowed]

    if missing or unknown:
        msg_parts = []
        if missing:
            msg_parts.append("missing required columns: " + ", ".join(missing))
        if unknown:
            msg_parts.append("unknown columns: " + ", ".join(unknown))
        raise ValidationError("CSV header invalid: " + " | ".join(msg_parts))

    return fields, [c for c in OPTIONAL_COLUMNS if c in fields]


def export(csv_path: Path, output_path: Path) -> None:
    if not csv_path.exists():
        raise ValidationError(f"CSV file not found: {csv_path}")

    errors: List[str] = []
    products: List[Dict[str, Any]] = []
    seen_keys = set()

    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        validate_columns(reader.fieldnames)

        for idx, row in enumerate(reader, start=2):  # header is line 1
            try:
                product = parse_row(row, idx)
                k = product["key"]
                if k in seen_keys:
                    raise ValidationError(f"row {idx}: duplicate key '{k}'")
                seen_keys.add(k)
                products.append(product)
            except ValidationError as exc:
                errors.append(str(exc))

    if errors:
        msg = "CSV validation failed:\n- " + "\n- ".join(errors)
        raise ValidationError(msg)

    products.sort(key=lambda p: p["key"])

    payload = {
        "schema_version": "v0",
        "products": products,
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="\n") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
        f.write("\n")


def main() -> int:
    parser = argparse.ArgumentParser(description="Export curated products JSON from Google Sheet CSV.")
    parser.add_argument("csv_path", type=Path, help="Path to CSV exported from Google Sheets")
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("app/data/products.json"),
        help="Output JSON path (default: app/data/products.json)",
    )
    args = parser.parse_args()

    try:
        export(args.csv_path, args.output)
    except ValidationError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    print(f"Wrote {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())