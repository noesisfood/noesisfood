#!/usr/bin/env python3
import argparse
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


DEFAULT_FEEDBACK_PATH = Path("data") / "correction_feedback" / "correction_feedback.jsonl"
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


def _to_float(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, bool):
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


def _product_label(product: Dict[str, Any]) -> str:
    barcode = str(product.get("barcode") or "").strip()
    key = str(product.get("key") or "").strip()
    name = str(product.get("name") or "").strip()
    brand = str(product.get("brand") or "").strip()
    identity = barcode or key or name or "unknown-product"
    if name and name != identity:
        return f"{identity} ({name})"
    if brand and not name:
        return f"{identity} ({brand})"
    return identity


def _changed_fields(original: Dict[str, Any], corrected: Dict[str, Any], record: Dict[str, Any]) -> List[str]:
    explicit = record.get("changed_fields")
    if isinstance(explicit, list) and explicit:
        return [str(item) for item in explicit]
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


def _detect_noise(record: Dict[str, Any], changed_fields: List[str]) -> List[str]:
    issues: List[str] = []
    product = record.get("product") if isinstance(record.get("product"), dict) else {}
    if not any(str(product.get(key) or "").strip() for key in ("barcode", "key", "name")):
        issues.append("missing_product_identifier")
    if not bool(record.get("corrected_in_session")):
        issues.append("not_corrected_session")
    if not changed_fields:
        issues.append("no_changed_fields")
    if changed_fields == ["unit"]:
        issues.append("unit_only_change")
    return issues


def load_feedback_records(path: Path) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    records: List[Dict[str, Any]] = []
    invalid: List[Dict[str, Any]] = []
    if not path.exists():
        return records, invalid
    with path.open("r", encoding="utf-8") as handle:
        for line_no, raw_line in enumerate(handle, start=1):
            line = raw_line.strip()
            if not line:
                continue
            try:
                item = json.loads(line)
            except Exception:
                invalid.append({"line": line_no, "reason": "invalid_json", "raw": line[:200]})
                continue
            if not isinstance(item, dict):
                invalid.append({"line": line_no, "reason": "not_object"})
                continue
            records.append(item)
    return records, invalid


def summarize_feedback(records: Iterable[Dict[str, Any]], invalid_records: Iterable[Dict[str, Any]] | None = None) -> Dict[str, Any]:
    record_list = list(records)
    invalid_list = list(invalid_records or [])
    by_product: Counter[str] = Counter()
    field_counter: Counter[str] = Counter()
    before_after: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    repeated_reports: List[Dict[str, Any]] = []
    noisy_records: List[Dict[str, Any]] = list(invalid_list)
    valid_records = 0

    for idx, record in enumerate(record_list, start=1):
        product = record.get("product") if isinstance(record.get("product"), dict) else {}
        original = _normalize_nutrition(record.get("original_nutrition_per_100"))
        corrected = _normalize_nutrition(record.get("corrected_nutrition_per_100"))
        changed_fields = _changed_fields(original, corrected, record)
        issues = _detect_noise(record, changed_fields)
        if issues:
            noisy_records.append({"line": idx, "reason": ",".join(issues), "product": _product_label(product)})
        valid_records += 1
        label = _product_label(product)
        by_product[label] += 1
        for field in changed_fields:
            field_counter[field] += 1
            before_after[field].append(
                {
                    "product": label,
                    "before": original.get(field),
                    "after": corrected.get(field),
                    "timestamp": record.get("timestamp"),
                }
            )

    for product, count in by_product.most_common():
        if count > 1:
            repeated_reports.append({"product": product, "count": count})

    return {
        "total_feedback_submissions": len(record_list),
        "valid_records": valid_records,
        "invalid_or_noisy_records": noisy_records,
        "submissions_by_product": dict(by_product.most_common()),
        "most_frequently_corrected_fields": [{"field": field, "count": count} for field, count in field_counter.most_common()],
        "before_after_values": dict(before_after),
        "repeated_product_reports": repeated_reports,
    }


def format_summary(summary: Dict[str, Any], limit: int = 5) -> str:
    lines: List[str] = []
    lines.append("Beta Feedback Review")
    lines.append(f"Total feedback submissions: {summary['total_feedback_submissions']}")
    lines.append(f"Valid records: {summary['valid_records']}")
    lines.append(f"Invalid/noisy records: {len(summary['invalid_or_noisy_records'])}")
    lines.append("")
    lines.append("Submissions by barcode/key/product:")
    submissions = summary["submissions_by_product"]
    if submissions:
        for product, count in list(submissions.items())[:limit]:
            lines.append(f"- {product}: {count}")
    else:
        lines.append("- none")
    lines.append("")
    lines.append("Most frequently corrected fields:")
    field_rows = summary["most_frequently_corrected_fields"]
    if field_rows:
        for row in field_rows[:limit]:
            lines.append(f"- {row['field']}: {row['count']}")
    else:
        lines.append("- none")
    lines.append("")
    lines.append("Repeated product reports:")
    repeated = summary["repeated_product_reports"]
    if repeated:
        for row in repeated[:limit]:
            lines.append(f"- {row['product']}: {row['count']}")
    else:
        lines.append("- none")
    lines.append("")
    lines.append("Before/after examples:")
    before_after = summary["before_after_values"]
    if before_after:
        for field in list(before_after.keys())[:limit]:
            sample = before_after[field][: min(limit, len(before_after[field]))]
            rendered = "; ".join(f"{item['product']} {item['before']} -> {item['after']}" for item in sample)
            lines.append(f"- {field}: {rendered}")
    else:
        lines.append("- none")
    lines.append("")
    lines.append("Invalid/noisy records:")
    noisy = summary["invalid_or_noisy_records"]
    if noisy:
        for row in noisy[:limit]:
            detail = row.get("product") or row.get("raw") or ""
            lines.append(f"- line {row.get('line')}: {row.get('reason')} {detail}".rstrip())
    else:
        lines.append("- none")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Summarize correction feedback for daily beta review.")
    parser.add_argument("--path", default=str(DEFAULT_FEEDBACK_PATH), help="Path to correction feedback JSONL file.")
    parser.add_argument("--limit", type=int, default=5, help="Number of rows to show in each section.")
    parser.add_argument("--json", action="store_true", help="Output the summary as JSON.")
    args = parser.parse_args()

    path = Path(args.path)
    records, invalid = load_feedback_records(path)
    summary = summarize_feedback(records, invalid)
    if args.json:
        print(json.dumps(summary, ensure_ascii=False, indent=2))
    else:
        print(format_summary(summary, limit=max(1, args.limit)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
