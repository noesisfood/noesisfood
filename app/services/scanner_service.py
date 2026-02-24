import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from app.services.openfoodfacts_service import fetch_off_product
from app.services.product_normalizer import normalize_openfoodfacts

APP_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = APP_DIR / "data"

PRODUCTS_FILE = DATA_DIR / "products.json"
RASFF_FILE = DATA_DIR / "rasff_alerts.json"

WHO_SUGAR_IDEAL = 25.0
WHO_SUGAR_UPPER = 50.0

SERVING_SUGAR_THRESHOLD_G = 12.5
SERVING_SUGAR_MULTIPLIER = 1.0
SERVING_SUGAR_PENALTY_CAP = 20

PROTEIN_BONUS_THRESHOLD_G = 3.0
PROTEIN_BONUS_MULTIPLIER = 1.0
PROTEIN_BONUS_CAP = 8


def _load_json(path: Path, default: Any) -> Any:
    try:
        if not path.exists():
            return default
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


products_db: Dict[str, Any] = _load_json(PRODUCTS_FILE, default={})
rasff_db: Dict[str, Any] = _load_json(RASFF_FILE, default={})


def _clamp_score(x: float) -> int:
    return max(0, min(100, int(round(x))))


def _build_data_quality(
    source: str,
    nutrition_per_100: dict,
    ingredients: list,
    serving_size_inferred: bool,
    is_beverage_inferred: bool,
    is_beverage: bool,
    beverage_reason: Optional[str],
) -> Dict[str, Any]:

    required_fields = ["sugar_g", "salt_g", "sat_fat_g"]
    present = []
    missing = []

    for f in required_fields:
        if nutrition_per_100.get(f) is not None:
            present.append(f)
        else:
            missing.append(f)

    nutrition_complete = len(missing) == 0
    ingredients_available = len(ingredients) > 0

    if source == "local":
        confidence = "high"
    elif nutrition_complete and ingredients_available:
        confidence = "high"
    elif nutrition_complete:
        confidence = "medium"
    else:
        confidence = "low"

    return {
        "source": source,
        "nutrition_fields_present": present,
        "missing_fields": missing,
        "nutrition_complete": nutrition_complete,
        "ingredients_available": ingredients_available,
        "serving_size_inferred": serving_size_inferred,
        "is_beverage": is_beverage,
        "is_beverage_inferred": is_beverage_inferred,
        "beverage_inference_reason": beverage_reason,
        "confidence": confidence,
    }


def _who_sugar_per_serving(nutrition_per_100: dict) -> float:
    sugar_per_100 = float(nutrition_per_100.get("sugar_g", 0) or 0)
    serving_size = float(nutrition_per_100.get("serving_size", 1.0) or 1.0)
    return (sugar_per_100 / 100.0) * serving_size


def _protein_bonus(nutrition_per_100: dict, is_beverage: bool) -> float:
    if is_beverage:
        return 0.0

    protein = float(nutrition_per_100.get("protein_g", 0) or 0)
    if protein <= PROTEIN_BONUS_THRESHOLD_G:
        return 0.0

    raw = (protein - PROTEIN_BONUS_THRESHOLD_G) * PROTEIN_BONUS_MULTIPLIER
    return min(float(PROTEIN_BONUS_CAP), max(0.0, raw))


def _v3_hybrid_score(nutrition_per_100: dict, is_beverage: bool) -> Dict[str, Any]:
    sugar = float(nutrition_per_100.get("sugar_g", 0) or 0)
    salt = float(nutrition_per_100.get("salt_g", 0) or 0)
    sat_fat = float(nutrition_per_100.get("sat_fat_g", 0) or 0)
    protein = float(nutrition_per_100.get("protein_g", 0) or 0)

    sugar_w = 6.0 if is_beverage else 5.0
    salt_w = 10.0
    satfat_w = 4.0

    penalty_sugar = sugar * sugar_w
    penalty_salt = salt * salt_w
    penalty_sat_fat = sat_fat * satfat_w

    sugar_per_serving = _who_sugar_per_serving(nutrition_per_100)

    raw_serving_penalty = 0.0
    if sugar_per_serving > SERVING_SUGAR_THRESHOLD_G:
        raw_serving_penalty = (
            (sugar_per_serving - SERVING_SUGAR_THRESHOLD_G)
            * SERVING_SUGAR_MULTIPLIER
        )

    penalty_serving_sugar = min(
        float(SERVING_SUGAR_PENALTY_CAP),
        max(0.0, raw_serving_penalty),
    )

    bonus_protein = _protein_bonus(nutrition_per_100, is_beverage)

    total_penalty = (
        penalty_sugar
        + penalty_salt
        + penalty_sat_fat
        + penalty_serving_sugar
    )

    final = _clamp_score(100 - total_penalty + bonus_protein)

    return {
        "score": final,
        "breakdown": {
            "version": "v3_hybrid_pro",
            "unit": nutrition_per_100.get("unit", "g"),
            "sugar_g_per_100": sugar,
            "salt_g_per_100": salt,
            "sat_fat_g_per_100": sat_fat,
            "protein_g_per_100": protein,
            "serving_size": float(nutrition_per_100.get("serving_size", 1.0) or 1.0),
            "sugar_per_serving_g": round(sugar_per_serving, 1),

            "penalty_sugar": int(round(penalty_sugar)),
            "penalty_salt": int(round(penalty_salt)),
            "penalty_sat_fat": int(round(penalty_sat_fat)),
            "penalty_serving_sugar": int(round(penalty_serving_sugar)),

            "bonus_protein": int(round(bonus_protein)),
            "total_penalty": int(round(total_penalty)),
            "final_score": final,

            "basis": "per_100g_or_100ml + per_serving_sugar + protein_bonus",
            "strict_profile": bool(is_beverage),

            "protein_bonus_rule": {
                "threshold_g": PROTEIN_BONUS_THRESHOLD_G,
                "multiplier": PROTEIN_BONUS_MULTIPLIER,
                "cap": PROTEIN_BONUS_CAP,
                "applies_to": "solids_only",
            },
            "serving_penalty_rule": {
                "threshold_g": SERVING_SUGAR_THRESHOLD_G,
                "multiplier": SERVING_SUGAR_MULTIPLIER,
                "cap": SERVING_SUGAR_PENALTY_CAP,
            },
        },
    }


def _who_sugar_impact(nutrition_per_100: dict) -> Dict[str, Any]:
    sugar_per_serving = _who_sugar_per_serving(nutrition_per_100)

    ideal_pct = (sugar_per_serving / WHO_SUGAR_IDEAL) * 100
    upper_pct = (sugar_per_serving / WHO_SUGAR_UPPER) * 100

    return {
        "sugar_per_serving_g": round(sugar_per_serving, 1),
        "ideal_limit_g": WHO_SUGAR_IDEAL,
        "upper_limit_g": WHO_SUGAR_UPPER,
        "percent_of_ideal": round(ideal_pct, 1),
        "percent_of_upper": round(upper_pct, 1),
        "exceeds_ideal": sugar_per_serving > WHO_SUGAR_IDEAL,
        "exceeds_upper": sugar_per_serving > WHO_SUGAR_UPPER,
    }


def _why_this_score(
    nutrition_per_100: dict,
    breakdown: dict,
    who: dict,
    quality: dict,
) -> Dict[str, Any]:
    """
    Consumer-friendly explanations.
    Returns { "why_this_score": [...], "tips": [...] }
    """
    unit = str(nutrition_per_100.get("unit", "g"))
    is_bev = bool(quality.get("is_beverage", False))

    sugar_100 = float(nutrition_per_100.get("sugar_g", 0) or 0)
    salt_100 = float(nutrition_per_100.get("salt_g", 0) or 0)
    sat_100 = float(nutrition_per_100.get("sat_fat_g", 0) or 0)
    protein_100 = float(nutrition_per_100.get("protein_g", 0) or 0)

    serving = float(nutrition_per_100.get("serving_size", 1.0) or 1.0)
    sugar_serv = float(who.get("sugar_per_serving_g", 0) or 0)

    bonus_pro = int(breakdown.get("bonus_protein", 0) or 0)
    pen_serv = int(breakdown.get("penalty_serving_sugar", 0) or 0)
    strict = bool(breakdown.get("strict_profile", False))

    why: List[str] = []
    tips: List[str] = []

    # Core nutrient bullets
    if is_bev:
        why.append(f"Είναι ρόφημα → εφαρμόζεται αυστηρότερο προφίλ για ζάχαρη (strict profile).")
    if strict and not is_bev:
        # just in case
        why.append("Ενεργοποιήθηκε strict profile.")

    why.append(f"Ζάχαρη ανά 100{unit}: {sugar_100:.1f} g")
    if sat_100 > 0:
        why.append(f"Κορεσμένα λιπαρά ανά 100{unit}: {sat_100:.1f} g")
    if salt_100 > 0:
        why.append(f"Αλάτι ανά 100{unit}: {salt_100:.2f} g")

    # Serving / WHO layer
    if serving > 1:
        why.append(f"Μερίδα: {serving:.0f}{unit} → ζάχαρη ανά μερίδα: {sugar_serv:.1f} g")
    else:
        # still show WHO if any sugar
        if sugar_serv > 0:
            why.append(f"Ζάχαρη ανά μερίδα: {sugar_serv:.1f} g")

    if pen_serv > 0:
        why.append(f"Extra ποινή μερίδας: +{pen_serv} (υψηλή ζάχαρη ανά μερίδα)")

    if bonus_pro > 0:
        why.append(f"Protein bonus: +{bonus_pro} (πρωτεΐνη {protein_100:.1f} g/100{unit})")

    # WHO guideline context
    pct_ideal = float(who.get("percent_of_ideal", 0) or 0)
    pct_upper = float(who.get("percent_of_upper", 0) or 0)
    if sugar_serv > 0:
        why.append(f"WHO sugar impact: {pct_ideal:.0f}% του ιδανικού (25g) / {pct_upper:.0f}% του upper (50g)")

    # Trust / data-quality hints
    if bool(quality.get("serving_size_inferred", False)):
        tips.append("Το μέγεθος μερίδας εκτιμήθηκε (δεν υπήρχε καθαρό serving size στο προϊόν).")

    reason = quality.get("beverage_inference_reason")
    if reason:
        tips.append(f"Beverage detection reason: {reason}")

    # Generic tips
    if is_bev and sugar_100 >= 5:
        tips.append("Tip: για ροφήματα, η ζάχαρη ανεβαίνει γρήγορα ανά μερίδα—έλεγξε και τις 'χωρίς ζάχαρη' επιλογές.")
    if (not is_bev) and sat_100 >= 5:
        tips.append("Tip: αν σε νοιάζει η καρδιαγγειακή υγεία, σύγκρινε και τα κορεσμένα λιπαρά ανά 100g.")

    return {"why_this_score": why, "tips": tips}


async def scan_product(product_id: str) -> dict:
    pid = (product_id or "").strip()

    rasff_alerts: List[str] = rasff_db.get(pid, []) or []
    product: Optional[dict] = products_db.get(pid)

    # LOCAL
    if product:
        nutrients = product.get("nutrients", {}) or {}
        is_beverage = bool(product.get("is_beverage", False))

        nutrition_per_100 = {
            "unit": "ml" if is_beverage else "g",
            "sugar_g": float(nutrients.get("sugar", 0) or 0),
            "salt_g": float(nutrients.get("salt", 0) or 0),
            "sat_fat_g": float(nutrients.get("fat", 0) or 0),
            "protein_g": float(nutrients.get("protein", 0) or 0),
            "serving_size": float(product.get("serving_size", 1.0) or 1.0),
        }

        v = _v3_hybrid_score(nutrition_per_100, is_beverage)
        who = _who_sugar_impact(nutrition_per_100)

        quality = _build_data_quality(
            source="local",
            nutrition_per_100=nutrition_per_100,
            ingredients=product.get("ingredients", []) or [],
            serving_size_inferred=False,
            is_beverage_inferred=False,
            is_beverage=is_beverage,
            beverage_reason="local_flag",
        )

        explain = _why_this_score(nutrition_per_100, v["breakdown"], who, quality)

        return {
            "source": "local",
            "matched_by": "local_id",
            "product_id": pid,
            "name": product.get("name", "Unknown Product"),
            "brand": product.get("brand"),
            "image_url": product.get("image_url"),
            "alerts": rasff_alerts,
            "ingredients": product.get("ingredients", []) or [],
            "nutrition_per_100": nutrition_per_100,
            "vitascore": v["score"],
            "vitascore_version": "v3_hybrid_pro",
            "vitascore_breakdown": v["breakdown"],
            "who_impact": who,
            "data_quality": quality,
            **explain,
        }

    # OFF
    off = await fetch_off_product(pid)
    if off.ok and off.payload:
        normalized = normalize_openfoodfacts(off.payload, barcode=pid)

        nutrition_per_100 = normalized.get("nutrition_per_100", {}) or {}
        ingredients = normalized.get("ingredients", []) or []
        is_beverage = bool(normalized.get("is_beverage", False))

        v = _v3_hybrid_score(nutrition_per_100, is_beverage)
        who = _who_sugar_impact(nutrition_per_100)

        quality = _build_data_quality(
            source="openfoodfacts",
            nutrition_per_100=nutrition_per_100,
            ingredients=ingredients,
            serving_size_inferred=bool(normalized.get("serving_size_inferred", False)),
            is_beverage_inferred=bool(normalized.get("is_beverage_inferred", False)),
            is_beverage=is_beverage,
            beverage_reason=normalized.get("beverage_inference_reason"),
        )

        explain = _why_this_score(nutrition_per_100, v["breakdown"], who, quality)

        return {
            "source": "openfoodfacts",
            "matched_by": "barcode_or_key",
            "off_code": normalized.get("off_code") or pid,
            "name": normalized.get("name", "Unknown Product"),
            "brand": normalized.get("brand"),
            "image_url": normalized.get("image_url"),
            "alerts": rasff_alerts,
            "ingredients": ingredients,
            "nutrition_per_100": nutrition_per_100,
            "vitascore": v["score"],
            "vitascore_version": "v3_hybrid_pro",
            "vitascore_breakdown": v["breakdown"],
            "who_impact": who,
            "data_quality": quality,
            **explain,
        }

    return {"error": "Product not found"}


def reload_data() -> None:
    global products_db, rasff_db
    products_db = _load_json(PRODUCTS_FILE, default={})
    rasff_db = _load_json(RASFF_FILE, default={})