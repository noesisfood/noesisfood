# app/services/product_normalizer.py
from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple


def _to_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).strip().replace(",", ".")
        if s == "":
            return None
        return float(s)
    except Exception:
        return None


def _clamp_nonneg(x: Optional[float]) -> float:
    if x is None:
        return 0.0
    return max(0.0, float(x))


def _get_nutriment(nutriments: Dict[str, Any], *keys: str) -> float:
    for k in keys:
        if k in nutriments:
            v = _to_float(nutriments.get(k))
            if v is not None:
                return _clamp_nonneg(v)
    return 0.0


def _parse_serving_size_to_g_or_ml(serving: Any) -> Optional[float]:
    """
    Returns numeric value in g/ml (no unit in return).
    Examples: "30 g"->30, "250ml"->250, "0.33 L"->330
    """
    if not serving:
        return None
    s = str(serving).lower().strip()

    m = re.search(r"(\d+(?:\.\d+)?)\s*(g|gr|gram|grams|ml|cl|l)\b", s)
    if not m:
        m = re.search(r"\((\d+(?:\.\d+)?)\s*(g|gr|ml|cl|l)\)", s)
    if not m:
        return None

    qty = _to_float(m.group(1))
    unit = m.group(2)
    if qty is None:
        return None

    if unit in ("g", "gr", "gram", "grams"):
        return float(qty)
    if unit == "ml":
        return float(qty)
    if unit == "cl":
        return float(qty) * 10.0
    if unit == "l":
        return float(qty) * 1000.0
    return None


def _parse_ingredients_as_objects(off_product: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []

    ingredients = off_product.get("ingredients")
    if isinstance(ingredients, list) and ingredients:
        for ing in ingredients:
            if isinstance(ing, dict):
                name = ing.get("text") or ing.get("id") or ""
            else:
                name = str(ing)
            name = str(name).strip()
            if name:
                out.append({"name": name, "class": "U", "note": "From OpenFoodFacts"})
        return out

    txt = off_product.get("ingredients_text") or off_product.get("ingredients_text_en") or ""
    txt = str(txt).strip()
    if not txt:
        return []

    parts = re.split(r"[;,]", txt)
    for p in parts:
        p = p.strip()
        if p:
            out.append({"name": p, "class": "U", "note": "From OpenFoodFacts"})
    return out


# ---------
# Beverage inference (robust)
# ---------

# Positive category signals (treat as strong when present)
_BEVERAGE_POSITIVE = (
    "en:beverages",
    "en:soft-drinks",
    "en:sodas",
    "en:carbonated-drinks",
    "en:carbonated-soft-drinks",
    "en:colas",
    "en:cola",
    "en:juices",
    "en:fruit-juices",
    "en:energy-drinks",
    "en:iced-teas",
    "en:waters",
    "en:flavoured-waters",
    "en:sports-drinks",
)

# Negative category signals (treat as strong when present)
_BEVERAGE_NEGATIVE = (
    "en:yogurts",
    "en:greek-yogurts",
    "en:dairy-desserts",
    "en:cheeses",
    "en:milk",
    "en:cream",
    "en:fermented-milk-products",
    "en:desserts",
    "en:soups",
    "en:sauces",
)

# Weak name hints (only used if categories did not decide)
_NAME_POSITIVE = (
    "cola",
    "coca-cola",
    "soft drink",
    "soda",
    "carbonated",
    "sparkling",
    "juice",
    "energy drink",
    "iced tea",
    "water",
)

_NAME_NEGATIVE = (
    "yogurt",
    "yoghurt",
    "joghurt",
    "cream",
    "cheese",
    "soup",
    "sauce",
    "dessert",
)


def _infer_is_beverage(off_product: Dict[str, Any]) -> Tuple[bool, bool, str]:
    """
    Returns: (is_beverage, is_inferred, reason)

    is_inferred = True means derived via heuristics (quantity/name/default).
    Strong signals (100ml and clear category tags) are NOT inferred.

    Important: OFF is inconsistent: beverages often come with nutrition_data_per="100g".
    So we DO NOT treat "100g" as a strong solid signal.
    """

    # 1) Strong signal: nutrition per 100ml
    per = str(off_product.get("nutrition_data_per") or "").lower()
    if "100ml" in per:
        return True, False, "nutrition_data_per=100ml"

    # 2) Strong signals: categories (prefer tags if present)
    cats = off_product.get("categories_tags") or []
    cats_l = [str(c).lower() for c in cats]

    if any(neg in cats_l for neg in _BEVERAGE_NEGATIVE):
        return False, False, "categories_negative"

    if any(pos in cats_l for pos in _BEVERAGE_POSITIVE):
        return True, False, "categories_positive"

    # 3) Heuristic: quantity looks like ml/cl/l
    quantity = str(off_product.get("quantity") or "").lower()

    # handle patterns like "6 x 330 ml", "330ml", "1.5 L", "50cl"
    if re.search(r"\b(\d+)\s*[x×]\s*(\d+(?:\.\d+)?)\s*(ml|cl|l)\b", quantity) or re.search(
        r"\b(\d+(?:\.\d+)?)\s*(ml|cl|l)\b", quantity
    ):
        return True, True, "quantity_ml_l"

    # 4) Heuristic: name/brand hints (weak)
    name = str(
        off_product.get("product_name")
        or off_product.get("product_name_en")
        or off_product.get("generic_name")
        or ""
    ).lower()
    brand = str(off_product.get("brands") or off_product.get("brand_owner") or "").lower()
    text = f"{name} {brand}".strip()

    if text:
        if any(n in text for n in _NAME_NEGATIVE):
            return False, True, "name_negative"
        if any(p in text for p in _NAME_POSITIVE):
            return True, True, "name_positive"

    # 5) Default: solid
    return False, True, "default_solid"


def normalize_openfoodfacts(off_payload: Dict[str, Any], barcode: Optional[str] = None) -> Dict[str, Any]:
    """
    Input: OFF payload (usually {"status":1, "product": {...}})
    Output: normalized dict + flags:
      - is_beverage
      - is_beverage_inferred
      - serving_size_inferred
    """
    off_product = off_payload.get("product") if isinstance(off_payload.get("product"), dict) else off_payload
    nutriments = off_product.get("nutriments") or {}

    sugar = _get_nutriment(nutriments, "sugars_100g", "sugar_100g", "sugars", "sugar")
    salt = _get_nutriment(nutriments, "salt_100g", "salt")
    sat_fat = _get_nutriment(
        nutriments,
        "saturated-fat_100g",
        "saturated_fat_100g",
        "saturated-fat",
        "saturated_fat",
    )
    protein = _get_nutriment(nutriments, "proteins_100g", "protein_100g", "proteins", "protein")

    is_beverage, is_bev_inferred, bev_reason = _infer_is_beverage(off_product)

    unit = "ml" if is_beverage else "g"

    # serving size parse
    serving = (
        _parse_serving_size_to_g_or_ml(off_product.get("serving_size"))
        or _parse_serving_size_to_g_or_ml(nutriments.get("serving_size"))
    )

    serving_size_inferred = False
    if serving is None:
        # sensible defaults
        serving = 330.0 if is_beverage else 100.0
        serving_size_inferred = True

    name = (
        off_product.get("product_name")
        or off_product.get("product_name_en")
        or off_product.get("generic_name")
        or "Unknown Product"
    )

    return {
        "off_code": barcode or off_product.get("code"),
        "name": str(name),
        "brand": off_product.get("brands") or off_product.get("brand_owner"),
        "image_url": off_product.get("image_url") or off_product.get("image_front_url"),
        "is_beverage": bool(is_beverage),
        "is_beverage_inferred": bool(is_bev_inferred),
        "beverage_inference_reason": bev_reason,
        "serving_size_inferred": bool(serving_size_inferred),
        "ingredients": _parse_ingredients_as_objects(off_product),
        "nutrition_per_100": {
            "unit": unit,
            "sugar_g": float(sugar),
            "salt_g": float(salt),
            "sat_fat_g": float(sat_fat),
            "protein_g": float(protein),
            "serving_size": float(serving),
        },
    }