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


# ---------
# Beverage inference (robust)
# ---------

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

    if re.search(r"\b(\d+)\s*[x×]\s*(\d+(?:\.\d+)?)\s*(ml|cl|l)\b", quantity) or re.search(
        r"\b(\d+(?:\.\d+)?)\s*(ml|cl|l)\b", quantity
    ):
        return True, True, "quantity_ml_l"

    # 4) Heuristic: name/brand hints (weak)
    name = str(
        off_product.get("product_name_en")
        or off_product.get("product_name")
        or off_product.get("generic_name_en")
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


# -------------------------
# Ingredients language preference
# -------------------------

def _best_ingredients_text(off_product: Dict[str, Any]) -> str:
    """
    Prefer English ingredients text when available.
    """
    candidates = [
        off_product.get("ingredients_text_en"),
        off_product.get("ingredients_text"),
        off_product.get("ingredients_text_fr"),
        off_product.get("ingredients_text_de"),
        off_product.get("ingredients_text_es"),
        off_product.get("ingredients_text_it"),
        off_product.get("ingredients_text_pl"),
    ]
    for c in candidates:
        s = str(c or "").strip()
        if s:
            return s
    return ""


def _ingredient_name_from_obj(ing: Dict[str, Any]) -> str:
    """
    Prefer English ingredient label if present.
    OFF ingredient objects sometimes include: text, text_en, id, etc.
    """
    if not isinstance(ing, dict):
        return str(ing or "").strip()

    for k in ("text_en", "text", "id"):
        v = ing.get(k)
        s = str(v or "").strip()
        if s:
            return s
    return ""


def _parse_ingredients_as_objects(off_product: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []

    ingredients = off_product.get("ingredients")
    if isinstance(ingredients, list) and ingredients:
        for ing in ingredients:
            name = _ingredient_name_from_obj(ing if isinstance(ing, dict) else {"text": str(ing)})
            name = str(name).strip()
            if name:
                out.append({"name": name, "class": "U", "note": "From OpenFoodFacts"})
        return out

    # Fallback to ingredients text, preferring English
    txt = _best_ingredients_text(off_product)
    if not txt:
        return []

    parts = re.split(r"[;,]", txt)
    for p in parts:
        p = p.strip()
        if p:
            out.append({"name": p, "class": "U", "note": "From OpenFoodFacts"})
    return out


def normalize_openfoodfacts(off_payload: Dict[str, Any], barcode: Optional[str] = None) -> Dict[str, Any]:
    """
    Input: OFF payload (usually {"status":1, "product": {...}})
    Output: normalized dict + flags:
      - is_beverage
      - is_beverage_inferred
      - serving_size_inferred
    """
    if not isinstance(off_payload, dict):
        off_payload = {}
    off_product = off_payload.get("product") if isinstance(off_payload.get("product"), dict) else off_payload
    if not isinstance(off_product, dict):
        off_product = {}
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
        serving = 330.0 if is_beverage else 100.0
        serving_size_inferred = True

    # Prefer English product name if available
    name = (
        off_product.get("product_name_en")
        or off_product.get("product_name")
        or off_product.get("generic_name_en")
        or off_product.get("generic_name")
        or "Unknown Product"
    )

    # Categories: keep both human categories and tags if present
    categories_raw = off_product.get("categories") or ""
    if isinstance(categories_raw, str):
        categories = [part.strip() for part in categories_raw.split(",") if part and part.strip()]
    elif isinstance(categories_raw, list):
        categories = [str(part).strip() for part in categories_raw if str(part).strip()]
    else:
        categories = []
    categories_tags = off_product.get("categories_tags") or []

    # Additives tags (useful for E-numbers)
    additives_tags = off_product.get("additives_tags") or []
    additives_original_tags = off_product.get("additives_original_tags") or []

    return {
        "off_code": barcode or off_product.get("code"),
        "name": str(name),
        "brand": off_product.get("brands") or off_product.get("brand_owner"),
        "image_url": off_product.get("image_url") or off_product.get("image_front_url"),
        "quantity": off_product.get("quantity"),
        "categories": categories,
        "categories_tags": categories_tags,
        "additives_tags": additives_tags,
        "additives_original_tags": additives_original_tags,
        "is_beverage": bool(is_beverage),
        "is_beverage_inferred": bool(is_bev_inferred),
        "beverage_inference_reason": bev_reason,
        "serving_size_inferred": bool(serving_size_inferred),
        "ingredients": _parse_ingredients_as_objects(off_product),
        "nutrition_per_100": {
            "unit": unit,
            "sugar_g": _clamp_nonneg(sugar),
            "salt_g": _clamp_nonneg(salt),
            "sat_fat_g": _clamp_nonneg(sat_fat),
            "protein_g": _clamp_nonneg(protein),
            "serving_size": float(serving),
        },
    }


# -------------------------
# Backward-compatible alias expected by scanner_service imports
# -------------------------
def normalize_product(raw: Dict[str, Any], source: Optional[str] = None) -> Dict[str, Any]:
    """
    Compatibility wrapper.
    - For OpenFoodFacts payloads -> normalize_openfoodfacts
    - For local payloads (already close to normalized) -> return as-is best-effort
    """
    if source == "openfoodfacts":
        try:
            barcode = str(raw.get("code") or raw.get("barcode") or raw.get("off_code") or "") or None
        except Exception:
            barcode = None
        return normalize_openfoodfacts(raw, barcode=barcode)

    # If it's clearly an OFF-like payload even without source
    if isinstance(raw, dict) and ("product" in raw or "nutriments" in raw):
        try:
            barcode = str(raw.get("code") or "") or None
        except Exception:
            barcode = None
        try:
            return normalize_openfoodfacts(raw, barcode=barcode)
        except Exception:
            pass

    # fallback: return raw as-is (for local DB)
    return raw
