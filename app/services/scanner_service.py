"""
NoesisFood - Scanner Service (WHO-first + v3_hybrid_pro)

+ Ingredients Intelligence v1
+ E-number explanations (E-codes + meaning/role)
+ NEW: detect E-numbers from OFF additives_tags (raw + normalized)
+ NEW: caffeine detection includes "kofeina" (PL)
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import httpx

from app.services.openfoodfacts_service import fetch_off_product
from app.services.product_normalizer import normalize_product


# -------------------------
# Paths / local data
# -------------------------
APP_DIR = Path(__file__).resolve().parent  # app/services/
DATA_DIR = APP_DIR.parent / "data"         # app/data/

PRODUCTS_FILE = DATA_DIR / "products.json"
RASFF_FILE = DATA_DIR / "rasff.json"


# -------------------------
# WHO reference points (baseline)
# -------------------------
WHO_SUGAR_IDEAL = 25.0
WHO_SUGAR_UPPER = 50.0
WHO_SALT_G_PER_DAY = 5.0
WHO_SATFAT_G_PER_DAY_PROXY = 20.0

SUPPORTED_LANGS = {"el", "en", "de", "fr"}
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_RESPONSES_URL = os.getenv("OPENAI_RESPONSES_URL", "https://api.openai.com/v1/responses").strip()
OPENAI_VISION_MODEL = os.getenv("OPENAI_VISION_MODEL", "gpt-4.1-mini").strip()

I18N_EXPLAIN: Dict[str, Dict[str, str]] = {
    "en": {
        "why_high_sugar_density": "High sugar density: {value}g per 100{unit} drives most of the penalty.",
        "why_moderate_sugar": "Moderate sugar: {value}g per 100{unit} contributes to the score.",
        "why_sat_fat_elevated": "Saturated fat is elevated: {value}g/100{unit} adds a strong penalty.",
        "why_salt_notable": "Salt is notable: {value}g/100{unit} increases the penalty.",
        "why_fiber_bonus": "Fiber helps offset penalties: {value}g/100{unit} adds a bonus.",
        "why_serving_spike": "Serving sugar spike: {value}g per serving triggers extra penalty (large single-serve impact).",
        "why_who_anchor": "WHO baseline (serving-level load) anchors the score: {score}/100.",
        "why_no_single_driver": "Score is driven by the available nutrition facts; no single extreme driver detected.",
        "tip_drinks_zero": "For drinks: try a 'no sugar' / 'zero' alternative or smaller serving size.",
        "tip_balance_day": "If you drink this daily, consider balancing with low-sugar choices elsewhere that day.",
        "intel_note_heuristic": "Ingredients Intelligence is heuristic and depends on ingredient text quality.",
        "intel_note_processing": "Processing score is an informational index, not an official NOVA classification.",
        "intel_flag_sweeteners": "Sweeteners present ({count})",
        "intel_flag_additives": "Additives / E-numbers detected ({count})",
        "intel_flag_preservatives": "Preservatives present ({count})",
        "intel_flag_emulsifiers": "Emulsifiers/Stabilizers present ({count})",
        "intel_flag_flavourings": "Flavourings present ({count})",
        "intel_flag_colorants": "Colorants present ({count})",
        "intel_flag_caffeine": "Contains caffeine",
        "intel_flag_allergens": "Allergens: {items}",
        "dq_note_confidence": "Confidence reflects completeness of nutrition facts + serving info.",
        "dq_note_educational": "Educational tool - not medical advice.",
        "dq_note_curated": "Beverage locked by curated layer.",
    },
    "el": {
        "why_high_sugar_density": "Υψηλή πυκνότητα ζάχαρης: {value}g ανά 100{unit} αυξάνει σημαντικά την ποινή.",
        "why_moderate_sugar": "Μέτρια ζάχαρη: {value}g ανά 100{unit} επηρεάζει το σκορ.",
        "why_sat_fat_elevated": "Αυξημένα κορεσμένα: {value}g/100{unit} προσθέτουν ισχυρή ποινή.",
        "why_salt_notable": "Το αλάτι είναι αυξημένο: {value}g/100{unit} αυξάνει την ποινή.",
        "why_fiber_bonus": "Οι φυτικές ίνες βοηθούν: {value}g/100{unit} προσθέτουν bonus.",
        "why_serving_spike": "Αιχμή ζάχαρης ανά μερίδα: {value}g αυξάνει την ποινή (μεγάλη επίδραση ανά μερίδα).",
        "why_who_anchor": "Η βάση WHO (ανά μερίδα) αγκυρώνει το σκορ: {score}/100.",
        "why_no_single_driver": "Το σκορ βασίζεται στα διαθέσιμα διατροφικά στοιχεία χωρίς έναν ακραίο παράγοντα.",
        "tip_drinks_zero": "Για ροφήματα: προτίμησε επιλογή χωρίς ζάχαρη ή μικρότερη μερίδα.",
        "tip_balance_day": "Αν το καταναλώνεις συχνά, ισορρόπησέ το με χαμηλή ζάχαρη μέσα στην ημέρα.",
        "intel_note_heuristic": "Το Ingredients Intelligence είναι ευρετικό και εξαρτάται από την ποιότητα του κειμένου συστατικών.",
        "intel_note_processing": "Το processing score είναι ενημερωτικός δείκτης, όχι επίσημη ταξινόμηση NOVA.",
        "intel_flag_sweeteners": "Παρουσία γλυκαντικών ({count})",
        "intel_flag_additives": "Εντοπίστηκαν πρόσθετα / E-numbers ({count})",
        "intel_flag_preservatives": "Παρουσία συντηρητικών ({count})",
        "intel_flag_emulsifiers": "Παρουσία γαλακτωματοποιητών/σταθεροποιητών ({count})",
        "intel_flag_flavourings": "Παρουσία αρωματικών ({count})",
        "intel_flag_colorants": "Παρουσία χρωστικών ({count})",
        "intel_flag_caffeine": "Περιέχει καφεΐνη",
        "intel_flag_allergens": "Αλλεργιογόνα: {items}",
        "dq_note_confidence": "Η εμπιστοσύνη βασίζεται στην πληρότητα διατροφικών στοιχείων και μερίδας.",
        "dq_note_educational": "Ενημερωτικό εργαλείο - όχι ιατρική συμβουλή.",
        "dq_note_curated": "Το ρόφημα κλειδώθηκε από το curated layer.",
    },
    "de": {
        "why_high_sugar_density": "Hohe Zuckerdichte: {value}g pro 100{unit} treibt den Malus stark.",
        "why_moderate_sugar": "Moderater Zucker: {value}g pro 100{unit} beeinflusst den Score.",
        "why_sat_fat_elevated": "Erhöhte gesättigte Fette: {value}g/100{unit} erzeugen einen starken Malus.",
        "why_salt_notable": "Auffälliges Salz: {value}g/100{unit} erhöht den Malus.",
        "why_fiber_bonus": "Ballaststoffe helfen: {value}g/100{unit} geben einen Bonus.",
        "why_serving_spike": "Zuckerspitze pro Portion: {value}g erhöht den Malus (starker Portionseffekt).",
        "why_who_anchor": "Die WHO-Basis (pro Portion) verankert den Score: {score}/100.",
        "why_no_single_driver": "Der Score basiert auf den verfügbaren Nährwerten ohne einen einzelnen Extremtreiber.",
        "tip_drinks_zero": "Für Getränke: zuckerfreie Option oder kleinere Portion wählen.",
        "tip_balance_day": "Bei täglichem Konsum mit zuckerärmeren Optionen im Tagesverlauf ausgleichen.",
        "intel_note_heuristic": "Ingredients Intelligence ist heuristisch und hängt von der Qualität des Zutaten-Textes ab.",
        "intel_note_processing": "Der Processing-Score ist ein Informationsindex, keine offizielle NOVA-Klassifikation.",
        "intel_flag_sweeteners": "Süßstoffe vorhanden ({count})",
        "intel_flag_additives": "Zusatzstoffe / E-Nummern erkannt ({count})",
        "intel_flag_preservatives": "Konservierungsstoffe vorhanden ({count})",
        "intel_flag_emulsifiers": "Emulgatoren/Stabilisatoren vorhanden ({count})",
        "intel_flag_flavourings": "Aromen vorhanden ({count})",
        "intel_flag_colorants": "Farbstoffe vorhanden ({count})",
        "intel_flag_caffeine": "Enthält Koffein",
        "intel_flag_allergens": "Allergene: {items}",
        "dq_note_confidence": "Die Konfidenz basiert auf der Vollständigkeit von Nährwerten und Portionsdaten.",
        "dq_note_educational": "Lernhilfe - keine medizinische Beratung.",
        "dq_note_curated": "Getränk durch Curated-Layer festgelegt.",
    },
    "fr": {
        "why_high_sugar_density": "Forte densité de sucre : {value}g pour 100{unit} augmente fortement la pénalité.",
        "why_moderate_sugar": "Sucre modéré : {value}g pour 100{unit} influence le score.",
        "why_sat_fat_elevated": "Graisses saturées élevées : {value}g/100{unit} ajoutent une forte pénalité.",
        "why_salt_notable": "Sel notable : {value}g/100{unit} augmente la pénalité.",
        "why_fiber_bonus": "Les fibres aident : {value}g/100{unit} ajoutent un bonus.",
        "why_serving_spike": "Pic de sucre par portion : {value}g augmente la pénalité (fort impact à la portion).",
        "why_who_anchor": "La base WHO (par portion) ancre le score : {score}/100.",
        "why_no_single_driver": "Le score repose sur les valeurs nutritionnelles disponibles sans facteur extrême unique.",
        "tip_drinks_zero": "Pour les boissons : choisir une option sans sucre ou une portion plus petite.",
        "tip_balance_day": "En consommation quotidienne, compenser avec des choix moins sucrés sur la journée.",
        "intel_note_heuristic": "Ingredients Intelligence est heuristique et dépend de la qualité du texte des ingrédients.",
        "intel_note_processing": "Le score de processing est un indice informatif, pas une classification NOVA officielle.",
        "intel_flag_sweeteners": "Édulcorants présents ({count})",
        "intel_flag_additives": "Additifs / E-numbers détectés ({count})",
        "intel_flag_preservatives": "Conservateurs présents ({count})",
        "intel_flag_emulsifiers": "Émulsifiants/Stabilisants présents ({count})",
        "intel_flag_flavourings": "Arômes présents ({count})",
        "intel_flag_colorants": "Colorants présents ({count})",
        "intel_flag_caffeine": "Contient de la caféine",
        "intel_flag_allergens": "Allergènes : {items}",
        "dq_note_confidence": "La confiance reflète la complétude des données nutritionnelles et de portion.",
        "dq_note_educational": "Outil éducatif - pas un avis médical.",
        "dq_note_curated": "Boisson verrouillée par la couche curated.",
    },
}


def t(lang: str, key: str, **kwargs: Any) -> str:
    lang_key = lang if lang in SUPPORTED_LANGS else "en"
    template = I18N_EXPLAIN.get(lang_key, {}).get(key) or I18N_EXPLAIN["en"].get(key) or key
    return template.format(**kwargs)


# -----------------------------
# Helpers
# -----------------------------
def _load_json(path: Path, default: Any) -> Any:
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        pass
    return default


def _to_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).strip()
        if not s:
            return None
        s = s.replace(",", ".")
        return float(s)
    except Exception:
        return None


def _as_list(x: Any) -> List[Any]:
    return x if isinstance(x, list) else []


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _pct(n: Optional[float], d: float) -> Optional[int]:
    if n is None or d <= 0:
        return None
    return int(round((float(n) / d) * 100.0))


def _get_path(d: Dict[str, Any], *keys: str) -> Any:
    cur: Any = d
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return None
        cur = cur[k]
    return cur


# -------------------------
# Local product + alerts
# -------------------------
def _find_local_product(products: List[Dict[str, Any]], key: str) -> Optional[Dict[str, Any]]:
    k = (key or "").strip()
    if not k:
        return None
    for p in products:
        if str(p.get("id", "")).strip() == k:
            return p
        if str(p.get("key", "")).strip() == k:
            return p
        if str(p.get("barcode", "")).strip() == k:
            return p
        if str(p.get("off_code", "")).strip() == k:
            return p
    return None


def _collect_alerts(rasff: List[Dict[str, Any]], product: Dict[str, Any]) -> List[str]:
    alerts: List[str] = []

    key_candidates = {
        str(product.get("barcode", "")).strip(),
        str(product.get("off_code", "")).strip(),
        str(product.get("id", "")).strip(),
        str(product.get("key", "")).strip(),
    }
    key_candidates = {k for k in key_candidates if k}

    name = (product.get("name") or "").lower()
    brand = (product.get("brand") or "").lower()

    for item in rasff:
        code = str(item.get("barcode") or item.get("off_code") or "").strip()
        if code and code in key_candidates:
            alerts.append(str(item.get("title") or item.get("alert") or "RASFF alert"))
            continue
        kw = str(item.get("keyword") or "").lower().strip()
        if kw and (kw in name or kw in brand):
            alerts.append(str(item.get("title") or item.get("alert") or "RASFF alert"))

    seen = set()
    out = []
    for a in alerts:
        if a and a not in seen:
            seen.add(a)
            out.append(a)
    return out


# -----------------------------
# Normalizer compatibility
# -----------------------------
def _normalize(raw: Dict[str, Any], source: Optional[str]) -> Dict[str, Any]:
    try:
        return normalize_product(raw, source=source)
    except TypeError:
        return normalize_product(raw)


# -----------------------------
# Nutrients extraction
# -----------------------------
def _nutrients_per_100(normalized: Dict[str, Any]) -> Dict[str, Optional[float]]:
    per100 = _get_path(normalized, "nutriments", "per_100")
    if isinstance(per100, dict) and per100:
        return {
            "energy_kcal": _to_float(per100.get("energy_kcal")),
            "sugar_g": _to_float(per100.get("sugar_g")),
            "saturated_fat_g": _to_float(per100.get("saturated_fat_g")),
            "salt_g": _to_float(per100.get("salt_g")),
            "fiber_g": _to_float(per100.get("fiber_g")),
            "protein_g": _to_float(per100.get("protein_g")),
            "fruits_veg_percent": _to_float(per100.get("fruits_veg_percent")),
        }

    old = normalized.get("nutrition_per_100") or {}
    if not isinstance(old, dict):
        old = {}
    return {
        "energy_kcal": _to_float(old.get("energy_kcal")),
        "sugar_g": _to_float(old.get("sugar_g")),
        "saturated_fat_g": _to_float(old.get("sat_fat_g") if "sat_fat_g" in old else old.get("saturated_fat_g")),
        "salt_g": _to_float(old.get("salt_g")),
        "fiber_g": _to_float(old.get("fiber_g")),
        "protein_g": _to_float(old.get("protein_g")),
        "fruits_veg_percent": _to_float(old.get("fruits_veg_percent")),
    }


def _guess_is_beverage(normalized: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
    meta_is_bev = _get_path(normalized, "meta", "is_beverage")
    if isinstance(meta_is_bev, bool):
        return meta_is_bev, {"signal": "meta.is_beverage", "value": meta_is_bev, "confidence": 0.95}

    dq_is_bev = _get_path(normalized, "data_quality", "is_beverage")
    if isinstance(dq_is_bev, bool):
        return dq_is_bev, {"signal": "data_quality.is_beverage", "value": dq_is_bev, "confidence": 0.85}

    unit_old = str(_get_path(normalized, "nutrition_per_100", "unit") or "").lower().strip()
    if unit_old == "ml":
        return True, {"signal": "nutrition_per_100.unit", "value": True, "confidence": 0.70}

    categories_values: List[str] = []
    for raw in (normalized.get("categories"), normalized.get("categories_tags")):
        if isinstance(raw, str):
            categories_values.extend([part.strip() for part in raw.split(",") if part and part.strip()])
        elif isinstance(raw, list):
            categories_values.extend([str(item).strip() for item in raw if str(item).strip()])
    categories_s = " ".join([str(x).lower() for x in categories_values])

    beverage_markers = [
        "beverage", "beverages", "drink", "drinks", "soft drink", "soda",
        "juice", "water", "sparkling", "energy drink", "sports drink",
        "tea", "coffee", "cola", "lemonade",
        "en:beverages", "en:soft-drinks", "en:juices", "en:waters",
    ]
    marker_hit = any(m in categories_s for m in beverage_markers)

    serving_unit = str(_get_path(normalized, "serving", "unit") or _get_path(normalized, "nutrition_per_100", "unit") or "").lower()
    serving_value = _to_float(_get_path(normalized, "serving", "value") or _get_path(normalized, "nutrition_per_100", "serving_size"))

    unit_hit = serving_unit in {"ml", "cl", "l"}
    confidence = 0.40
    signals = []
    if marker_hit:
        confidence += 0.35
        signals.append("category_marker")
    if unit_hit:
        confidence += 0.20
        signals.append("serving_unit_ml_like")
    if serving_value is not None and unit_hit and serving_value >= 150:
        confidence += 0.10
        signals.append("serving_value_plausible")

    confidence = float(_clamp(confidence, 0.0, 0.95))
    return (confidence >= 0.60), {
        "signal": "heuristic",
        "signals": signals,
        "confidence": confidence,
        "serving_unit": serving_unit or None,
        "serving_value": serving_value,
    }


def _serving_size_in_g_or_ml(normalized: Dict[str, Any], is_beverage: bool) -> Tuple[Optional[float], str, str]:
    unit = str(_get_path(normalized, "serving", "unit") or "").lower().strip()
    val = _to_float(_get_path(normalized, "serving", "value"))

    if val is None:
        val = _to_float(_get_path(normalized, "nutrition_per_100", "serving_size"))
        unit = str(_get_path(normalized, "nutrition_per_100", "unit") or unit).lower().strip()

    if val is not None and unit in {"g", "ml"}:
        return val, unit, "from_product"

    if is_beverage and val is not None and unit in {"cl", "l"}:
        if unit == "cl":
            return val * 10.0, "ml", "converted_from_cl"
        if unit == "l":
            return val * 1000.0, "ml", "converted_from_l"

    if is_beverage:
        return 250.0, "ml", "default_250ml"
    return 100.0, "g", "default_100g"


def _per_serving_from_per_100(per100_val: Optional[float], serving_amount: float) -> Optional[float]:
    if per100_val is None:
        return None
    return per100_val * (serving_amount / 100.0)


# -----------------------------
# Ingredients Intelligence v1 + E explanations
# -----------------------------
_E_NUMBER_RE = re.compile(r"\bE\s?(\d{3,4})([a-z])?\b", re.IGNORECASE)

# NEW: include PL "kofeina"
_CAFFEINE_MARKERS = ("caffeine", "koffein", "caffein", "kofeina")

# Small high-impact glossary (expand anytime)
_E_GLOSSARY: Dict[str, Dict[str, str]] = {
    "E950": {"name": "Acesulfame K", "meaning_el": "Γλυκαντικό (χωρίς ζάχαρη)"},
    "E951": {"name": "Aspartame", "meaning_el": "Γλυκαντικό"},
    "E955": {"name": "Sucralose", "meaning_el": "Γλυκαντικό"},
    "E960": {"name": "Steviol glycosides (Stevia)", "meaning_el": "Γλυκαντικό (στέβια)"},
    "E150D": {"name": "Caramel colour (Class IV)", "meaning_el": "Χρωστική καραμέλας"},
    "E150": {"name": "Caramel colour", "meaning_el": "Χρωστική καραμέλας"},
    "E330": {"name": "Citric acid", "meaning_el": "Οξύ / ρυθμιστής οξύτητας"},
    "E338": {"name": "Phosphoric acid", "meaning_el": "Οξύ / ρυθμιστής οξύτητας"},
    "E202": {"name": "Potassium sorbate", "meaning_el": "Συντηρητικό"},
    "E211": {"name": "Sodium benzoate", "meaning_el": "Συντηρητικό"},
    "E415": {"name": "Xanthan gum", "meaning_el": "Πηκτικό / σταθεροποιητής"},
    "E322": {"name": "Lecithins", "meaning_el": "Γαλακτωματοποιητής"},
    "E471": {"name": "Mono- & diglycerides of fatty acids", "meaning_el": "Γαλακτωματοποιητής"},
    "E621": {"name": "Monosodium glutamate (MSG)", "meaning_el": "Ενισχυτικό γεύσης"},
}

_ALLERGENS = {
    "milk": ["milk", "milch", "lait", "latte", "γάλα", "γαλα", "MILCH", "MILK"],
    "gluten": ["gluten", "wheat", "weizen", "σιτάρι", "σιταρι", "barley", "rye", "oats"],
    "soy": ["soy", "soja", "soya", "σόγια", "σογια"],
    "nuts": ["nuts", "almond", "hazelnut", "walnut", "peanut", "cashew", "pistachio", "αμύγδαλο", "φουντούκι"],
    "egg": ["egg", "eggs", "eier", "αυγό", "αυγο"],
    "fish": ["fish", "fisch", "ψάρι", "ψαρι"],
    "shellfish": ["shrimp", "prawn", "crab", "lobster", "shellfish", "γαρίδα", "γαριδα"],
    "sesame": ["sesame", "σησάμι", "σησαμι"],
    "mustard": ["mustard", "senf", "μουστάρδα", "μουσταρδα"],
    "celery": ["celery", "sellerie", "σέλινο", "σελινο"],
}

_DAIRY_MARKERS = [
    "milk", "milch", "lait", "yogurt", "yoghurt", "jogurt", "yaourt", "cheese", "käse", "fromage",
    "dairy", "γαλα", "γάλα", "γιαούρ", "τυρί",
]

_REDUCED_FAT_MARKERS = [
    "light", "lite", "low fat", "low-fat", "reduced fat", "reduced-fat", "fat free", "fat-free", "0%", "0 %",
    "reduced-fat", "reduced fat", "fettreduziert", "leicht", "lightprodukt", "allégé", "allege", "0% fat",
    "χαμηλα λιπαρα", "χαμηλά λιπαρά", "μειωμενα λιπαρα", "μειωμένα λιπαρά",
]

_NUTS_SEEDS_MARKERS = [
    "walnut", "walnuts", "nut", "nuts", "almond", "almonds", "hazelnut", "hazelnuts", "pistachio", "pistachios",
    "cashew", "cashews", "pecan", "pecans", "peanut", "peanuts", "seed", "seeds", "sesame", "sunflower seed",
    "pumpkin seed", "linseed", "flaxseed", "chia", "καρύδι", "καρύδια", "ξηροί καρποί", "σπόροι",
]

_LEGUME_MARKERS = [
    "legume", "legumes", "pulse", "pulses", "bean", "beans", "lentil", "lentils", "chickpea", "chickpeas",
    "pea", "peas", "fagioli", "lenticchie", "ρεβίθια", "φακές", "φασόλια", "όσπρια",
]

_YOGURT_MARKERS = ["yogurt", "yoghurt", "jogurt", "yaourt", "γιαούρτι", "γιαουρτι"]
_CHEESE_MARKERS = ["cheese", "käse", "fromage", "feta", "τυρί", "τυρι", "φέτα", "φετα"]
_TOMATO_VEG_MARKERS = [
    "tomato", "tomatoes", "chopped tomatoes", "diced tomatoes", "peeled tomatoes", "passata",
    "pomodoro", "pomodori", "tomate", "tomates", "ντομάτα", "ντομάτες", "τομάτα", "τομάτες",
]
_FRUIT_MARKERS = [
    "fruit", "fruits", "apple", "apples", "pear", "pears", "peach", "peaches", "apricot", "apricots",
    "banana", "bananas", "berry", "berries", "mango", "orange", "oranges", "grape", "grapes",
    "μήλο", "μήλα", "φρούτο", "φρούτα",
]
_OATS_GRAINS_MARKERS = [
    "oat", "oats", "oatmeal", "rolled oats", "whole grain oats", "porridge oats",
    "grain", "grains", "barley", "buckwheat", "quinoa", "rye", "spelt",
    "βρώμη", "δημητριακά",
]

_PLAIN_NUTS_SEEDS_MARKERS = [
    "walnut", "walnuts", "almond", "almonds", "hazelnut", "hazelnuts", "pistachio", "pistachios",
    "cashew", "cashews", "pecan", "pecans", "macadamia", "macadamias", "brazil nut", "brazil nuts",
    "mixed nuts", "mixed seeds", "sunflower seed", "pumpkin seed", "flaxseed", "linseed", "chia seed",
    "sesame seed", "καρύδι", "καρύδια", "αμύγδαλο", "αμύγδαλα", "φουντούκι", "φουντούκια",
    "ξηροί καρποί", "σπόροι",
]

_NUTS_SEEDS_EXCLUSION_MARKERS = [
    "salted", "sea salt", "with salt", "roasted salted", "gesalzen", "salé", "sale", "salt", "sel", "salz",
    "flavoured", "flavored", "aroma", "aromas", "arôme", "smoked", "bbq", "barbecue", "chili", "paprika",
    "spicy", "wasabi", "tamari", "soy sauce", "honey", "caramel", "candied", "praline", "glazed", "coated",
    "coating", "chocolate", "cocoa", "sugar", "syrup", "sirop", "sucre", "zucker", "ζάχαρη", "σοκολάτα",
]

_WHOLE_FOOD_EXCLUSION_MARKERS = [
    "sauce", "salsa", "ketchup", "spread", "dip", "seasoned", "seasoning", "prepared", "ready meal",
    "processed", "snack", "chips", "crisps", "cracker", "cookie", "biscuit", "dessert",
    "sauce tomate", "σάλτσα", "σνακ",
]

_ING_MAP = {
    "Sweetener": ["aspartame", "acesulfame", "acesulfame-k", "sucralose", "stevia", "steviol", "saccharin",
                  "cyclamate", "neotame", "advantame", "süßstoff", "sweetener", "sweeteners"],
    "Preservative": ["preservative", "preservatives", "konservierungsstoff", "konservierungsstoffe", "sorbate",
                     "sorbic", "benzoate", "benzoic", "nitrite", "nitrate", "sulfite", "sulphite", "natamycin"],
    "Emulsifier": ["emulsifier", "emulsifiers", "emulgator", "emulgatoren", "lecithin", "lecithins",
                   "mono- and diglycerides", "monoglycerides", "diglycerides"],
    "Stabilizer": ["stabilizer", "stabilizers", "stabilisator", "stabilisatoren", "xanthan", "guar", "pectin",
                   "carrageenan", "cellulose gum", "gellan"],
    "Colorant": ["color", "colour", "colorant", "farbstoff", "caramel", "caramel colour", "caramel color",
                 "barwnik", "barwniki"],  # PL
    "Flavoring": ["flavour", "flavouring", "flavourings", "flavor", "flavoring", "aroma", "aromas", "aromaty",
                  "natural flavourings", "natural flavorings"],
    "Caffeine": ["caffeine", "koffein", "caffein", "kofeina"],
    "Acidifier": ["acid", "acids", "säuerungsmittel", "citric", "phosphoric", "malic", "lactic",
                  "acidity regulator", "acid regulator", "acidity-regulator",
                  "kwas", "regulator kwasowości"],  # PL
}

_E_GROUPS = {
    "Sweetener": {"950", "951", "952", "954", "955", "957", "959", "960", "961", "962", "969"},
    "Preservative": {"200", "202", "203", "210", "211", "212", "213", "220", "221", "222", "223", "224", "225",
                     "226", "227", "228", "249", "250", "251", "252"},
    "Colorant": {"100", "101", "102", "104", "110", "120", "122", "124", "129", "131", "132", "133", "150", "150a",
                 "150b", "150c", "150d", "160a", "160c", "160d", "171"},
    "Emulsifier": {"322", "471", "472", "472a", "472b", "472c", "472d", "472e", "472f"},
    "Stabilizer": {"407", "410", "412", "414", "415", "418", "440", "441", "466"},
    "Acidifier": {"330", "338", "296", "270", "331", "332", "333"},
}

def _norm_ing_text(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"\s+", " ", s)
    return s

def _normalized_product_text(normalized: Dict[str, Any]) -> str:
    parts: List[str] = []
    for value in [
        normalized.get("name"),
        normalized.get("brand"),
    ]:
        if isinstance(value, str) and value.strip():
            parts.append(value.strip().lower())

    for key in ["categories", "categories_tags"]:
        raw = normalized.get(key)
        if isinstance(raw, str) and raw.strip():
            parts.append(raw.strip().lower())
        elif isinstance(raw, list):
            parts.extend([str(item).strip().lower() for item in raw if str(item).strip()])

    return " | ".join(parts)

def _contains_any(text: str, needles: List[str]) -> bool:
    tl = str(text or "").lower()
    return any(str(needle or "").lower() in tl for needle in needles)

def _pattern_score_adjustments(
    normalized: Dict[str, Any],
    per100: Dict[str, Optional[float]],
    intelligence: Dict[str, Any],
    *,
    is_beverage: bool,
) -> Dict[str, Any]:
    markers = intelligence.get("markers", {}) if isinstance(intelligence, dict) else {}
    product_text = _normalized_product_text(normalized)
    detected_e = _as_list(intelligence.get("detected_e_numbers")) if isinstance(intelligence, dict) else []
    e_count = len(detected_e)
    processing_score = int(_to_float(intelligence.get("processing_score")) or 0) if isinstance(intelligence, dict) else 0
    counts_by_class = intelligence.get("counts_by_class", {}) if isinstance(intelligence, dict) else {}
    ingredient_count = sum(int(v or 0) for v in counts_by_class.values()) if isinstance(counts_by_class, dict) else 0
    sugar = _to_float(per100.get("sugar_g"))
    dairy_like = _contains_any(product_text, _DAIRY_MARKERS)
    if isinstance(intelligence, dict) and "MILK" in _as_list(intelligence.get("allergens")):
        dairy_like = True
    reduced_fat_keyword = _contains_any(product_text, _REDUCED_FAT_MARKERS)
    zero_sweetened_beverage = bool(is_beverage and (sugar is not None and sugar <= 1.5) and int(markers.get("sweeteners", 0)) > 0)

    adjustments: List[Dict[str, Any]] = []
    cap: Optional[int] = None

    if zero_sweetened_beverage:
        adjustments.append({"rule_id": "non_sugar_sweetener_presence", "delta": -10})
        if int(markers.get("sweeteners", 0)) >= 2:
            adjustments.append({"rule_id": "multiple_non_sugar_sweeteners", "delta": -6})
        additive_complexity = (
            e_count >= 3
            or int(markers.get("flavourings", 0)) > 0
            or int(markers.get("colorants", 0)) > 0
            or processing_score >= 6
        )
        if additive_complexity:
            adjustments.append({"rule_id": "additive_heavy_zero_beverage", "delta": -8})
            cap = 74
        else:
            cap = 82

    simple_dairy = (
        dairy_like
        and reduced_fat_keyword
        and int(markers.get("sweeteners", 0)) == 0
        and int(markers.get("flavourings", 0)) == 0
        and int(markers.get("colorants", 0)) == 0
        and int(markers.get("preservatives", 0)) == 0
        and int(markers.get("emulsifiers_stabilizers", 0)) == 0
        and e_count <= 1
        and ingredient_count <= 6
        and processing_score <= 3
    )
    additive_heavy_dairy = (
        dairy_like
        and reduced_fat_keyword
        and (
            int(markers.get("sweeteners", 0)) > 0
            or int(markers.get("flavourings", 0)) > 0
            or int(markers.get("emulsifiers_stabilizers", 0)) > 0
            or e_count >= 2
            or ingredient_count >= 8
            or processing_score >= 5
        )
    )

    if simple_dairy:
        adjustments.append({"rule_id": "reduced_fat_dairy_simple", "delta": 4})
    elif additive_heavy_dairy:
        adjustments.append({"rule_id": "reduced_fat_dairy_additive_heavy", "delta": -6})

    total_delta = sum(int(item.get("delta", 0)) for item in adjustments)
    return {
        "applied": adjustments,
        "total_delta": total_delta,
        "score_cap": cap,
        "flags": {
            "zero_sweetened_beverage": zero_sweetened_beverage,
            "simple_reduced_fat_dairy": simple_dairy,
            "additive_heavy_light_dairy": additive_heavy_dairy,
        },
    }


def _traditional_balance_adjustments(
    normalized: Dict[str, Any],
    per100: Dict[str, Optional[float]],
    intelligence: Dict[str, Any],
    *,
    is_beverage: bool,
    lang: str,
) -> Dict[str, Any]:
    if is_beverage:
        return {"applied": [], "total_delta": 0}

    markers = intelligence.get("markers", {}) if isinstance(intelligence, dict) else {}
    if not isinstance(markers, dict):
        markers = {}
    product_text = _normalized_product_text(normalized)
    ingredients = _as_list(normalized.get("ingredients"))
    ingredient_count = len(ingredients)
    processing_score = int(_to_float(intelligence.get("processing_score")) or 0) if isinstance(intelligence, dict) else 0
    e_count = int(markers.get("e_numbers") or 0)
    salt = _to_float(per100.get("salt_g"))
    satfat = _to_float(per100.get("saturated_fat_g"))
    energy = _to_float(per100.get("energy_kcal"))
    sugar = _to_float(per100.get("sugar_g"))

    no_additives = all(int(markers.get(k) or 0) == 0 for k in (
        "sweeteners", "flavourings", "colorants", "preservatives", "emulsifiers_stabilizers", "e_numbers"
    ))
    minimally_processed = processing_score <= 2
    simple_single = ingredient_count <= 2
    simple_short = ingredient_count <= 4
    nuts_or_seeds = _contains_any(product_text, _NUTS_SEEDS_MARKERS)
    plain_nuts_or_seeds = _contains_any(product_text, _PLAIN_NUTS_SEEDS_MARKERS)
    nuts_seed_excluded = _contains_any(product_text, _NUTS_SEEDS_EXCLUSION_MARKERS)
    legumes = _contains_any(product_text, _LEGUME_MARKERS)
    plain_yogurt = _contains_any(product_text, _YOGURT_MARKERS) and int(markers.get("sweeteners", 0)) == 0 and int(markers.get("flavourings", 0)) == 0
    simple_cheese = _contains_any(product_text, _CHEESE_MARKERS) and ingredient_count <= 5 and int(markers.get("sweeteners", 0)) == 0
    nutrient_dense_category = nuts_or_seeds or legumes or plain_yogurt or simple_cheese
    plain_nuts_seed_candidate = bool(
        plain_nuts_or_seeds
        and not nuts_seed_excluded
        and minimally_processed
        and no_additives
        and simple_short
        and int(markers.get("sweeteners", 0)) == 0
        and int(markers.get("flavourings", 0)) == 0
        and int(markers.get("colorants", 0)) == 0
        and int(markers.get("preservatives", 0)) == 0
        and e_count == 0
        and (salt is None or salt <= 0.2)
        and (sugar is None or sugar <= 6.0)
    )

    message_map = {
        "single_ingredient_simple": {
            "el": "Η απλή, μονοσυστατική σύνθεση λειτουργεί θετικά.",
            "en": "The simple single-ingredient composition helps the assessment.",
            "de": "Die einfache Zusammensetzung aus nur einer Zutat wirkt sich positiv aus.",
            "fr": "La composition simple à ingrédient unique aide l’évaluation.",
        },
        "minimal_processing": {
            "el": "Το προϊόν είναι ελάχιστα επεξεργασμένο.",
            "en": "The product is minimally processed.",
            "de": "Das Produkt ist nur minimal verarbeitet.",
            "fr": "Le produit est très peu transformé.",
        },
        "nutrient_dense_category": {
            "el": "Η κατηγορία του προϊόντος έχει υψηλή θρεπτική πυκνότητα.",
            "en": "This product category has high nutrient density.",
            "de": "Diese Produktkategorie weist eine hohe Nährstoffdichte auf.",
            "fr": "Cette catégorie de produit présente une forte densité nutritionnelle.",
        },
        "no_additives_simple": {
            "el": "Η απουσία προσθέτων λειτουργεί θετικά.",
            "en": "The absence of additives helps the assessment.",
            "de": "Das Fehlen von Zusatzstoffen wirkt sich positiv aus.",
            "fr": "L’absence d’additifs aide l’évaluation.",
        },
        "traditional_simple": {
            "el": "Η απλή παραδοσιακή σύνθεση βελτιώνει τη συνολική εικόνα.",
            "en": "The simple traditional composition improves the overall picture.",
            "de": "Die einfache traditionelle Zusammensetzung verbessert das Gesamtbild.",
            "fr": "La composition simple et traditionnelle améliore l’ensemble.",
        },
        "plain_nuts_seed_category": {
            "el": "Η κατηγορία των απλών ξηρών καρπών έχει υψηλή θρεπτική πυκνότητα.",
            "en": "Plain nuts and seeds are a nutrient-dense product category.",
            "de": "Einfache Nüsse und Samen gehören zu einer nährstoffdichten Produktkategorie.",
            "fr": "Les noix et graines simples appartiennent à une catégorie à forte densité nutritionnelle.",
        },
        "plain_nuts_seed_simple": {
            "el": "Η μονοσυστατική σύνθεση λειτουργεί θετικά.",
            "en": "The single-ingredient composition helps the assessment.",
            "de": "Die Zusammensetzung aus nur einer Zutat wirkt sich positiv aus.",
            "fr": "La composition à ingrédient unique aide l’évaluation.",
        },
    }

    applied: List[Dict[str, Any]] = []

    if simple_single and minimally_processed:
        applied.append({"rule_id": "single_ingredient_simple", "delta": 3, "impact_weight": 58})
    if minimally_processed and simple_short:
        applied.append({"rule_id": "minimal_processing", "delta": 2, "impact_weight": 56})
    if no_additives and simple_short:
        applied.append({"rule_id": "no_additives_simple", "delta": 2, "impact_weight": 54})
    if nutrient_dense_category:
        applied.append({"rule_id": "nutrient_dense_category", "delta": 2, "impact_weight": 52})
    if (plain_yogurt or simple_cheese) and simple_short and no_additives:
        applied.append({"rule_id": "traditional_simple", "delta": 1, "impact_weight": 50})
    if plain_nuts_seed_candidate:
        applied.append({"rule_id": "plain_nuts_seed_category", "delta": 4, "impact_weight": 64})
        if simple_single:
            applied.append({"rule_id": "plain_nuts_seed_simple", "delta": 2, "impact_weight": 60})

    total_delta = sum(int(item.get("delta", 0)) for item in applied)
    if salt is not None and salt >= 1.8:
        total_delta -= 2
    if satfat is not None and satfat >= 10:
        total_delta -= 1
    if energy is not None and energy >= 650 and not nuts_or_seeds:
        total_delta -= 1

    total_cap = 13 if plain_nuts_seed_candidate else 7
    total_delta = max(0, min(total_cap, total_delta))
    running = 0
    kept: List[Dict[str, Any]] = []
    for item in applied:
        delta = int(item.get("delta", 0))
        if running + delta > total_delta:
            continue
        running += delta
        kept.append({
            "rule_id": item["rule_id"],
            "delta": delta,
            "impact_direction": "positive",
            "impact_weight": item.get("impact_weight", 50),
            "message": message_map[item["rule_id"]].get(lang) or message_map[item["rule_id"]]["en"],
        })

    return {
        "applied": kept,
        "total_delta": total_delta,
        "flags": {
            "simple_single": simple_single,
            "minimally_processed": minimally_processed,
            "nutrient_dense_category": nutrient_dense_category,
            "no_additives": no_additives,
            "plain_nuts_seed_candidate": plain_nuts_seed_candidate,
        },
    }


def _whole_food_floor_adjustments(
    normalized: Dict[str, Any],
    per100: Dict[str, Optional[float]],
    intelligence: Dict[str, Any],
    *,
    is_beverage: bool,
    lang: str,
    current_score: int,
) -> Dict[str, Any]:
    if is_beverage:
        return {"applied": [], "floor_score": None, "floor_delta": 0}

    markers = intelligence.get("markers", {}) if isinstance(intelligence, dict) else {}
    if not isinstance(markers, dict):
        markers = {}
    product_text = _normalized_product_text(normalized)
    ingredient_count = len(_as_list(normalized.get("ingredients")))
    processing_score = int(_to_float(intelligence.get("processing_score")) or 0) if isinstance(intelligence, dict) else 0
    salt = _to_float(per100.get("salt_g"))
    sugar = _to_float(per100.get("sugar_g"))
    satfat = _to_float(per100.get("saturated_fat_g"))

    no_additives = all(int(markers.get(k) or 0) == 0 for k in (
        "sweeteners", "flavourings", "colorants", "preservatives", "emulsifiers_stabilizers", "e_numbers"
    ))
    minimally_processed = processing_score <= 2
    simple_single = ingredient_count <= 2
    simple_short = ingredient_count <= 4
    excluded = _contains_any(product_text, _WHOLE_FOOD_EXCLUSION_MARKERS)

    plain_nuts_seed = (
        _contains_any(product_text, _PLAIN_NUTS_SEEDS_MARKERS)
        and not _contains_any(product_text, _NUTS_SEEDS_EXCLUSION_MARKERS)
        and simple_short and minimally_processed and no_additives
        and (salt is None or salt <= 0.2)
        and (sugar is None or sugar <= 6.0)
    )
    plain_legumes = (
        _contains_any(product_text, _LEGUME_MARKERS)
        and simple_short and minimally_processed and no_additives and not excluded
        and (salt is None or salt <= 0.2)
        and (sugar is None or sugar <= 6.0)
    )
    plain_tomato_veg = (
        _contains_any(product_text, _TOMATO_VEG_MARKERS)
        and simple_short and minimally_processed and no_additives and not excluded
        and (salt is None or salt <= 0.2)
        and (sugar is None or sugar <= 8.0)
    )
    plain_fruit = (
        _contains_any(product_text, _FRUIT_MARKERS)
        and simple_short and minimally_processed and no_additives and not excluded
        and (salt is None or salt <= 0.1)
        and (sugar is None or sugar <= 18.0)
    )
    simple_oats_grains = (
        _contains_any(product_text, _OATS_GRAINS_MARKERS)
        and simple_short and minimally_processed and no_additives and not excluded
        and (salt is None or salt <= 0.2)
        and (sugar is None or sugar <= 10.0)
    )

    floor_score: Optional[int] = None
    applied: List[Dict[str, Any]] = []

    if plain_nuts_seed:
        floor_score = 64
    elif plain_legumes:
        floor_score = 63
    elif plain_tomato_veg:
        floor_score = 62
    elif simple_oats_grains:
        floor_score = 60
    elif plain_fruit:
        floor_score = 61

    if floor_score is None:
        return {"applied": [], "floor_score": None, "floor_delta": 0}

    if salt is not None and salt >= 1.0:
        floor_score -= 4
    if sugar is not None and sugar >= 15.0 and not plain_fruit:
        floor_score -= 4
    if satfat is not None and satfat >= 15.0 and not plain_nuts_seed:
        floor_score -= 3

    floor_score = int(max(0, floor_score))
    if current_score >= floor_score:
        return {"applied": [], "floor_score": floor_score, "floor_delta": 0}

    message_map = {
        "whole_food_category": {
            "el": "Το προϊόν ανήκει σε κατηγορία απλών, ολόκληρων τροφίμων.",
            "en": "This product belongs to a simple whole-food category.",
            "de": "Dieses Produkt gehört zu einer einfachen Whole-Food-Kategorie.",
            "fr": "Ce produit appartient à une catégorie d’aliments entiers simples.",
        },
        "minimal_processing_floor": {
            "el": "Η ελάχιστη επεξεργασία βελτιώνει τη συνολική εικόνα.",
            "en": "Minimal processing improves the overall picture.",
            "de": "Die minimale Verarbeitung verbessert das Gesamtbild.",
            "fr": "La transformation minimale améliore l’ensemble.",
        },
        "simple_category_floor": {
            "el": "Η πολύ απλή σύνθεση της κατηγορίας λειτουργεί θετικά.",
            "en": "The very simple category composition helps the assessment.",
            "de": "Die sehr einfache Zusammensetzung dieser Kategorie wirkt sich positiv aus.",
            "fr": "La composition très simple de cette catégorie aide l’évaluation.",
        },
        "not_processed_snack_floor": {
            "el": "Η κατηγορία αυτή δεν πρέπει να αντιμετωπίζεται όπως τα επεξεργασμένα σνακ.",
            "en": "This category should not be treated like processed snack foods.",
            "de": "Diese Kategorie sollte nicht wie verarbeitete Snacks behandelt werden.",
            "fr": "Cette catégorie ne doit pas être traitée comme des snacks transformés.",
        },
    }

    reason_ids = ["whole_food_category", "minimal_processing_floor", "simple_category_floor"]
    if plain_nuts_seed or simple_oats_grains or plain_legumes:
        reason_ids.append("not_processed_snack_floor")

    for idx, rule_id in enumerate(reason_ids):
        applied.append({
            "rule_id": rule_id,
            "impact_direction": "positive",
            "impact_weight": 57 - idx,
            "message": message_map[rule_id].get(lang) or message_map[rule_id]["en"],
        })

    return {
        "applied": applied,
        "floor_score": floor_score,
        "floor_delta": int(floor_score - current_score),
        "flags": {
            "plain_nuts_seed": plain_nuts_seed,
            "plain_legumes": plain_legumes,
            "plain_tomato_veg": plain_tomato_veg,
            "plain_fruit": plain_fruit,
            "simple_oats_grains": simple_oats_grains,
        },
    }

def _extract_e_numbers(text: str) -> List[str]:
    out: List[str] = []
    for m in _E_NUMBER_RE.finditer(text or ""):
        num = (m.group(1) or "").strip()
        suf = (m.group(2) or "").strip().lower()
        code = f"E{num}{suf}".upper() if suf else f"E{num}".upper()
        if code not in out:
            out.append(code)
    return out

def _e_base(code: str) -> str:
    c = code.strip().upper().replace(" ", "")
    m = re.match(r"^(E\d{3,4})([A-Z])?$", c)
    if not m:
        return c
    base = m.group(1)
    suf = m.group(2)
    return f"{base}{suf}" if suf else base

def _class_from_e(e_code: str) -> str:
    t = e_code.strip().upper().replace(" ", "")
    m = re.match(r"E(\d{3,4})([A-Z])?", t)
    if not m:
        return "Additive"
    num = m.group(1)
    suf = (m.group(2) or "").lower()
    key = f"{num}{suf}" if suf else num
    for cls, nums in _E_GROUPS.items():
        if key in nums or num in nums:
            return cls
    return "Additive"

def _e_explain(e_code: str) -> Dict[str, str]:
    c = _e_base(e_code)
    info = _E_GLOSSARY.get(c)
    if not info and len(c) > 4 and c[:-1] in _E_GLOSSARY:
        info = _E_GLOSSARY.get(c[:-1])
    role = _class_from_e(c)
    if not info:
        return {
            "code": c,
            "name": "Food additive (E-number)",
            "role": role,
            "meaning_el": "Κωδικός πρόσθετου τροφίμων στην ΕΕ (δείχνουμε τον βασικό ρόλο).",
        }
    return {"code": c, "name": info.get("name","Food additive"), "role": role, "meaning_el": info.get("meaning_el","")}

def _detect_allergens(text: str) -> List[str]:
    t = (text or "")
    tl = t.lower()
    found: List[str] = []

    caps_tokens = set(re.findall(r"\b[A-ZÄÖÜ]{3,}\b", t))
    for allergen, markers in _ALLERGENS.items():
        for mk in markers:
            if mk in caps_tokens:
                if allergen not in found:
                    found.append(allergen)
                break

    for allergen, markers in _ALLERGENS.items():
        for mk in markers:
            if str(mk).lower() in tl:
                if allergen not in found:
                    found.append(allergen)
                break
    return found

def _classify_ingredient(name: str) -> Tuple[str, str, List[str], List[str]]:
    raw = _norm_ing_text(name)
    tl = raw.lower()
    tags: List[str] = []
    matches: List[str] = []

    e_nums = _extract_e_numbers(raw)
    if e_nums:
        tags.extend(e_nums)
        chosen = _class_from_e(e_nums[0])
        matches.append("e_number")
        risk = "medium" if chosen in {"Sweetener","Preservative","Colorant"} else "low"
        return chosen, risk, tags, matches

    for cls, kws in _ING_MAP.items():
        for kw in kws:
            if kw in tl:
                matches.append(f"kw:{kw}")
                risk = "medium" if cls in {"Sweetener","Preservative"} else "low"
                return cls, risk, tags, matches

    alls = _detect_allergens(raw)
    if alls:
        matches.append("allergen")
        tags.extend([a.upper() for a in alls])
        return "Allergen", "medium", tags, matches

    return "Other", "low", tags, matches

def _e_from_additives_tags(tags: Any) -> List[str]:
    """
    OFF additives_tags example: ["en:e330","en:e150d","en:e338"]
    Convert to ["E330","E150D","E338"]
    """
    out: List[str] = []
    for t in _as_list(tags):
        s = str(t or "").strip().lower()
        if not s:
            continue
        # accept formats: en:e150d, e150d, fr:e330
        m = re.search(r"e(\d{3,4})([a-z])?$", s)
        if not m:
            continue
        num = m.group(1)
        suf = (m.group(2) or "").upper()
        code = f"E{num}{suf}" if suf else f"E{num}"
        if code not in out:
            out.append(code)
    return out

def _collect_additives_tags_from_sources(norm: Dict[str, Any], raw: Any) -> List[str]:
    """
    Try multiple places:
    - norm["additives_tags"] / norm["additives_original_tags"]
    - raw["product"]["additives_tags"] / raw["additives_tags"]
    """
    candidates: List[str] = []
    for key in ("additives_tags", "additives_original_tags"):
        candidates += _e_from_additives_tags(norm.get(key))

    if isinstance(raw, dict):
        # raw may be OFF full payload {"product": {...}}
        prod = raw.get("product") if isinstance(raw.get("product"), dict) else raw
        if isinstance(prod, dict):
            candidates += _e_from_additives_tags(prod.get("additives_tags"))
            candidates += _e_from_additives_tags(prod.get("additives_original_tags"))
        candidates += _e_from_additives_tags(raw.get("additives_tags"))
        candidates += _e_from_additives_tags(raw.get("additives_original_tags"))

    # dedupe keep order
    seen = set()
    out: List[str] = []
    for c in candidates:
        if c not in seen:
            seen.add(c)
            out.append(c)
    return out

def _ingredients_intelligence(
    ingredients: List[Dict[str, Any]],
    *,
    is_beverage: bool,
    additives_e_numbers: List[str],
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    enriched: List[Dict[str, Any]] = []
    counts: Dict[str, int] = {}
    all_e_numbers: List[str] = []
    all_allergens: List[str] = []
    markers: Dict[str, int] = {
        "sweeteners": 0,
        "flavourings": 0,
        "emulsifiers_stabilizers": 0,
        "preservatives": 0,
        "colorants": 0,
        "e_numbers": 0,
        "caffeine": 0,
    }

    global_caffeine_found = False

    # 1) start with E-numbers from additives_tags
    for e in additives_e_numbers or []:
        e = _e_base(e)
        if e and e not in all_e_numbers:
            all_e_numbers.append(e)

    # 2) enrich ingredients and also collect E from text
    for ing in ingredients or []:
        name = ing.get("name") if isinstance(ing, dict) else str(ing)
        name = _norm_ing_text(str(name or ""))
        if not name:
            continue

        tl = name.lower()
        if any(mk in tl for mk in _CAFFEINE_MARKERS):
            global_caffeine_found = True

        cls, risk, tags, matches = _classify_ingredient(name)

        counts[cls] = counts.get(cls, 0) + 1

        tags_e = [t for t in tags if str(t).upper().startswith("E")]
        if tags_e:
            markers["e_numbers"] += 1
            for e in tags_e:
                e = _e_base(str(e).upper().replace(" ", ""))
                if e not in all_e_numbers:
                    all_e_numbers.append(e)

        if cls == "Sweetener":
            markers["sweeteners"] += 1
        elif cls in {"Emulsifier", "Stabilizer"}:
            markers["emulsifiers_stabilizers"] += 1
        elif cls == "Preservative":
            markers["preservatives"] += 1
        elif cls == "Colorant":
            markers["colorants"] += 1
        elif cls == "Flavoring":
            markers["flavourings"] += 1
        elif cls == "Caffeine":
            markers["caffeine"] += 1

        alls = _detect_allergens(name)
        for a in alls:
            if a not in all_allergens:
                all_allergens.append(a)

        out = dict(ing) if isinstance(ing, dict) else {"name": name}
        out["name"] = name
        out["class"] = cls
        out["risk"] = risk
        if tags:
            out["tags"] = tags
        if matches:
            out["matches"] = matches
        enriched.append(out)

    if global_caffeine_found:
        markers["caffeine"] = max(markers["caffeine"], 1)

    # If any E found (either way), mark it
    if all_e_numbers:
        markers["e_numbers"] = max(markers["e_numbers"], 1)

    # Processing score
    score = 0.0
    score += min(3.0, markers["sweeteners"] * 1.5)
    score += min(2.0, markers["flavourings"] * 1.0)
    score += min(2.0, markers["emulsifiers_stabilizers"] * 0.8)
    score += min(1.5, markers["preservatives"] * 0.8)
    score += min(1.0, markers["colorants"] * 0.6)
    score += min(1.5, markers["e_numbers"] * 0.5)
    if is_beverage:
        score = min(10.0, score + (0.5 if markers["sweeteners"] > 0 else 0.0))

    score_i = int(round(_clamp(score, 0.0, 10.0)))
    if score_i <= 2:
        proc_label = "Minimally processed"
    elif score_i <= 5:
        proc_label = "Processed"
    else:
        proc_label = "Highly processed"

    # Build E-number details
    e_details = [_e_explain(e) for e in sorted(all_e_numbers)]
    seen = set()
    e_details_unique: List[Dict[str, str]] = []
    for it in e_details:
        c = it.get("code")
        if c and c not in seen:
            seen.add(c)
            e_details_unique.append(it)

    flags: List[str] = []
    if markers["sweeteners"] > 0:
        flags.append(f"Sweeteners present ({markers['sweeteners']})")
    if markers["e_numbers"] > 0 and e_details_unique:
        flags.append(f"Additives / E-numbers detected ({len(e_details_unique)})")
    if markers["preservatives"] > 0:
        flags.append(f"Preservatives present ({markers['preservatives']})")
    if markers["emulsifiers_stabilizers"] > 0:
        flags.append(f"Emulsifiers/Stabilizers present ({markers['emulsifiers_stabilizers']})")
    if markers["flavourings"] > 0:
        flags.append(f"Flavourings present ({markers['flavourings']})")
    if markers["colorants"] > 0:
        flags.append(f"Colorants present ({markers['colorants']})")
    if markers["caffeine"] > 0:
        flags.append("Contains caffeine")
    if all_allergens:
        flags.append("Allergens: " + ", ".join([a.upper() for a in all_allergens]))

    intelligence = {
        "processing_score": score_i,
        "processing_label": proc_label,
        "flags": flags,
        "counts_by_class": counts,
        "detected_e_numbers": sorted(all_e_numbers),
        "e_number_details": e_details_unique,
        "allergens": [a.upper() for a in all_allergens],
        "markers": markers,
        "notes": [
            "Ingredients Intelligence is heuristic and depends on ingredient text quality.",
            "Processing score is an informational index, not an official NOVA classification.",
        ],
    }
    return enriched, intelligence


# -----------------------------
# VitaScore v3_hybrid_pro
# -----------------------------
@dataclass
class VitaScoreConfig:
    w_per100: float = 0.70
    w_serving: float = 0.30
    beverage_sugar_multiplier: float = 1.25
    beverage_energy_multiplier: float = 0.90
    serving_sugar_spike_threshold_g: float = 25.0
    serving_sugar_spike_multiplier: float = 1.20
    serving_sugar_spike_cap: float = 12.0
    cap_sugar_points: float = 40.0
    cap_satfat_points: float = 18.0
    cap_salt_points: float = 18.0
    cap_energy_points: float = 12.0
    cap_fiber_bonus: float = 12.0
    cap_protein_bonus: float = 8.0
    cap_fv_bonus: float = 10.0
    min_score: int = 1
    max_score: int = 100


def _points_from_thresholds(value: float, thresholds: List[Tuple[float, float]]) -> float:
    for mx, pts in thresholds:
        if value <= mx:
            return float(pts)
    return float(thresholds[-1][1])


def _score_per100(n: Dict[str, Optional[float]], is_beverage: bool, cfg: VitaScoreConfig) -> Tuple[float, Dict[str, Any]]:
    sugar = n["sugar_g"] or 0.0
    satfat = n["saturated_fat_g"] or 0.0
    salt = n["salt_g"] or 0.0
    energy = n["energy_kcal"] or 0.0
    fiber = n["fiber_g"] or 0.0
    protein = n["protein_g"] or 0.0
    fv = n.get("fruits_veg_percent") or 0.0

    sugar_thresholds = [(1.0, 0), (2.5, 3), (5.0, 8), (7.5, 14), (10.0, 20), (12.5, 26), (15.0, 32), (20.0, 40), (9999.0, 48)]
    satfat_thresholds = [(0.5, 0), (1.0, 2), (2.0, 5), (3.0, 8), (4.0, 11), (5.0, 14), (7.0, 18), (9999.0, 22)]
    salt_thresholds = [(0.10, 0), (0.25, 3), (0.50, 7), (0.75, 10), (1.00, 13), (1.25, 16), (1.50, 18), (9999.0, 22)]
    energy_thresholds = [(40, 0), (80, 2), (120, 4), (160, 6), (200, 8), (260, 10), (9999, 12)]

    sugar_pts = _points_from_thresholds(sugar, sugar_thresholds)
    energy_pts = _points_from_thresholds(energy, energy_thresholds)
    satfat_pts = _points_from_thresholds(satfat, satfat_thresholds)
    salt_pts = _points_from_thresholds(salt, salt_thresholds)

    if is_beverage:
        sugar_pts *= cfg.beverage_sugar_multiplier
        energy_pts *= cfg.beverage_energy_multiplier

    sugar_pts = min(sugar_pts, cfg.cap_sugar_points)
    satfat_pts = min(satfat_pts, cfg.cap_satfat_points)
    salt_pts = min(salt_pts, cfg.cap_salt_points)
    energy_pts = min(energy_pts, cfg.cap_energy_points)

    fiber_bonus = _clamp(fiber * 2.2, 0.0, cfg.cap_fiber_bonus)
    protein_bonus = _clamp(protein * 0.9, 0.0, cfg.cap_protein_bonus)

    fv_bonus = 0.0
    if fv > 0:
        fv_bonus = _clamp((fv - 20.0) / 6.0, 0.0, cfg.cap_fv_bonus)

    penalties = sugar_pts + satfat_pts + salt_pts + energy_pts
    bonuses = fiber_bonus + protein_bonus + fv_bonus

    part = {
        "mode": "per_100",
        "inputs": {
            "energy_kcal_per_100": n["energy_kcal"],
            "sugar_g_per_100": n["sugar_g"],
            "saturated_fat_g_per_100": n["saturated_fat_g"],
            "salt_g_per_100": n["salt_g"],
            "fiber_g_per_100": n["fiber_g"],
            "protein_g_per_100": n["protein_g"],
            "fruits_veg_percent": n.get("fruits_veg_percent"),
        },
        "penalties": {
            "sugar_points": round(sugar_pts, 2),
            "satfat_points": round(satfat_pts, 2),
            "salt_points": round(salt_pts, 2),
            "energy_points": round(energy_pts, 2),
            "total_penalties": round(penalties, 2),
        },
        "bonuses": {
            "fiber_bonus": round(fiber_bonus, 2),
            "protein_bonus": round(protein_bonus, 2),
            "fv_bonus": round(fv_bonus, 2),
            "total_bonuses": round(bonuses, 2),
        },
        "net": round(penalties - bonuses, 2),
    }
    return penalties - bonuses, part


def _score_serving(n: Dict[str, Optional[float]], serving_amount: float, is_beverage: bool, cfg: VitaScoreConfig) -> Tuple[float, Dict[str, Any]]:
    sugar_s = _per_serving_from_per_100(n["sugar_g"], serving_amount)
    energy_s = _per_serving_from_per_100(n["energy_kcal"], serving_amount)
    salt_s = _per_serving_from_per_100(n["salt_g"], serving_amount)

    if sugar_s is None and energy_s is None and salt_s is None:
        return 0.0, {"mode": "per_serving", "inputs": {}, "penalties": {"total_penalties": 0.0}, "bonuses": {"total_bonuses": 0.0}, "net": 0.0, "note": "no_serving_data"}

    sugar_s_val = sugar_s or 0.0
    energy_s_val = energy_s or 0.0
    salt_s_val = salt_s or 0.0

    sugar_serving_thresholds = [(2.5, 0), (5.0, 3), (10.0, 7), (15.0, 10), (25.0, 14), (35.0, 18), (9999.0, 22)]
    energy_serving_thresholds = [(80, 0), (150, 2), (250, 4), (350, 6), (500, 8), (700, 10), (9999, 12)]
    salt_serving_thresholds = [(0.20, 0), (0.50, 2), (1.00, 5), (1.50, 8), (2.00, 10), (9999.0, 12)]

    sugar_pts = _points_from_thresholds(sugar_s_val, sugar_serving_thresholds)
    energy_pts = _points_from_thresholds(energy_s_val, energy_serving_thresholds)
    salt_pts = _points_from_thresholds(salt_s_val, salt_serving_thresholds)

    if is_beverage:
        sugar_pts *= 1.15
        energy_pts *= 0.95

    spike_extra = 0.0
    if sugar_s is not None and sugar_s_val >= cfg.serving_sugar_spike_threshold_g:
        spike_extra = (sugar_s_val - cfg.serving_sugar_spike_threshold_g) * 0.20
        spike_extra *= cfg.serving_sugar_spike_multiplier
        spike_extra = min(spike_extra, cfg.serving_sugar_spike_cap)

    penalties = sugar_pts + energy_pts + salt_pts + spike_extra
    penalties = min(penalties, 28.0)

    part = {
        "mode": "per_serving",
        "inputs": {
            "serving_amount": serving_amount,
            "sugar_g_per_serving": round(sugar_s_val, 3) if sugar_s is not None else None,
            "energy_kcal_per_serving": round(energy_s_val, 1) if energy_s is not None else None,
            "salt_g_per_serving": round(salt_s_val, 3) if salt_s is not None else None,
        },
        "penalties": {
            "sugar_points": round(sugar_pts, 2),
            "energy_points": round(energy_pts, 2),
            "salt_points": round(salt_pts, 2),
            "sugar_spike_extra": round(spike_extra, 2),
            "total_penalties": round(penalties, 2),
        },
        "bonuses": {"total_bonuses": 0.0},
        "net": round(penalties, 2),
    }
    return penalties, part


def _map_net_to_vitascore(net: float, cfg: VitaScoreConfig) -> int:
    net = max(0.0, net)
    if net <= 10:
        score = 100 - net * 1.2
    elif net <= 25:
        score = 88 - (net - 10) * 1.3
    elif net <= 45:
        score = 68 - (net - 25) * 1.15
    else:
        score = 45 - (net - 45) * 0.85
    score = _clamp(score, float(cfg.min_score), float(cfg.max_score))
    return int(round(score))


def _who_sugar_impact(normalized: Dict[str, Any], per100: Dict[str, Optional[float]], is_beverage: bool) -> Dict[str, Any]:
    serving_amount, unit, note = _serving_size_in_g_or_ml(normalized, is_beverage)
    sugar_per100 = per100.get("sugar_g")

    sugar_per_serving = None
    if serving_amount is not None and sugar_per100 is not None:
        sugar_per_serving = _per_serving_from_per_100(sugar_per100, serving_amount)

    impact = {
        "reference": {"ideal_g_per_day": WHO_SUGAR_IDEAL, "upper_g_per_day": WHO_SUGAR_UPPER, "note": "Daily reference points; shown here as serving-level comparison."},
        "serving": {"serving_amount": serving_amount, "serving_unit": unit, "serving_source": note},
        "sugar": {
            "g_per_100": sugar_per100,
            "g_per_serving": round(sugar_per_serving, 2) if sugar_per_serving is not None else None,
            "percent_of_ideal_25g": _pct(sugar_per_serving, WHO_SUGAR_IDEAL) if sugar_per_serving is not None else None,
            "percent_of_upper_50g": _pct(sugar_per_serving, WHO_SUGAR_UPPER) if sugar_per_serving is not None else None,
        },
    }

    if sugar_per_serving is None:
        interp = "No reliable sugar-per-serving could be computed (missing sugar per 100 or serving size)."
    else:
        if sugar_per_serving >= WHO_SUGAR_UPPER:
            interp = "One serving exceeds the typical 'upper' daily sugar reference (50g)."
        elif sugar_per_serving >= WHO_SUGAR_IDEAL:
            interp = "One serving reaches or exceeds the typical 'ideal' daily sugar reference (25g)."
        elif sugar_per_serving >= 12.5:
            interp = "One serving is a noticeable share of the typical 25g/day sugar reference."
        else:
            interp = "One serving is a relatively small share of the typical 25g/day sugar reference."

    impact["interpretation"] = interp
    return impact


def _who_baseline_score(who_impact: Dict[str, Any], per100: Dict[str, Optional[float]], *, is_beverage: bool) -> Tuple[int, Dict[str, Any]]:
    serving = who_impact.get("serving", {}) if isinstance(who_impact, dict) else {}
    s_amount = serving.get("serving_amount")
    s_unit = serving.get("serving_unit")

    if not isinstance(s_amount, (int, float)):
        s_amount = 100.0
        s_unit = "ml" if is_beverage else "g"

    sugar = who_impact.get("sugar", {}) if isinstance(who_impact, dict) else {}
    sugar_pct_ideal = sugar.get("percent_of_ideal_25g")
    sugar_pct_upper = sugar.get("percent_of_upper_50g")

    salt_per100 = per100.get("salt_g")
    satfat_per100 = per100.get("saturated_fat_g")

    salt_serv = float(salt_per100) * (float(s_amount) / 100.0) if salt_per100 is not None else None
    satfat_serv = float(satfat_per100) * (float(s_amount) / 100.0) if satfat_per100 is not None else None

    salt_pct = _pct(salt_serv, WHO_SALT_G_PER_DAY) if salt_serv is not None else None
    satfat_pct = _pct(satfat_serv, WHO_SATFAT_G_PER_DAY_PROXY) if satfat_serv is not None else None

    sugar_load = float(sugar_pct_ideal or 0.0) / 100.0
    salt_load = float(salt_pct or 0.0) / 100.0
    satf_load = float(satfat_pct or 0.0) / 100.0

    sugar_pen = min(90.0, 60.0 * sugar_load)
    salt_pen  = min(35.0, 30.0 * salt_load)
    satf_pen  = min(30.0, 25.0 * satf_load)

    if isinstance(sugar_pct_upper, (int, float)) and float(sugar_pct_upper) >= 100.0:
        sugar_pen = min(95.0, sugar_pen + 15.0)

    total_pen = sugar_pen + salt_pen + satf_pen
    who_score = int(round(_clamp(100.0 - total_pen, 1.0, 100.0)))

    breakdown = {
        "mode": "who_baseline",
        "serving": {"amount": float(s_amount), "unit": s_unit},
        "inputs": {
            "sugar_pct_ideal_25g": sugar_pct_ideal,
            "sugar_pct_upper_50g": sugar_pct_upper,
            "salt_g_per_serving": round(salt_serv, 3) if salt_serv is not None else None,
            "salt_pct_5g": salt_pct,
            "satfat_g_per_serving": round(satfat_serv, 3) if satfat_serv is not None else None,
            "satfat_pct_20g_proxy": satfat_pct,
        },
        "penalties": {
            "sugar_penalty": round(sugar_pen, 1),
            "salt_penalty": round(salt_pen, 1),
            "satfat_penalty": round(satf_pen, 1),
            "total_penalty": round(total_pen, 1),
        },
        "score": who_score,
        "notes": [
            "WHO baseline uses serving-level share of daily reference points.",
            "Sat fat uses a practical 20g/day proxy (assumption).",
        ],
    }
    return who_score, breakdown


# -----------------------------
# Explainability + data quality
# -----------------------------
def _build_explanations(
    per100: Dict[str, Optional[float]],
    breakdown: Dict[str, Any],
    is_beverage: bool,
    lang: str = "en",
) -> Tuple[List[str], List[str]]:
    why: List[str] = []
    tips: List[str] = []

    sugar = per100.get("sugar_g")
    salt = per100.get("salt_g")
    satfat = per100.get("saturated_fat_g")
    fiber = per100.get("fiber_g")

    sugar_pts = _to_float(_get_path(breakdown, "per_100", "penalties", "sugar_points")) or 0.0
    salt_pts = _to_float(_get_path(breakdown, "per_100", "penalties", "salt_points")) or 0.0
    satfat_pts = _to_float(_get_path(breakdown, "per_100", "penalties", "satfat_points")) or 0.0
    unit = "ml" if is_beverage else "g"

    if sugar is not None and sugar_pts >= 10:
        why.append(t(lang, "why_high_sugar_density", value=f"{sugar:.1f}", unit=unit))
    elif sugar is not None and sugar >= 5:
        why.append(t(lang, "why_moderate_sugar", value=f"{sugar:.1f}", unit=unit))

    if satfat is not None and satfat_pts >= 8:
        why.append(t(lang, "why_sat_fat_elevated", value=f"{satfat:.1f}", unit=unit))
    if salt is not None and salt_pts >= 7:
        why.append(t(lang, "why_salt_notable", value=f"{salt:.2f}", unit=unit))

    if fiber is not None and fiber >= 2.5:
        why.append(t(lang, "why_fiber_bonus", value=f"{fiber:.1f}", unit=unit))

    spike = _to_float(_get_path(breakdown, "per_serving", "penalties", "sugar_spike_extra")) or 0.0
    sugar_serv = _to_float(_get_path(breakdown, "per_serving", "inputs", "sugar_g_per_serving"))
    if sugar_serv is not None and spike > 0:
        why.append(t(lang, "why_serving_spike", value=f"{sugar_serv:.1f}"))

    who_base = _get_path(breakdown, "who_baseline", "score")
    if isinstance(who_base, int):
        why.append(t(lang, "why_who_anchor", score=who_base))

    if is_beverage:
        if sugar is not None and sugar >= 5:
            tips.append(t(lang, "tip_drinks_zero"))
        tips.append(t(lang, "tip_balance_day"))

    if not why:
        why.append(t(lang, "why_no_single_driver"))

    return why[:5], tips[:5]


def _localize_intelligence(intel: Dict[str, Any], lang: str) -> Dict[str, Any]:
    if not isinstance(intel, dict):
        return intel

    localized = dict(intel)
    flags = _as_list(localized.get("flags"))
    notes = _as_list(localized.get("notes"))

    out_flags: List[str] = []
    for f in flags:
        s = str(f)
        if s.startswith("Sweeteners present (") and s.endswith(")"):
            out_flags.append(t(lang, "intel_flag_sweeteners", count=s[s.find("(") + 1 : -1]))
        elif s.startswith("Additives / E-numbers detected (") and s.endswith(")"):
            out_flags.append(t(lang, "intel_flag_additives", count=s[s.find("(") + 1 : -1]))
        elif s.startswith("Preservatives present (") and s.endswith(")"):
            out_flags.append(t(lang, "intel_flag_preservatives", count=s[s.find("(") + 1 : -1]))
        elif s.startswith("Emulsifiers/Stabilizers present (") and s.endswith(")"):
            out_flags.append(t(lang, "intel_flag_emulsifiers", count=s[s.find("(") + 1 : -1]))
        elif s.startswith("Flavourings present (") and s.endswith(")"):
            out_flags.append(t(lang, "intel_flag_flavourings", count=s[s.find("(") + 1 : -1]))
        elif s.startswith("Colorants present (") and s.endswith(")"):
            out_flags.append(t(lang, "intel_flag_colorants", count=s[s.find("(") + 1 : -1]))
        elif s == "Contains caffeine":
            out_flags.append(t(lang, "intel_flag_caffeine"))
        elif s.startswith("Allergens: "):
            out_flags.append(t(lang, "intel_flag_allergens", items=s.replace("Allergens: ", "", 1)))
        else:
            out_flags.append(s)
    localized["flags"] = out_flags

    out_notes: List[str] = []
    for n in notes:
        s = str(n)
        if s == "Ingredients Intelligence is heuristic and depends on ingredient text quality.":
            out_notes.append(t(lang, "intel_note_heuristic"))
        elif s == "Processing score is an informational index, not an official NOVA classification.":
            out_notes.append(t(lang, "intel_note_processing"))
        else:
            out_notes.append(s)
    localized["notes"] = out_notes
    return localized


def _localize_data_quality_notes(dq: Dict[str, Any], lang: str) -> Dict[str, Any]:
    if not isinstance(dq, dict):
        return dq
    localized = dict(dq)
    notes = _as_list(localized.get("notes"))
    out_notes: List[str] = []
    for n in notes:
        s = str(n)
        if s == "Confidence reflects completeness of nutrition facts + serving info.":
            out_notes.append(t(lang, "dq_note_confidence"))
        elif s == "Educational tool - not medical advice.":
            out_notes.append(t(lang, "dq_note_educational"))
        elif s == "Beverage locked by curated layer.":
            out_notes.append(t(lang, "dq_note_curated"))
        else:
            out_notes.append(s)
    localized["notes"] = out_notes
    return localized


def _data_quality(normalized: Dict[str, Any], per100: Dict[str, Optional[float]], bev_meta: Dict[str, Any]) -> Dict[str, Any]:
    required_keys = ["energy_kcal", "sugar_g", "salt_g", "saturated_fat_g"]
    present = sum(1 for k in required_keys if per100.get(k) is not None)
    missing = [k for k in required_keys if per100.get(k) is None]

    serving_amount = _to_float(_get_path(normalized, "serving", "value") or _get_path(normalized, "nutrition_per_100", "serving_size"))
    serving_unit = str(_get_path(normalized, "serving", "unit") or _get_path(normalized, "nutrition_per_100", "unit") or "").lower().strip()
    has_serving = serving_amount is not None and serving_amount > 0 and serving_unit in {"g", "ml", "cl", "l"}

    confidence = 0.35 + (present / len(required_keys)) * 0.45
    if has_serving:
        confidence += 0.10

    bev_conf = _to_float(bev_meta.get("confidence")) if isinstance(bev_meta, dict) else None
    if bev_conf is not None:
        confidence += (bev_conf - 0.5) * 0.10

    confidence = float(_clamp(confidence, 0.05, 0.95))
    notes = [
        "Confidence reflects completeness of nutrition facts + serving info.",
        "Educational tool - not medical advice.",
    ]
    if isinstance(bev_meta, dict) and str(bev_meta.get("signal") or "").lower() == "curated":
        notes.append("Beverage locked by curated layer.")

    return {
        "confidence": round(confidence, 2),
        "missing_core_fields": missing,
        "has_serving": bool(has_serving),
        "beverage_detection": bev_meta,
        "notes": notes,
    }



# -------------------------
# Public API used by routes
# -------------------------
_cfg = VitaScoreConfig()


def _scan_error(code: str, message: str, status_code: int) -> Dict[str, Any]:
    return {
        "error": message,
        "error_code": code,
        "status_code": int(status_code),
    }


def _lookup_state_payload(state: str, missing_fields: Optional[List[str]] = None) -> Dict[str, Any]:
    return {
        "lookup_state": state,
        "lookup_missing_fields": list(missing_fields or []),
    }


def _is_supported_lookup_key(value: str) -> bool:
    key = str(value or "").strip()
    if not key:
        return False
    if key.isdigit():
        return len(key) in {8, 12, 13, 14}
    return bool(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{1,63}", key))


def _has_minimum_product_data(normalized: Dict[str, Any]) -> bool:
    name = str(normalized.get("name") or "").strip()
    ingredients = _as_list(normalized.get("ingredients"))
    nutrition = normalized.get("nutrition_per_100") or {}
    if not isinstance(nutrition, dict):
        nutrition = {}
    nutrition_values = [
        nutrition.get("sugar_g"),
        nutrition.get("salt_g"),
        nutrition.get("sat_fat_g"),
        nutrition.get("protein_g"),
    ]
    has_nutrition = any(v is not None for v in nutrition_values)
    return bool(name or ingredients or has_nutrition)


def _lookup_missing_fields(normalized: Dict[str, Any], raw: Optional[Dict[str, Any]] = None) -> List[str]:
    normalized = normalized if isinstance(normalized, dict) else {}
    nutrition = normalized.get("nutrition_per_100") or {}
    if not isinstance(nutrition, dict):
        nutrition = {}
    raw_product = raw.get("product") if isinstance(raw, dict) and isinstance(raw.get("product"), dict) else (raw if isinstance(raw, dict) else {})
    raw_nutriments = raw_product.get("nutriments") if isinstance(raw_product.get("nutriments"), dict) else {}
    ingredients = _as_list(normalized.get("ingredients"))
    categories = normalized.get("categories") or normalized.get("categories_tags") or []
    additives = _as_list(normalized.get("additives_tags")) or _as_list(normalized.get("additives_original_tags"))
    serving_value = _to_float(_get_path(normalized, "serving", "value") or nutrition.get("serving_size"))
    missing = []
    if not str(normalized.get("name") or "").strip():
        missing.append("product_name")
    if not ingredients:
        missing.append("ingredients")
    raw_nutrition_present = any(k in raw_nutriments for k in (
        "sugars_100g", "sugar_100g", "sugars", "sugar",
        "salt_100g", "salt",
        "saturated-fat_100g", "saturated_fat_100g", "saturated-fat", "saturated_fat",
        "proteins_100g", "protein_100g", "proteins", "protein",
        "energy-kcal_100g", "energy-kcal", "energy_100g", "energy"
    ))
    normalized_nutrition_present = any(nutrition.get(k) is not None for k in ("sugar_g", "salt_g", "sat_fat_g", "protein_g", "energy_kcal"))
    if not raw_nutrition_present and not normalized_nutrition_present:
        missing.append("nutriments")
    if serving_value is None:
        missing.append("serving_size")
    if not additives:
        missing.append("additives")
    if isinstance(categories, str):
        categories = [c.strip() for c in categories.split(",") if c.strip()]
    if not categories:
        missing.append("categories")
    return missing


def _manual_ingredients_from_text(text: Any, note: str = "From manual") -> List[Dict[str, Any]]:
    parts = [
        part.strip()
        for part in re.split(r"[,\n;]", str(text or ""))
        if part and str(part).strip()
    ]
    return [{"name": part, "class": "U", "note": note} for part in parts]


def _merge_ingredient_text(existing_ingredients: Any, extracted_text: Any) -> str:
    parts: List[str] = []
    seen = set()

    def _push(value: Any) -> None:
        text = str(value or "").strip()
        if not text:
            return
        key = re.sub(r"\s+", " ", text).strip().lower()
        if not key or key in seen:
            return
        seen.add(key)
        parts.append(text)

    for item in _as_list(existing_ingredients):
        if isinstance(item, dict):
            _push(item.get("name"))
        else:
            _push(item)
    for item in re.split(r"[;,]", str(extracted_text or "")):
        _push(item)
    return ", ".join(parts)


def _merge_categories(existing_categories: Any, extracted_categories: Any) -> List[str]:
    merged: List[str] = []
    seen = set()
    for source in (_as_list(existing_categories), _as_list(extracted_categories)):
        for item in source:
            text = str(item or "").strip()
            if not text:
                continue
            key = text.lower()
            if key in seen:
                continue
            seen.add(key)
            merged.append(text)
    return merged


def _first_present(*values: Any) -> Any:
    for value in values:
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        return value
    return None


def _extract_json_object(text: str) -> Optional[Dict[str, Any]]:
    s = str(text or "").strip()
    if not s:
        return None
    try:
        parsed = json.loads(s)
        return parsed if isinstance(parsed, dict) else None
    except Exception:
        pass
    m = re.search(r"\{[\s\S]*\}", s)
    if not m:
        return None
    try:
        parsed = json.loads(m.group(0))
        return parsed if isinstance(parsed, dict) else None
    except Exception:
        return None


def _responses_output_text(payload: Dict[str, Any]) -> str:
    if isinstance(payload.get("output_text"), str) and payload.get("output_text").strip():
        return str(payload["output_text"])
    chunks: List[str] = []
    for item in _as_list(payload.get("output")):
        if not isinstance(item, dict):
            continue
        for content in _as_list(item.get("content")):
            if not isinstance(content, dict):
                continue
            text = content.get("text") or content.get("output_text")
            if isinstance(text, str) and text.strip():
                chunks.append(text.strip())
    return "\n".join(chunks).strip()


def _photo_extraction_unavailable() -> Dict[str, Any]:
    err = _scan_error("PHOTO_EXTRACTION_UNAVAILABLE", "Photo extraction is not available.", 422)
    err.update(_lookup_state_payload("found_but_incomplete"))
    err["analysis_state"] = "insufficient_data"
    err["analysis_confidence"] = "low"
    return err


async def _extract_photo_payload_with_ai(payload: Dict[str, Any]) -> Dict[str, Any]:
    if not OPENAI_API_KEY:
        return _photo_extraction_unavailable()

    ingredient_image = str(payload.get("ingredient_image_data_url") or "").strip()
    nutrition_image = str(payload.get("nutrition_image_data_url") or "").strip()
    if not ingredient_image and not nutrition_image:
        err = _scan_error("PHOTO_EXTRACTION_FAILED", "Could not extract enough data from the photo.", 422)
        err.update(_lookup_state_payload("found_but_incomplete"))
        err["analysis_state"] = "insufficient_data"
        err["analysis_confidence"] = "low"
        return err

    content: List[Dict[str, Any]] = [
        {
            "type": "input_text",
            "text": (
                "Extract product data from the provided label photos. "
                "Return only a JSON object with keys: "
                "product_name, brand, ingredients_text, categories, "
                "nutrition_per_100 {unit, energy_kcal, sugar_g, salt_g, sat_fat_g, protein_g}, "
                "confidence, extracted_fields, notes. "
                "Use null for unknown values. categories must be an array of short strings. "
                "ingredients_text must be a single cleaned string. confidence must be high, medium, or low. "
                "If the image is unclear, still extract what is visible and note uncertainty."
            ),
        }
    ]
    if ingredient_image:
        content.append({"type": "input_text", "text": "Ingredient label photo:"})
        content.append({"type": "input_image", "image_url": ingredient_image})
    if nutrition_image:
        content.append({"type": "input_text", "text": "Nutrition table photo:"})
        content.append({"type": "input_image", "image_url": nutrition_image})

    body = {
        "model": OPENAI_VISION_MODEL,
        "input": [{"role": "user", "content": content}],
        "max_output_tokens": 900,
    }
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }

    try:
        async with httpx.AsyncClient(timeout=35.0) as client:
            res = await client.post(OPENAI_RESPONSES_URL, headers=headers, json=body)
    except Exception:
        return _photo_extraction_unavailable()

    if res.status_code >= 400:
        return _photo_extraction_unavailable()

    try:
        data = res.json()
    except Exception:
        return _photo_extraction_unavailable()

    parsed = _extract_json_object(_responses_output_text(data))
    if not parsed:
        err = _scan_error("PHOTO_EXTRACTION_FAILED", "Could not extract enough data from the photo.", 422)
        err.update(_lookup_state_payload("found_but_incomplete"))
        err["analysis_state"] = "insufficient_data"
        err["analysis_confidence"] = "low"
        return err
    return parsed


def _analysis_mode(
    *,
    lookup_state: str,
    per100: Dict[str, Optional[float]],
    ingredients: List[Dict[str, Any]],
    ingredients_intelligence: Dict[str, Any],
    categories: Any,
) -> Tuple[str, str]:
    nutriments_present = sum(
        1 for k in ("energy_kcal", "sugar_g", "salt_g", "saturated_fat_g", "protein_g")
        if per100.get(k) is not None
    )
    ingredients_present = bool(_as_list(ingredients))
    categories_present = bool(categories if isinstance(categories, list) else str(categories or "").strip())
    markers = ingredients_intelligence.get("markers") if isinstance(ingredients_intelligence, dict) else {}
    if not isinstance(markers, dict):
        markers = {}
    signal_present = any(
        int(markers.get(k) or 0) > 0
        for k in ("sweeteners", "preservatives", "colorants", "flavourings", "caffeine", "emulsifiers_stabilizers")
    )

    evidence_points = (
        (2 if nutriments_present >= 3 else 1 if nutriments_present >= 1 else 0)
        + (1 if ingredients_present else 0)
        + (1 if categories_present else 0)
        + (1 if signal_present else 0)
    )

    if lookup_state == "found_and_analyzable" and nutriments_present >= 3 and ingredients_present:
        return "full_analysis", "high"
    if evidence_points >= 4:
        return "partial_analysis", "high"
    if evidence_points >= 3:
        return "partial_analysis", "medium"
    if evidence_points >= 2:
        return "partial_analysis", "low"
    return "limited_estimate", "low"


def _conservative_partial_score(score: int, confidence: str) -> int:
    if confidence == "low":
        return int(round(_clamp(score, 25.0, 72.0)))
    if confidence == "medium":
        return int(round(_clamp(score, 18.0, 82.0)))
    return int(round(_clamp(score, 12.0, 90.0)))


def _limited_estimate_score(
    per100: Dict[str, Optional[float]],
    ingredients: List[Dict[str, Any]],
    ingredients_intelligence: Dict[str, Any],
) -> int:
    markers = ingredients_intelligence.get("markers") if isinstance(ingredients_intelligence, dict) else {}
    if not isinstance(markers, dict):
        markers = {}
    score = 50.0
    sugar = _to_float(per100.get("sugar_g"))
    salt = _to_float(per100.get("salt_g"))
    satfat = _to_float(per100.get("saturated_fat_g"))
    if sugar is not None:
        if sugar >= 10:
            score -= 10
        elif sugar >= 5:
            score -= 5
        elif sugar <= 2:
            score += 3
    if salt is not None and salt >= 1:
        score -= 5
    if satfat is not None and satfat >= 5:
        score -= 4
    score -= min(12, Number(markers.get("sweeteners") or 0) * 6) if False else 0
    score -= min(8, float(markers.get("sweeteners") or 0) * 4.0)
    score -= min(6, float(markers.get("colorants") or 0) * 3.0)
    score -= min(6, float(markers.get("preservatives") or 0) * 3.0)
    score -= min(5, float(markers.get("flavourings") or 0) * 2.0)
    score -= min(6, float(markers.get("e_numbers") or 0) * 2.0)
    ingredient_count = len(_as_list(ingredients))
    if ingredient_count and ingredient_count <= 5 and not any(float(markers.get(k) or 0) > 0 for k in ("sweeteners", "colorants", "preservatives", "flavourings")):
        score += 4
    return int(round(_clamp(score, 30.0, 70.0)))


def _fallback_assessment_response(
    *,
    key: str,
    norm: Dict[str, Any],
    raw: Optional[Dict[str, Any]],
    source: Optional[str],
    matched_by: Optional[str],
    lang: str,
    lookup_state: str,
) -> Dict[str, Any]:
    norm = norm if isinstance(norm, dict) else {}
    product_categories = norm.get("categories") or norm.get("categories_tags") or []
    if isinstance(product_categories, str):
      product_categories = [c.strip() for c in product_categories.split(",") if c.strip()]
    ingredients_raw = _as_list(norm.get("ingredients"))
    try:
        is_bev, _bev_meta = _guess_is_beverage(norm)
    except Exception:
        is_bev = bool(_get_path(norm, "meta", "is_beverage"))
    try:
        additives_e_numbers = _collect_additives_tags_from_sources(norm, raw)
        ingredients, ingredients_intelligence = _ingredients_intelligence(
            ingredients_raw,
            is_beverage=is_bev,
            additives_e_numbers=additives_e_numbers,
        )
    except Exception:
        ingredients = ingredients_raw
        ingredients_intelligence = {
            "processing_score": None,
            "processing_label": "",
            "markers": {},
            "flags": [],
            "e_number_details": [],
        }
    try:
        per100 = _nutrients_per_100(norm)
    except Exception:
        per100 = {"energy_kcal": None, "sugar_g": None, "salt_g": None, "saturated_fat_g": None, "fiber_g": None, "protein_g": None, "fruits_veg_percent": None}
    score = _limited_estimate_score(per100, ingredients, ingredients_intelligence)
    balance_adjustments = _traditional_balance_adjustments(norm, per100, ingredients_intelligence, is_beverage=is_bev, lang=lang)
    score += int(balance_adjustments.get("total_delta", 0) or 0)
    floor_adjustments = _whole_food_floor_adjustments(
        norm,
        per100,
        ingredients_intelligence,
        is_beverage=is_bev,
        lang=lang,
        current_score=score,
    )
    floor_score = floor_adjustments.get("floor_score")
    if isinstance(floor_score, int):
        score = max(score, floor_score)
    score = int(round(_clamp(score, 1.0, 100.0)))
    lookup_missing = _lookup_missing_fields(norm, raw)
    qty = norm.get("quantity")
    if isinstance(qty, str) and qty.strip().startswith("0"):
        qty = None
    return {
        "key": key,
        "source": source,
        "matched_by": matched_by,
        "lookup_state": lookup_state,
        "lookup_missing_fields": lookup_missing,
        "analysis_state": "limited_estimate",
        "analysis_confidence": "low",
        "product": {
            "name": norm.get("name") or "Unknown product",
            "brand": norm.get("brand"),
            "image_url": norm.get("image_url"),
            "quantity": qty,
            "categories": product_categories,
            "barcode": key if key and not key.startswith("manual:") else None,
        },
        "alerts": [],
        "ingredients": ingredients,
        "ingredients_intelligence": ingredients_intelligence,
        "nutrition_per_100": {
            "unit": str(per100.get("unit") or _get_path(norm, "nutrition_per_100", "unit") or "g").strip().lower() or "g",
            "sugar_g": per100.get("sugar_g"),
            "salt_g": per100.get("salt_g"),
            "sat_fat_g": per100.get("saturated_fat_g"),
            "protein_g": per100.get("protein_g"),
            "energy_kcal": per100.get("energy_kcal"),
            "serving_size": _to_float(_get_path(norm, "nutrition_per_100", "serving_size") or serving_amount),
        },
        "vitascore": score,
        "vitascore_version": "v3_hybrid_pro",
        "vitascore_breakdown": {
            "model": "v3_hybrid_pro",
            "weights": {"per_100": _cfg.w_per100, "per_serving": _cfg.w_serving},
            "per_100": {"inputs": {
                "sugar_g_per_100": per100.get("sugar_g"),
                "salt_g_per_100": per100.get("salt_g"),
                "saturated_fat_g_per_100": per100.get("saturated_fat_g"),
                "protein_g_per_100": per100.get("protein_g"),
                "energy_kcal_per_100": per100.get("energy_kcal"),
            }},
            "per_serving": {"inputs": {}},
            "hybrid_score": score,
            "who_baseline": {"score": score},
            "balance_adjustments": balance_adjustments,
            "floor_adjustments": floor_adjustments,
            "analysis_mode": {"state": "limited_estimate", "confidence": "low"},
        },
        "why_this_score": [],
        "tips": [],
        "who_impact": None,
        "data_quality": {"confidence": 0.2, "missing_core_fields": lookup_missing, "has_serving": False, "beverage_detection": {"value": _get_path(norm, "meta", "is_beverage"), "signal": "fallback_estimate"}},
        "meta": {
            "is_beverage": bool(_get_path(norm, "meta", "is_beverage")),
            "serving": {"amount": None, "unit": None, "source": "fallback_estimate"},
            "lookup_state": lookup_state,
            "lookup_missing_fields": lookup_missing,
            "analysis_state": "limited_estimate",
            "analysis_confidence": "low",
        },
    }


def _analyze_normalized_product(
    *,
    key: str,
    norm: Dict[str, Any],
    raw: Optional[Dict[str, Any]],
    source: Optional[str],
    matched_by: Optional[str],
    lang: str,
    rasff: List[Dict[str, Any]],
    curated_is_beverage: bool = False,
    curated_beverage_signal: Optional[str] = None,
) -> Dict[str, Any]:
    lookup_missing = _lookup_missing_fields(norm, raw)
    lookup_state = "found_but_incomplete" if lookup_missing else "found_and_analyzable"
    alerts = _collect_alerts(_as_list(rasff), norm)
    ingredients_raw = norm.get("ingredients") or []

    try:
        is_bev, bev_meta = _guess_is_beverage(norm)
        if curated_is_beverage:
            is_bev = True
            bev_meta = {
                "signal": curated_beverage_signal or "curated",
                "value": True,
                "confidence": 0.99,
            }
        serving_amount, serving_unit, serving_note = _serving_size_in_g_or_ml(norm, is_bev)

        additives_e_numbers = _collect_additives_tags_from_sources(norm, raw)
        if source == "local":
            merged_e_numbers = list(additives_e_numbers)
            for additive in (_as_list(norm.get("additives")) or _as_list(raw.get("additives") if isinstance(raw, dict) else [])):
                token = str(additive).strip().upper()
                if token and token not in merged_e_numbers:
                    merged_e_numbers.append(token)
            additives_e_numbers = merged_e_numbers

        ingredients, ingredients_intelligence = _ingredients_intelligence(
            _as_list(ingredients_raw),
            is_beverage=is_bev,
            additives_e_numbers=additives_e_numbers,
        )

        per100 = _nutrients_per_100(norm)
        product_categories = norm.get("categories") or norm.get("categories_tags") or []
        analysis_state, analysis_confidence = _analysis_mode(
            lookup_state=lookup_state,
            per100=per100,
            ingredients=ingredients,
            ingredients_intelligence=ingredients_intelligence,
            categories=product_categories,
        )
        net100, part100 = _score_per100(per100, is_bev, _cfg)
        netS, partS = _score_serving(per100, serving_amount or 100.0, is_bev, _cfg)
        net = (_cfg.w_per100 * net100) + (_cfg.w_serving * netS)
        hybrid_score = _map_net_to_vitascore(net, _cfg)

        breakdown = {
            "model": "v3_hybrid_pro",
            "weights": {"per_100": _cfg.w_per100, "per_serving": _cfg.w_serving},
            "per_100": part100,
            "per_serving": partS,
            "net_hybrid": round(net, 2),
            "hybrid_score": hybrid_score,
        }

        who = _who_sugar_impact(norm, per100, is_bev)
        who_score, who_breakdown = _who_baseline_score(who, per100, is_beverage=is_bev)

        w_who = 0.85 if is_bev else 0.75
        w_hyb = 1.0 - w_who
        base_score = int(round((w_who * who_score) + (w_hyb * hybrid_score)))
        pattern_adjustments = _pattern_score_adjustments(norm, per100, ingredients_intelligence, is_beverage=is_bev)
        balance_adjustments = _traditional_balance_adjustments(norm, per100, ingredients_intelligence, is_beverage=is_bev, lang=lang)
        score = base_score + int(pattern_adjustments.get("total_delta", 0) or 0) + int(balance_adjustments.get("total_delta", 0) or 0)
        score_cap = pattern_adjustments.get("score_cap")
        if isinstance(score_cap, int):
            score = min(score, score_cap)
        score = int(round(_clamp(score, 1.0, 100.0)))
        if analysis_state == "partial_analysis":
            score = _conservative_partial_score(score, analysis_confidence)
        elif analysis_state == "limited_estimate":
            score = _limited_estimate_score(per100, ingredients, ingredients_intelligence)

        breakdown["who_baseline"] = who_breakdown
        breakdown["who_weights"] = {"who": w_who, "hybrid": w_hyb}
        breakdown["pre_pattern_score"] = base_score
        breakdown["pattern_adjustments"] = pattern_adjustments
        breakdown["balance_adjustments"] = balance_adjustments
        floor_adjustments = _whole_food_floor_adjustments(
            norm,
            per100,
            ingredients_intelligence,
            is_beverage=is_bev,
            lang=lang,
            current_score=score,
        )
        floor_score = floor_adjustments.get("floor_score")
        if isinstance(floor_score, int):
            score = max(score, floor_score)
        breakdown["floor_adjustments"] = floor_adjustments
        breakdown["analysis_mode"] = {
            "state": analysis_state,
            "confidence": analysis_confidence,
        }

        why, tips = _build_explanations(per100, breakdown, is_bev, lang=lang)
        dq = _localize_data_quality_notes(_data_quality(norm, per100, bev_meta), lang)
        ingredients_intelligence = _localize_intelligence(ingredients_intelligence, lang)
    except Exception:
        return _fallback_assessment_response(
            key=key,
            norm=norm,
            raw=raw,
            source=source,
            matched_by=matched_by,
            lang=lang,
            lookup_state="found_but_incomplete",
        )

    if isinstance(product_categories, str):
        product_categories = [c.strip() for c in product_categories.split(",") if c.strip()]

    qty = norm.get("quantity")
    if isinstance(qty, str) and qty.strip().startswith("0"):
        qty = None

    product_block = {
        "name": norm.get("name"),
        "brand": norm.get("brand"),
        "image_url": norm.get("image_url"),
        "quantity": qty,
        "categories": product_categories,
        "barcode": key if key and not key.startswith("manual:") else None,
    }

    return {
        "key": key,
        "source": source,
        "matched_by": matched_by,
        "lookup_state": lookup_state,
        "lookup_missing_fields": lookup_missing,
        "analysis_state": analysis_state,
        "analysis_confidence": analysis_confidence,
        "product": product_block,
        "alerts": alerts,
        "ingredients": ingredients,
        "ingredients_intelligence": ingredients_intelligence,
        "nutrition_per_100": {
            "unit": str(per100.get("unit") or _get_path(norm, "nutrition_per_100", "unit") or "g").strip().lower() or "g",
            "sugar_g": per100.get("sugar_g"),
            "salt_g": per100.get("salt_g"),
            "sat_fat_g": per100.get("saturated_fat_g"),
            "protein_g": per100.get("protein_g"),
            "energy_kcal": per100.get("energy_kcal"),
            "serving_size": _to_float(_get_path(norm, "nutrition_per_100", "serving_size")),
        },
        "vitascore": score,
        "vitascore_version": "v3_hybrid_pro",
        "vitascore_breakdown": breakdown,
        "why_this_score": why,
        "tips": tips,
        "who_impact": who,
        "data_quality": dq,
        "meta": {
            "is_beverage": is_bev,
            "serving": {"amount": serving_amount, "unit": serving_unit, "source": serving_note},
            "lookup_state": lookup_state,
            "lookup_missing_fields": lookup_missing,
            "analysis_state": analysis_state,
            "analysis_confidence": analysis_confidence,
        },
    }


async def scan_product(key: str, lang: str = "en") -> Dict[str, Any]:
    lang = lang if lang in SUPPORTED_LANGS else "en"
    key = (key or "").strip()
    if not key:
        err = _scan_error("INVALID_BARCODE", "Missing product id or barcode.", 400)
        err.update(_lookup_state_payload("invalid_barcode"))
        err["analysis_state"] = "insufficient_data"
        err["analysis_confidence"] = "low"
        return err
    if not _is_supported_lookup_key(key):
        err = _scan_error("INVALID_BARCODE", "Invalid barcode.", 400)
        err.update(_lookup_state_payload("invalid_barcode"))
        err["analysis_state"] = "insufficient_data"
        err["analysis_confidence"] = "low"
        return err

    products = _load_json(PRODUCTS_FILE, [])
    if isinstance(products, dict) and isinstance(products.get("products"), list):
        products = products["products"]
    elif not isinstance(products, list):
        products = []
    rasff = _load_json(RASFF_FILE, [])

    matched_by = None
    raw: Optional[Dict[str, Any]] = None
    source: Optional[str] = None
    curated_is_beverage = False
    curated_beverage_signal: Optional[str] = None
    off_error: Optional[Dict[str, Any]] = None

    local = _find_local_product(_as_list(products), key)
    if local:
        raw = local
        source = "local"
        matched_by = "local_db"
        local_serving_unit = str(_get_path(local, "serving_size", "unit") or "").strip().lower()
        local_nutrition_unit = str(_get_path(local, "nutrition_per_100", "unit") or "").strip().lower()
        if local_serving_unit == "ml" or local_nutrition_unit == "ml":
            curated_is_beverage = True
            curated_beverage_signal = "curated"

    if raw is None:
        try:
            off_result = await fetch_off_product(key)
            if off_result.ok and isinstance(off_result.payload, dict):
                raw = off_result.payload
                source = "openfoodfacts"
                matched_by = "barcode_or_key"
            else:
                off_error = {
                    "status": int(off_result.status or 0),
                    "error": str(off_result.error or "").strip(),
                }
        except Exception:
            raw = None
            off_error = {"status": 502, "error": "OpenFoodFacts request failed"}

    if raw is None:
        status = int((off_error or {}).get("status") or 0)
        if status == 404:
            return _fallback_assessment_response(
                key=key,
                norm={"name": "Unknown product", "barcode": key},
                raw=None,
                source="openfoodfacts",
                matched_by="barcode_or_key",
                lang=lang,
                lookup_state="not_found",
            )
        if status == 400:
            err = _scan_error("INVALID_BARCODE", "Invalid barcode.", 400)
            err.update(_lookup_state_payload("invalid_barcode"))
            err["analysis_state"] = "insufficient_data"
            err["analysis_confidence"] = "low"
            return err
        return _fallback_assessment_response(
            key=key,
            norm={"name": "Unknown product", "barcode": key},
            raw=None,
            source="openfoodfacts",
            matched_by="barcode_or_key",
            lang=lang,
            lookup_state="found_but_incomplete",
        )

    try:
        norm = _normalize(raw, source=source)
    except Exception:
        return _fallback_assessment_response(
            key=key,
            norm={"name": "Unknown product", "barcode": key},
            raw=raw if isinstance(raw, dict) else None,
            source=source,
            matched_by=matched_by,
            lang=lang,
            lookup_state="found_but_incomplete",
        )
    if not isinstance(norm, dict) or not norm:
        return _fallback_assessment_response(
            key=key,
            norm={"name": "Unknown product", "barcode": key},
            raw=raw if isinstance(raw, dict) else None,
            source=source,
            matched_by=matched_by,
            lang=lang,
            lookup_state="found_but_incomplete",
        )
    if not _has_minimum_product_data(norm):
        return _fallback_assessment_response(
            key=key,
            norm=norm,
            raw=raw if isinstance(raw, dict) else None,
            source=source,
            matched_by=matched_by,
            lang=lang,
            lookup_state="found_but_incomplete",
        )
    if source == "local":
        curated = raw if isinstance(raw, dict) else {}

        serving_size = norm.get("serving_size")
        if not isinstance(serving_size, dict):
            fallback_serving = curated.get("serving_size")
            serving_size = fallback_serving if isinstance(fallback_serving, dict) else None
        if isinstance(serving_size, dict):
            norm["serving"] = {
                "value": serving_size.get("value"),
                "unit": str(serving_size.get("unit") or "").strip().lower() or None,
            }

        nutrients_per_100 = norm.get("nutrients_per_100")
        if not isinstance(nutrients_per_100, dict):
            fallback_nutrients = curated.get("nutrients_per_100")
            nutrients_per_100 = fallback_nutrients if isinstance(fallback_nutrients, dict) else None
        if isinstance(nutrients_per_100, dict):
            norm["nutriments"] = {
                "per_100": {
                    "energy_kcal": nutrients_per_100.get("energy_kcal"),
                    "sugar_g": nutrients_per_100.get("sugar_g"),
                    "salt_g": nutrients_per_100.get("salt_g"),
                    "saturated_fat_g": nutrients_per_100.get("saturated_fat_g"),
                    "fiber_g": nutrients_per_100.get("fiber_g"),
                    "protein_g": nutrients_per_100.get("protein_g"),
                    "fruits_veg_percent": nutrients_per_100.get("fruits_veg_percent"),
                }
            }

        ingredients_value = norm.get("ingredients")
        if not isinstance(ingredients_value, dict):
            fallback_ingredients = curated.get("ingredients")
            ingredients_value = fallback_ingredients if isinstance(fallback_ingredients, dict) else None
        if isinstance(ingredients_value, dict) and isinstance(ingredients_value.get("text"), str):
            norm["ingredients_meta"] = {
                "language": ingredients_value.get("language"),
                "source_language": ingredients_value.get("source_language"),
            }
            parsed_ingredients = [
                {
                    "name": part.strip(),
                    "class": "Other",
                    "note": "From curated",
                }
                for part in re.split(r"[;,]", ingredients_value.get("text", ""))
                if part.strip()
            ]
            additives_from_curated = _as_list(norm.get("additives")) or _as_list(curated.get("additives"))
            for additive in additives_from_curated:
                additive_name = str(additive).strip()
                if additive_name:
                    parsed_ingredients.append(
                        {
                            "name": additive_name,
                            "class": "E-number",
                            "note": "From curated",
                        }
                    )
            norm["ingredients"] = parsed_ingredients

    return _analyze_normalized_product(
        key=key,
        norm=norm,
        raw=raw if isinstance(raw, dict) else None,
        source=source,
        matched_by=matched_by,
        lang=lang,
        rasff=_as_list(rasff),
        curated_is_beverage=curated_is_beverage,
        curated_beverage_signal=curated_beverage_signal,
    )


async def analyze_manual_product(payload: Dict[str, Any], lang: str = "en") -> Dict[str, Any]:
    lang = lang if lang in SUPPORTED_LANGS else "en"
    payload = payload if isinstance(payload, dict) else {}
    name = str(payload.get("name") or "").strip() or "Manual product"
    brand = str(payload.get("brand") or "").strip() or None
    unit = str(payload.get("unit") or "g").strip().lower()
    unit = "ml" if unit == "ml" else "g"
    ingredients_text = str(payload.get("ingredients_text") or "").strip()
    ingredients = _manual_ingredients_from_text(ingredients_text, note=str(payload.get("ingredients_note") or "From manual"))
    nutrition = {
        "unit": unit,
        "sugar_g": _to_float(payload.get("sugar_g")),
        "salt_g": _to_float(payload.get("salt_g")),
        "sat_fat_g": _to_float(payload.get("sat_fat_g")),
        "protein_g": _to_float(payload.get("protein_g")),
        "serving_size": _to_float(payload.get("serving_size")),
    }
    categories = payload.get("categories") or []
    if isinstance(categories, str):
        categories = [c.strip() for c in categories.split(",") if c.strip()]
    elif not isinstance(categories, list):
        categories = []

    norm = {
        "name": name,
        "brand": brand,
        "image_url": str(payload.get("image_url") or "").strip() or None,
        "quantity": str(payload.get("quantity") or "").strip() or None,
        "categories": categories,
        "categories_tags": [],
        "ingredients": ingredients,
        "nutrition_per_100": nutrition,
        "serving": {
            "value": _to_float(payload.get("serving_size")),
            "unit": unit,
        },
        "meta": {
            "is_beverage": unit == "ml",
        },
    }
    if not _has_minimum_product_data(norm):
        err = _scan_error("MISSING_KEY_DATA", "Key data required for product assessment is missing.", 422)
        err.update(_lookup_state_payload("found_but_incomplete", _lookup_missing_fields(norm, payload if isinstance(payload, dict) else None)))
        err["analysis_state"] = "insufficient_data"
        err["analysis_confidence"] = "low"
        return err

    return _analyze_normalized_product(
        key=f"manual:{re.sub(r'[^0-9]', '', str(payload.get('timestamp') or '')) or 'entry'}",
        norm=norm,
        raw=payload,
        source="manual",
        matched_by="manual_entry",
        lang=lang,
        rasff=[],
    )


async def analyze_photo_product(payload: Dict[str, Any], lang: str = "en") -> Dict[str, Any]:
    lang = lang if lang in SUPPORTED_LANGS else "en"
    payload = payload if isinstance(payload, dict) else {}
    extracted = await _extract_photo_payload_with_ai(payload)
    if isinstance(extracted, dict) and extracted.get("error"):
        return extracted

    existing_analysis = payload.get("existing_analysis") if isinstance(payload.get("existing_analysis"), dict) else {}
    existing_product = payload.get("existing_product") if isinstance(payload.get("existing_product"), dict) else {}
    existing_nutrition = existing_analysis.get("nutrition_per_100") if isinstance(existing_analysis.get("nutrition_per_100"), dict) else {}
    existing_meta = existing_analysis.get("meta") if isinstance(existing_analysis.get("meta"), dict) else {}
    existing_serving = existing_meta.get("serving") if isinstance(existing_meta.get("serving"), dict) else {}
    existing_key = str(existing_analysis.get("key") or payload.get("existing_key") or "").strip()
    existing_lookup_state = str(existing_analysis.get("lookup_state") or existing_meta.get("lookup_state") or "").strip()
    existing_missing = existing_analysis.get("lookup_missing_fields") if isinstance(existing_analysis.get("lookup_missing_fields"), list) else existing_meta.get("lookup_missing_fields")
    existing_ingredients = existing_analysis.get("ingredients") if isinstance(existing_analysis.get("ingredients"), list) else []

    nutrition = extracted.get("nutrition_per_100") if isinstance(extracted.get("nutrition_per_100"), dict) else {}
    merged_payload = {
        "name": str(_first_present(extracted.get("product_name"), payload.get("name"), payload.get("product_name"), existing_product.get("name")) or "").strip(),
        "brand": str(_first_present(extracted.get("brand"), payload.get("brand"), existing_product.get("brand")) or "").strip(),
        "ingredients_text": _merge_ingredient_text(existing_ingredients, _first_present(extracted.get("ingredients_text"), payload.get("ingredients_text"))),
        "ingredients_note": "From photo",
        "sugar_g": _first_present(nutrition.get("sugar_g"), existing_nutrition.get("sugar_g")),
        "salt_g": _first_present(nutrition.get("salt_g"), existing_nutrition.get("salt_g")),
        "sat_fat_g": _first_present(nutrition.get("sat_fat_g"), existing_nutrition.get("sat_fat_g")),
        "protein_g": _first_present(nutrition.get("protein_g"), existing_nutrition.get("protein_g")),
        "unit": str(_first_present(nutrition.get("unit"), payload.get("unit"), existing_nutrition.get("unit"), existing_serving.get("unit"), "g") or "g").strip().lower() or "g",
        "categories": _merge_categories(existing_product.get("categories") or payload.get("categories") or [], extracted.get("categories") or []),
        "timestamp": payload.get("timestamp"),
        "quantity": _first_present(payload.get("quantity"), existing_product.get("quantity")),
        "serving_size": _first_present(nutrition.get("serving_size"), existing_serving.get("amount")),
        "image_url": _first_present(payload.get("image_url"), existing_product.get("image_url")),
    }

    if not merged_payload["name"]:
        existing = existing_product
        merged_payload["name"] = str(existing.get("name") or "").strip()
        if not merged_payload["brand"]:
            merged_payload["brand"] = str(existing.get("brand") or "").strip()
        if not merged_payload["categories"]:
            merged_payload["categories"] = existing.get("categories") or []

    result = await analyze_manual_product(merged_payload, lang=lang)
    if isinstance(result, dict) and not result.get("error"):
        result["key"] = existing_key or result.get("key")
        result["source"] = existing_analysis.get("source") or "photo"
        result["matched_by"] = "photo_enrichment"
        if existing_lookup_state:
            result["lookup_state"] = existing_lookup_state
        if isinstance(existing_missing, list) and existing_missing:
            result["lookup_missing_fields"] = list(dict.fromkeys([*existing_missing, *(_as_list(result.get("lookup_missing_fields")))]))
        if isinstance(result.get("product"), dict):
            if existing_product.get("name") and not str(result["product"].get("name") or "").strip():
                result["product"]["name"] = str(existing_product.get("name"))
            if existing_product.get("brand") and not str(result["product"].get("brand") or "").strip():
                result["product"]["brand"] = str(existing_product.get("brand"))
            if existing_product.get("image_url") and not str(result["product"].get("image_url") or "").strip():
                result["product"]["image_url"] = str(existing_product.get("image_url"))
            if existing_product.get("quantity") and not str(result["product"].get("quantity") or "").strip():
                result["product"]["quantity"] = str(existing_product.get("quantity"))
            if existing_key and not str(result["product"].get("barcode") or "").strip():
                result["product"]["barcode"] = existing_key
        result["photo_extraction"] = {
            "confidence": str(extracted.get("confidence") or "low").strip().lower() or "low",
            "extracted_fields": _as_list(extracted.get("extracted_fields")),
            "notes": str(extracted.get("notes") or "").strip(),
            "used_ingredient_photo": bool(str(payload.get("ingredient_image_data_url") or "").strip()),
            "used_nutrition_photo": bool(str(payload.get("nutrition_image_data_url") or "").strip()),
        }
        if isinstance(result.get("meta"), dict):
            result["meta"]["photo_extraction"] = result["photo_extraction"]
            if existing_lookup_state:
                result["meta"]["lookup_state"] = existing_lookup_state
            if isinstance(result.get("lookup_missing_fields"), list):
                result["meta"]["lookup_missing_fields"] = result["lookup_missing_fields"]
    return result
