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
import copy
import logging
import time
import asyncio
import html
import xml.etree.ElementTree as ET
from urllib.parse import urljoin
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import httpx

from app.services.openfoodfacts_service import fetch_off_product
from app.services.product_normalizer import is_placeholder_product_name, normalize_product

logger = logging.getLogger("noesisfood.scan")

# -------------------------
# Paths / local data
# -------------------------
APP_DIR = Path(__file__).resolve().parent  # app/services/
DATA_DIR = APP_DIR.parent / "data"         # app/data/

PRODUCTS_FILE = DATA_DIR / "products.json"
RASFF_FILE = DATA_DIR / "rasff.json"
SAFETY_ALERTS_FILE = DATA_DIR / "rasff_alerts.json"
PRODUCT_ENRICHMENTS_FILE = DATA_DIR / "product_enrichments.json"

_JSON_CACHE: Dict[str, Dict[str, Any]] = {}
_SCAN_RESULT_CACHE: Dict[str, Dict[str, Any]] = {}
_SCAN_RESULT_CACHE_TTL_SEC = 10 * 60
_SAFETY_LOOKUP_CACHE: Dict[str, Dict[str, Any]] = {}
_SAFETY_LOOKUP_CACHE_TTL_SEC = 10 * 60
_SAFETY_HTTP_CACHE: Dict[str, Dict[str, Any]] = {}
_SAFETY_HTTP_CACHE_TTL_SEC = 15 * 60

LEBENSMITTELWARNUNG_FEED_URL = "https://www.lebensmittelwarnung.de/___LMW-Redaktion/RSSNewsfeed/Functions/RssFeeds/rssnewsfeed_Alle_DE.xml?nn=314268"
RASFF_PUBLIC_API_URL = "https://api.datalake.sante.service.ec.europa.eu/rasff/irasff-general-info-view"
RASFF_PUBLIC_SEARCH_URL = "https://webgate.ec.europa.eu/rasff-window/screen/search"
RASFF_PUBLIC_API_VERSION = "v1.1"
RASFF_LOOKBACK_DAYS = 240
RASFF_MAX_PAGES = 3
EFET_SOURCE_KEY = "efet_gr"
EFET_SOURCE_LABEL = "EFET"
EFET_BASE_URL = "https://www.efet.gr/"
EFET_RECALL_LISTING_URLS = [
    EFET_BASE_URL,
    "https://www.efet.gr/index.php/el/enimerosi/deltia-typou/anakleiseis-cat",
]
EFET_LOOKBACK_DAYS = 240
EFET_MAX_DISCOVERY_PAGES = 2
EFET_MAX_DETAIL_PAGES = 10


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
        path_str = str(path)
        if path.exists():
            stat = path.stat()
            cache_entry = _JSON_CACHE.get(path_str)
            mtime_ns = int(getattr(stat, "st_mtime_ns", 0))
            size = int(getattr(stat, "st_size", 0))
            if cache_entry and cache_entry.get("mtime_ns") == mtime_ns and cache_entry.get("size") == size:
                return copy.deepcopy(cache_entry.get("data"))
            data = json.loads(path.read_text(encoding="utf-8"))
            _JSON_CACHE[path_str] = {
                "mtime_ns": mtime_ns,
                "size": size,
                "data": data,
            }
            return copy.deepcopy(data)
    except Exception:
        pass
    return default


def _save_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    serialized = json.dumps(data, ensure_ascii=False, indent=2)
    tmp_path = path.with_suffix(f"{path.suffix}.tmp")
    tmp_path.write_text(serialized, encoding="utf-8")
    tmp_path.replace(path)
    _JSON_CACHE[str(path)] = {
        "mtime_ns": int(path.stat().st_mtime_ns),
        "size": int(path.stat().st_size),
        "data": copy.deepcopy(data),
    }


def _cache_key_scan_result(key: str, lang: str) -> str:
    return f"{str(lang or 'en').strip().lower()}::{str(key or '').strip()}"


def _scan_result_cache_get(key: str, lang: str) -> Optional[Dict[str, Any]]:
    cache_key = _cache_key_scan_result(key, lang)
    item = _SCAN_RESULT_CACHE.get(cache_key)
    if not item:
        return None
    if (time.perf_counter() - float(item.get("ts") or 0.0)) > _SCAN_RESULT_CACHE_TTL_SEC:
        _SCAN_RESULT_CACHE.pop(cache_key, None)
        return None
    return copy.deepcopy(item.get("data"))


def _scan_result_cache_set(key: str, lang: str, data: Dict[str, Any]) -> None:
    if not isinstance(data, dict):
        return
    cache_key = _cache_key_scan_result(key, lang)
    _SCAN_RESULT_CACHE[cache_key] = {
        "ts": time.perf_counter(),
        "data": copy.deepcopy(data),
    }


def _safety_lookup_cache_get(cache_key: str) -> Optional[Dict[str, Any]]:
    item = _SAFETY_LOOKUP_CACHE.get(cache_key)
    if not item:
        return None
    if (time.perf_counter() - float(item.get("ts") or 0.0)) > _SAFETY_LOOKUP_CACHE_TTL_SEC:
        _SAFETY_LOOKUP_CACHE.pop(cache_key, None)
        return None
    return copy.deepcopy(item.get("data"))


def _safety_lookup_cache_set(cache_key: str, data: Dict[str, Any]) -> None:
    _SAFETY_LOOKUP_CACHE[cache_key] = {
        "ts": time.perf_counter(),
        "data": copy.deepcopy(data),
    }


def _safety_http_cache_get(cache_key: str) -> Optional[str]:
    item = _SAFETY_HTTP_CACHE.get(cache_key)
    if not item:
        return None
    if (time.perf_counter() - float(item.get("ts") or 0.0)) > _SAFETY_HTTP_CACHE_TTL_SEC:
        _SAFETY_HTTP_CACHE.pop(cache_key, None)
        return None
    return str(item.get("text") or "")


def _safety_http_cache_set(cache_key: str, text: str) -> None:
    _SAFETY_HTTP_CACHE[cache_key] = {
        "ts": time.perf_counter(),
        "text": str(text or ""),
    }


def _new_safety_observability() -> Dict[str, Any]:
    return {
        "source_checked": {},
        "source_matched": {},
        "confidence_assigned": {},
        "batch_scope_explicit": 0,
        "duplicate_collapsed": 0,
        "fallback_used": False,
        "fetch_count": {},
        "page_count": {},
        "no_match_reason": {},
    }


def _bump_observability_bucket(observability: Dict[str, Any], field: str, key: str, amount: int = 1) -> None:
    if not isinstance(observability, dict):
        return
    bucket = observability.get(field)
    if not isinstance(bucket, dict):
        bucket = {}
        observability[field] = bucket
    norm_key = str(key or "").strip()
    if not norm_key:
        return
    bucket[norm_key] = int(bucket.get(norm_key) or 0) + int(amount)


def _set_no_match_reason(observability: Dict[str, Any], source: str, reason: str) -> None:
    if not isinstance(observability, dict):
        return
    bucket = observability.get("no_match_reason")
    if not isinstance(bucket, dict):
        bucket = {}
        observability["no_match_reason"] = bucket
    source_key = str(source or "").strip()
    reason_key = str(reason or "").strip()
    if source_key and reason_key and source_key not in bucket:
        bucket[source_key] = reason_key


def _merge_safety_observability(*items: Any) -> Dict[str, Any]:
    merged = _new_safety_observability()
    for item in items:
        if not isinstance(item, dict):
            continue
        for field in ("source_checked", "source_matched", "confidence_assigned", "fetch_count", "page_count", "no_match_reason"):
            bucket = item.get(field)
            if not isinstance(bucket, dict):
                continue
            target = merged.get(field)
            if not isinstance(target, dict):
                target = {}
                merged[field] = target
            if field == "no_match_reason":
                for key, value in bucket.items():
                    key_str = str(key or "").strip()
                    if key_str and key_str not in target:
                        target[key_str] = str(value or "").strip()
                continue
            for key, value in bucket.items():
                key_str = str(key or "").strip()
                if not key_str:
                    continue
                target[key_str] = int(target.get(key_str) or 0) + int(value or 0)
        merged["batch_scope_explicit"] += int(item.get("batch_scope_explicit") or 0)
        merged["duplicate_collapsed"] += int(item.get("duplicate_collapsed") or 0)
        merged["fallback_used"] = bool(merged.get("fallback_used")) or bool(item.get("fallback_used"))
    return merged


def _attach_scan_timing(data: Dict[str, Any], timing: Dict[str, Any]) -> Dict[str, Any]:
    payload = copy.deepcopy(data) if isinstance(data, dict) else {}
    meta = payload.get("meta")
    if not isinstance(meta, dict):
        meta = {}
        payload["meta"] = meta
    meta["performance"] = timing
    return _attach_scan_resolution_metadata(payload, timing)


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
    solid_food_markers = [
        "vegetable", "vegetables", "tomato", "tomatoes", "legume", "legumes", "bean", "beans",
        "lentil", "lentils", "chickpea", "chickpeas", "nut", "nuts", "seed", "seeds",
        "grain", "grains", "oat", "oats", "pasta", "rice", "cheese", "yogurt", "yoghurt",
        "fruit", "fruits", "canned vegetables", "peeled tomatoes",
        "en:vegetables", "en:tomatoes", "en:nuts", "en:seeds", "en:legumes", "en:cereal-pastas",
        "en:pastas", "en:shelled-nuts", "en:canned-vegetables", "en:peeled-tomatoes",
    ]
    marker_hit = any(m in categories_s for m in beverage_markers)
    solid_hit = any(m in categories_s for m in solid_food_markers)

    serving_unit = str(_get_path(normalized, "serving", "unit") or _get_path(normalized, "nutrition_per_100", "unit") or "").lower()
    serving_value = _to_float(_get_path(normalized, "serving", "value") or _get_path(normalized, "nutrition_per_100", "serving_size"))

    unit_hit = serving_unit in {"ml", "cl", "l"}
    confidence = 0.40
    signals = []
    if marker_hit:
        confidence += 0.35
        signals.append("category_marker")
    if solid_hit and not unit_hit:
        confidence -= 0.35
        signals.append("solid_food_marker")
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
_TRADITIONAL_CHEESE_MARKERS = [
    "feta", "φέτα", "φετα", "sheep cheese", "goat cheese", "greek cheese", "white cheese", "brined cheese",
    "traditional cheese", "fromage grec", "schafskäse",
]
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

_CONFECTIONERY_MARKERS = [
    "chocolate", "milk chocolate", "white chocolate", "wafer", "waffel", "biscuit", "cookie", "cream filling",
    "filled chocolate", "strawberry filling", "hazelnut cream", "candy", "confectionery",
    "σοκολάτα", "γκοφρέτα", "μπισκότο",
]

_CLEAN_WATER_MARKERS = [
    "water", "mineral water", "natural mineral water", "sparkling water", "carbonated water",
    "wasser", "mineralwasser", "mineral water with carbonation", "mit kohlensäure",
    "eau", "eau minérale", "eau minerale", "eau gazeuse",
    "νερό", "μεταλλικό νερό", "ανθρακούχο νερό",
]

_CHEESE_EXCLUSION_MARKERS = [
    "processed cheese", "cheese spread", "spreadable cheese", "analogue cheese", "processed cheese product",
    "cream cheese spread", "fromage fondu", "fromage à tartiner", "schmelzkäse", "schmelzkase",
    "τυρί κρέμα", "τυρι κρεμα", "άλειμμα τυριού", "αλειμμα τυριου",
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

_INGREDIENT_HEADING_MARKERS = {
    "zutaten", "ingredients", "ingredient", "ingrédients", "ingredienti", "ingredientes",
    "συστατικά", "składniki", "skladniki",
}

_INGREDIENT_NOISE_MARKERS = [
    "rainforest alliance", "rainforest allianz", "alliance-zertifiziert", "allianz-zertifiziert",
    "certified", "zertifiziert", "certifié", "πιστοποιη", "certification", "sustainable sourcing",
    "mehr unter", "more at", "learn more", "see more", "visit", "www.", ".com", ".org", ".de", ".fr",
    "ra.org", "fairtrade", "utz", "recycling", "packaging", "label", "consumer service",
    "service client", "kundendienst", "hotline",
    "gefüllt mit", "filled with", "avec", "mit haselnussgeschmack", "with hazelnut flavour",
    "waffelröll", "waffelroll", "wafer roll", "wafer rolls", "geschmack", "bodenhaltung",
]

_INGREDIENT_FRAGMENT_EXACT = {
    "mager", "magerm", "kulör", "kuloer", "fettarmer kakao", "fettarmer", "fettarm",
}

def _ingredient_confidence_text(value: str) -> str:
    text = _norm_ing_text(value).lower()
    text = re.sub(r"[\(\)\[\]\{\}:]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text

def _is_noisy_ingredient_text(value: str) -> bool:
    raw = _norm_ing_text(value)
    if not raw:
        return True

    tl = _ingredient_confidence_text(raw)
    if not tl:
        return True

    if tl in _INGREDIENT_HEADING_MARKERS:
        return True

    if re.search(r"(https?://|www\.|[a-z0-9-]+\.(com|org|de|fr|gr|eu)\b)", tl):
        return True

    if any(marker in tl for marker in _INGREDIENT_NOISE_MARKERS):
        return True

    if tl in _INGREDIENT_FRAGMENT_EXACT:
        return True

    if re.fullmatch(r"(zutaten|ingredients?|ingrédients|συστατικά|składniki)\s*[:\-]?", tl):
        return True

    if re.match(r"^(von|mit|für|pour|with)\b", tl):
        return True

    if re.search(r"\b(aus|from|de|d’|des)\b", tl) and len(tl.split()) >= 4 and not re.search(r"\be\s?\d{3,4}[a-z]?\b", tl):
        return True

    if re.search(r"\b(geschmack|flavour|flavor|saveur)\b", tl):
        return True

    if re.match(r"^\d+(?:[.,]\d+)?\s+[a-zà-ÿäöüß-]+(?:\s+[a-zà-ÿäöüß-]+){0,2}$", tl):
        if any(token in tl for token in ["fettarm", "mager", "kakao", "fat", "gras"]):
            return True

    # Likely wrapper or sentence fragment rather than composition.
    if (
        len(tl) >= 45
        and len(tl.split()) >= 6
        and not re.search(r"\be\s?\d{3,4}[a-z]?\b", tl)
        and not any(ch in raw for ch in ",;%")
    ):
        return True

    # Product-description style fragments are usually sentence-like and not ingredient-like.
    if any(phrase in tl for phrase in [
        "certified cocoa", "source of", "rich in", "with a touch of", "made with",
        "ideal for", "perfect for", "qualité", "qualität", "ποιότητα",
    ]):
        return True

    return False

def _sanitize_ingredient_candidate(name: str) -> str:
    text = _norm_ing_text(name)
    text = re.sub(r"^(zutaten|ingredients?|ingrédients|ingredientes|ingredienti|συστατικά|składniki)\s*[:\-]\s*", "", text, flags=re.I)
    text = re.sub(r"^[\-–—•\s]+", "", text)
    text = _norm_ing_text(text)
    return text

def _sanitize_ingredients_minimal(ingredients: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    seen_names: set[str] = set()
    for ing in ingredients or []:
        raw_name = ing.get("name") if isinstance(ing, dict) else str(ing)
        name = _sanitize_ingredient_candidate(str(raw_name or ""))
        if not name or _is_noisy_ingredient_text(name):
            continue
        name_key = _ingredient_confidence_text(name)
        if not name_key or name_key in seen_names:
            continue
        seen_names.add(name_key)
        item = dict(ing) if isinstance(ing, dict) else {"name": name}
        item["name"] = name
        out.append(item)
    return out


def _normalize_safety_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def _normalize_match_text(value: Any) -> str:
    text = _normalize_safety_text(value).lower()
    text = html.unescape(text)
    text = re.sub(r"[\u2018\u2019\u201a\u201c\u201d\u201e'`´]", "", text)
    text = re.sub(r"[^a-z0-9à-ÿäöüßα-ωάέήίόύώϊϋΐΰ\s-]", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _match_tokens(value: Any) -> List[str]:
    text = _normalize_match_text(value)
    stopwords = {
        "and", "mit", "und", "avec", "pour", "der", "die", "das", "des", "les", "the",
        "de", "du", "la", "le", "του", "της", "των", "και", "με", "von", "pour", "aux",
    }
    tokens: List[str] = []
    for token in text.split():
        if len(token) < 4:
            continue
        if token in stopwords:
            continue
        tokens.append(token)
    return tokens


def _dedupe_tokens(tokens: List[str]) -> List[str]:
    seen: List[str] = []
    for token in tokens:
        token = str(token or "").strip()
        if token and token not in seen:
            seen.append(token)
    return seen


def _product_name_variants(value: Any) -> List[str]:
    raw = _normalize_safety_text(value)
    if not raw:
        return []
    variants = [raw]
    simplified = re.sub(r"(?i)\b(?:unofficial translation|translation)\b.*$", "", raw).strip(" -:/")
    if simplified and simplified not in variants:
        variants.append(simplified)
    for part in re.split(r"\s*(?:/|///|\|\||\*\*\*)\s*", simplified or raw):
        part = _normalize_safety_text(part)
        if part and part not in variants:
            variants.append(part)
    no_parens = re.sub(r"\([^)]*\)", " ", simplified or raw)
    no_parens = _normalize_safety_text(no_parens)
    if no_parens and no_parens not in variants:
        variants.append(no_parens)
    normalized_variants: List[str] = []
    for item in variants:
        norm = _normalize_match_text(item)
        if norm:
            normalized_variants.append(norm)
    return _dedupe_tokens(normalized_variants)


def _token_overlap_ratio(left: List[str], right: List[str]) -> float:
    left_set = set([token for token in left if token])
    right_set = set([token for token in right if token])
    if not left_set or not right_set:
        return 0.0
    return float(len(left_set.intersection(right_set))) / float(max(1, min(len(left_set), len(right_set))))


def _strip_html_to_text(value: str) -> str:
    text = html.unescape(str(value or ""))
    text = re.sub(r"(?is)<script.*?</script>", " ", text)
    text = re.sub(r"(?is)<style.*?</style>", " ", text)
    text = re.sub(r"(?s)<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _extract_html_tag_text(html_text: str, tag: str) -> str:
    match = re.search(rf"(?is)<{tag}\b[^>]*>(.*?)</{tag}>", str(html_text or ""))
    return _strip_html_to_text(match.group(1)) if match else ""


def _extract_html_meta_content(html_text: str, attr_name: str, attr_value: str) -> str:
    pattern = rf'(?is)<meta\b[^>]*{attr_name}\s*=\s*["\']{re.escape(attr_value)}["\'][^>]*content\s*=\s*["\']([^"\']+)["\']'
    match = re.search(pattern, str(html_text or ""))
    if match:
        return _normalize_safety_text(html.unescape(match.group(1)))
    pattern = rf'(?is)<meta\b[^>]*content\s*=\s*["\']([^"\']+)["\'][^>]*{attr_name}\s*=\s*["\']{re.escape(attr_value)}["\']'
    match = re.search(pattern, str(html_text or ""))
    if match:
        return _normalize_safety_text(html.unescape(match.group(1)))
    return ""


def _extract_efet_listing_links(html_text: str) -> List[str]:
    source = str(html_text or "")
    hits: List[str] = []
    for match in re.findall(r'(?is)href\s*=\s*["\']([^"\']*anakleiseis-cat/item/[^"\']+)["\']', source):
        absolute = urljoin(EFET_BASE_URL, html.unescape(match))
        if absolute not in hits:
            hits.append(absolute)
    return hits


def _parse_safety_published_ts(value: str) -> Optional[float]:
    normalized = _normalize_safety_text(value)
    if not normalized:
        return None
    candidates = [
        normalized,
        normalized.rstrip("Z"),
        normalized[:19] if "T" in normalized and len(normalized) >= 19 else normalized,
    ]
    for candidate in candidates:
        for fmt in (
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d",
            "%d/%m/%Y",
            "%d-%m-%Y",
            "%d.%m.%Y",
            "%a, %d %b %Y %H:%M:%S %z",
        ):
            try:
                return time.mktime(time.strptime(candidate, fmt))
            except Exception:
                continue
    return None


def _extract_efet_product_name(title: str, text_blob: str) -> str:
    raw_title = _normalize_safety_text(title)
    body = _normalize_safety_text(text_blob)
    cleaned_title = re.sub(
        r"(?i)^\s*(?:δελτίο τύπου\s*[-:|]?\s*)?(?:ανάκληση(?:\s+προϊόν(?:τος|των)?)?|recall of products?|product recall)\s*[-:|]?\s*",
        "",
        raw_title,
    ).strip(" -:/")
    if cleaned_title and cleaned_title != raw_title:
        return cleaned_title
    for pattern in (
        r"(?i)(?:ανάκληση|recall of)\s+(?:του|της|των|the)?\s*προϊόν(?:τος|των)?\s+([A-ZΑ-Ω0-9][^.,;\n]{4,120})",
        r"(?i)(?:προϊόν|product)\s+([A-ZΑ-Ω0-9][^.,;\n]{4,120})",
    ):
        match = re.search(pattern, body)
        if match:
            candidate = _normalize_safety_text(match.group(1)).strip(" -:/")
            if candidate:
                return candidate
    return raw_title


def _extract_efet_reference(text_blob: str) -> Optional[str]:
    normalized = _normalize_safety_text(text_blob)
    for pattern in (
        r"(?i)\bRASFF\b[^0-9]{0,12}([0-9]{4}\.[0-9]{3,5})",
        r"(?i)\breference\b\s*[:#]?\s*([0-9]{4}\.[0-9]{3,5})",
    ):
        match = re.search(pattern, normalized)
        if match:
            return _normalize_safety_text(match.group(1))
    return None


def _extract_efet_company(text_blob: str) -> Optional[str]:
    normalized = _normalize_safety_text(text_blob)
    best_candidate: Optional[str] = None
    for pattern in (
        r"(?i)(?:εταιρείας|εταιρίας|company|distributor|producer|παραγωγός|διανομέας)\s+([A-ZΑ-Ω0-9][^,;\n]{2,80}?)(?=\s+(?:ανακαλεί|recalls|withdraws|που)\b|[,;]|$)",
        r"(?i)(?:με την εμπορική επωνυμία|sold by|marketed by)\s+([A-ZΑ-Ω0-9][^.,;\n]{2,80})",
        r"(?i)(?:εταιρεία|επιχείρηση|company)\s+([A-ZΑ-Ω0-9][A-ZΑ-Ω0-9&.,'’`\-\s]{2,80}?)(?=\s+(?:ανακαλεί|recalls|withdraws)\b|[.,;]|$)",
    ):
        match = re.search(pattern, normalized)
        if match:
            candidate = _normalize_safety_text(match.group(1)).strip(" -:/")
            if candidate and (best_candidate is None or len(candidate) > len(best_candidate)):
                best_candidate = candidate
    return best_candidate


def _extract_efet_packaging(text_blob: str) -> Optional[str]:
    normalized = _normalize_safety_text(text_blob)
    match = re.search(r"(?i)\b\d+(?:[.,]\d+)?\s*(?:g|gr|kg|ml|l|lt|τεμ(?:άχια)?|τμχ)\b", normalized)
    return _normalize_safety_text(match.group(0)) if match else None


def _extract_efet_hazard_reason(text_blob: str) -> Optional[str]:
    normalized = _normalize_safety_text(text_blob)
    for pattern in (
        r"(?i)(?:λόγω|due to|because of)\s+([^.;]{6,160})",
        r"(?i)(?:μη ασφαλές|hazard|risk|κίνδυνος|reason)\s*[:\-]?\s*([^.;]{6,160})",
        r"(?i)(?:παρουσίας|presence of)\s+([^.;]{6,160})",
    ):
        match = re.search(pattern, normalized)
        if match:
            candidate = _normalize_safety_text(match.group(1)).strip(" -:/")
            if candidate:
                return candidate
    return None


def _normalize_official_overlap_key(
    source: str,
    product_name: Any,
    batch: Any = None,
    lot: Any = None,
    best_before: Any = None,
    packaging: Any = None,
    reference: Any = None,
) -> Optional[str]:
    source_key = str(source or "").strip().lower()
    if source_key not in {"rasff_dg_sante_api", EFET_SOURCE_KEY}:
        return None
    variants = _product_name_variants(product_name)
    product_key = max(variants, key=len) if variants else _normalize_match_text(product_name)
    if not product_key:
        return None
    scope_key = next((
        item for item in [
            _normalize_match_text(reference).replace(".", " "),
            _normalize_match_text(batch),
            _normalize_match_text(lot),
            _normalize_match_text(best_before),
            _normalize_match_text(packaging),
        ]
        if item
    ), "")
    return f"{product_key}::{scope_key}"


def _alerts_likely_same_official_event(existing: Dict[str, Any], candidate: Dict[str, Any]) -> bool:
    existing_key = _normalize_safety_text(existing.get("overlap_key")).lower()
    candidate_key = _normalize_safety_text(candidate.get("overlap_key")).lower()
    if not existing_key or not candidate_key:
        return False
    if existing_key != candidate_key:
        return False
    if existing_key.endswith("::"):
        return False
    return True


def _normalize_efet_entry(url: str, html_text: str) -> Optional[Dict[str, Any]]:
    title = (
        _extract_html_meta_content(html_text, "property", "og:title")
        or _extract_html_tag_text(html_text, "h1")
        or _extract_html_tag_text(html_text, "title")
    )
    text_blob = _strip_html_to_text(html_text)
    if not title and not text_blob:
        return None
    published_at = (
        _extract_html_meta_content(html_text, "property", "article:published_time")
        or _extract_html_meta_content(html_text, "name", "publish-date")
    )
    if not published_at:
        date_match = re.search(r"\b(\d{2}[/-]\d{2}[/-]\d{4}|\d{4}-\d{2}-\d{2})\b", text_blob)
        if date_match:
            published_at = date_match.group(1)
    product_name = _extract_efet_product_name(title, text_blob)
    scope_details = _extract_safety_scope_details(text_blob)
    hazard = _extract_efet_hazard_reason(text_blob)
    company = _extract_efet_company(text_blob)
    packaging = _extract_efet_packaging(text_blob)
    reference = _extract_efet_reference(text_blob)
    summary_parts = [
        company or "",
        packaging or "",
        hazard or "",
        f"Best before: {scope_details.get('best_before')}" if scope_details.get("best_before") else "",
    ]
    summary = " | ".join([part for part in summary_parts if part])
    return {
        "title": _normalize_safety_text(title or product_name or "EFET recall"),
        "summary": _normalize_safety_text(summary or hazard or text_blob[:320]),
        "url": url,
        "published_at": _normalize_safety_text(published_at) or None,
        "product_name": _normalize_safety_text(product_name),
        "product_variants": _product_name_variants(product_name or title),
        "company": company,
        "packaging": packaging,
        "hazard": hazard,
        "reference": reference,
        "batch_specific": bool(scope_details.get("batch_specific")),
        "batch": scope_details.get("batch"),
        "lot": scope_details.get("lot"),
        "best_before": scope_details.get("best_before"),
        "barcodes": _extract_alert_barcodes(text_blob),
        "text_blob": text_blob,
        "overlap_key": _normalize_official_overlap_key(
            EFET_SOURCE_KEY,
            product_name,
            scope_details.get("batch"),
            scope_details.get("lot"),
            scope_details.get("best_before"),
            packaging,
            reference,
        ),
        "source": EFET_SOURCE_KEY,
        "source_label": EFET_SOURCE_LABEL,
    }


def _normalize_safety_alert_entries(raw: Any) -> List[Dict[str, Any]]:
    entries: List[Dict[str, Any]] = []
    if isinstance(raw, dict):
        for barcode, value in raw.items():
            if isinstance(value, list):
                for item in value:
                    entries.append({
                        "barcode": str(barcode).strip(),
                        "title": _normalize_safety_text(item),
                        "source": "local_rasff_alert_index",
                        "severity": "medium",
                        "scope": "product",
                        "batch_specific": False,
                    })
            elif isinstance(value, dict):
                entry = dict(value)
                entry.setdefault("barcode", str(barcode).strip())
                entries.append(entry)
    elif isinstance(raw, list):
        for item in raw:
            if isinstance(item, dict):
                entries.append(dict(item))
    return entries


def _safety_match_score(entry: Dict[str, Any], barcode: str, product_name: str, brand: str) -> int:
    score = 0
    barcode = _normalize_safety_text(barcode).lower()
    product_name = _normalize_safety_text(product_name).lower()
    brand = _normalize_safety_text(brand).lower()

    entry_codes = {
        _normalize_safety_text(entry.get("barcode")).lower(),
        _normalize_safety_text(entry.get("gtin")).lower(),
        _normalize_safety_text(entry.get("off_code")).lower(),
        _normalize_safety_text(entry.get("code")).lower(),
    }
    entry_codes = {code for code in entry_codes if code}
    if barcode and barcode in entry_codes:
        score += 100

    entry_brand = _normalize_safety_text(entry.get("brand")).lower()
    if brand and entry_brand and brand == entry_brand:
        score += 15

    entry_name = _normalize_safety_text(entry.get("product_name") or entry.get("name") or entry.get("title")).lower()
    if product_name and entry_name:
        if product_name == entry_name:
            score += 30
        elif product_name in entry_name or entry_name in product_name:
            score += 20

    keyword = _normalize_safety_text(entry.get("keyword")).lower()
    if keyword and ((product_name and keyword in product_name) or (brand and keyword in brand)):
        score += 10

    return score


def _build_local_safety_lookup_payload(key: str, norm: Dict[str, Any]) -> Dict[str, Any]:
    checked = False
    source_name = "local_rasff_alert_index"
    source_label = "Local RASFF alert index"
    has_matches = False
    alerts: List[Dict[str, Any]] = []

    raw_dataset = _load_json(SAFETY_ALERTS_FILE, None)
    if raw_dataset is None and RASFF_FILE.exists():
        raw_dataset = _load_json(RASFF_FILE, None)

    if raw_dataset is not None:
        checked = True
        product_name = str(norm.get("name") or "").strip()
        brand = str(norm.get("brand") or "").strip()
        entries = _normalize_safety_alert_entries(raw_dataset)
        matched: List[Tuple[int, Dict[str, Any]]] = []
        for entry in entries:
            score = _safety_match_score(entry, key, product_name, brand)
            if score <= 0:
                continue
            matched.append((score, entry))
        matched.sort(key=lambda item: item[0], reverse=True)

        seen = set()
        for score, entry in matched[:8]:
            title = _normalize_safety_text(entry.get("title") or entry.get("alert") or "Safety alert")
            summary = _normalize_safety_text(entry.get("summary") or entry.get("description"))
            url = _normalize_safety_text(entry.get("url") or entry.get("link"))
            batch = _normalize_safety_text(entry.get("batch") or entry.get("batch_number"))
            lot = _normalize_safety_text(entry.get("lot") or entry.get("lot_number"))
            best_before = _normalize_safety_text(entry.get("best_before") or entry.get("expiry") or entry.get("best_before_date"))
            severity = _normalize_safety_text(entry.get("severity") or "medium").lower() or "medium"
            scope = _normalize_safety_text(entry.get("scope") or ("batch" if entry.get("batch_specific") else "product")).lower() or "product"
            batch_specific = bool(entry.get("batch_specific")) or scope in {"batch", "lot", "best_before"}
            dedupe_key = (title.lower(), batch.lower(), lot.lower(), best_before.lower(), url.lower())
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            alerts.append({
                "title": title,
                "summary": summary,
                "url": url or None,
                "severity": severity if severity in {"high", "medium", "low"} else "medium",
                "scope": "batch" if batch_specific else "product",
                "batch_specific": batch_specific,
                "batch": batch or None,
                "lot": lot or None,
                "best_before": best_before or None,
                "source": source_name,
                "source_label": _normalize_safety_text(entry.get("source_label") or entry.get("source")) or source_label,
                "match_score": score,
            })
        has_matches = len(alerts) > 0

    return {
        "checked": checked,
        "source": source_name if checked else None,
        "source_label": source_label if checked else None,
        "has_matches": has_matches,
        "alerts": alerts,
    }


async def _fetch_safety_url_text(url: str, *, timeout_sec: float = 3.5) -> Optional[str]:
    cache_key = f"url::{str(url or '').strip()}"
    cached = _safety_http_cache_get(cache_key)
    if cached is not None:
        return cached
    try:
        async with httpx.AsyncClient(timeout=timeout_sec, follow_redirects=True) as client:
            response = await client.get(url, headers={"User-Agent": "NoesisFood/0.1 (safety lookup)"})
        if response.status_code != 200:
            return None
        text = response.text or ""
        _safety_http_cache_set(cache_key, text)
        return text
    except Exception:
        return None


async def _fetch_safety_url_json(url: str, *, timeout_sec: float = 5.5) -> Optional[Dict[str, Any]]:
    text = await _fetch_safety_url_text(url, timeout_sec=timeout_sec)
    if not text:
        return None
    try:
        payload = json.loads(text)
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _extract_safety_scope_details(text: str) -> Dict[str, Optional[str]]:
    normalized = _normalize_safety_text(text)
    batch_match = re.search(
        r"(?:chargennummer\s*/\s*los-kennzeichnung|chargennummer|charge|chargen|batch|partie|parti(?:da|de)?|παρτίδα)\s*[:#]?\s*(.+?)(?=\s(?:weitere kennzeichnung|grund der meldung|haltbarkeit|mindesthaltbar|hersteller|inverkehrbringer|betroffene bundesländer|vertrieb über|was ist der grund|$))",
        normalized,
        flags=re.I,
    )
    lot_match = re.search(
        r"(?:los-kennzeichnung|lot|los|lotto)\s*[:#]?\s*(.+?)(?=\s(?:weitere kennzeichnung|grund der meldung|haltbarkeit|mindesthaltbar|hersteller|inverkehrbringer|betroffene bundesländer|vertrieb über|was ist der grund|$))",
        normalized,
        flags=re.I,
    )
    best_before_match = re.search(
        r"(?:mindestens haltbar bis(?: ende)?|best before|best-before|mhd|à consommer de préférence avant|ανάλωση κατά προτίμηση πριν από)\s*[:#]?\s*([0-9]{1,2}[./-][0-9]{1,2}[./-][0-9]{2,4}|[0-9]{2}[./-][0-9]{4}|[0-9]{4}-[0-9]{2}-[0-9]{2})",
        normalized,
        flags=re.I,
    )
    batch_specific = bool(batch_match or lot_match or best_before_match or re.search(r"\b(?:specific batches|bestimmte chargen|certains lots|συγκεκριμένες παρτίδες)\b", normalized, flags=re.I))
    batch_value = re.sub(r"\s+", " ", (batch_match.group(1) if batch_match else "")).strip(" -:/") or None
    lot_value = re.sub(r"\s+", " ", (lot_match.group(1) if lot_match else "")).strip(" -:/") or None
    return {
        "batch_specific": batch_specific,
        "batch": batch_value,
        "lot": lot_value,
        "best_before": best_before_match.group(1).strip() if best_before_match else None,
    }


def _extract_alert_barcodes(text: str) -> List[str]:
    source = str(text or "")
    hits: List[str] = []
    for match in re.findall(r"\b\d{8,14}\b", source):
        hits.append(match)
    for match in re.findall(r"(?:GTIN|EAN|Barcode)\s*[:#]?\s*([0-9][0-9\s]{7,20}[0-9])", source, flags=re.I):
        compact = re.sub(r"\s+", "", match)
        if 8 <= len(compact) <= 14 and compact.isdigit():
            hits.append(compact)
    seen: List[str] = []
    for hit in hits:
        if hit not in seen:
            seen.append(hit)
    return seen


def _severity_from_text(text: str) -> str:
    normalized = _normalize_match_text(text)
    if any(term in normalized for term in ["gesundheitsgefahr", "health risk", "risk to health", "risque pour la santé", "κίνδυνος για την υγεία", "pathogen", "salmonella", "listeria"]):
        return "high"
    if any(term in normalized for term in ["serious", "grave", "aflatoxin", "foreign body", "glass fragment", "metal fragment"]):
        return "high"
    if any(term in normalized for term in ["undeclared", "nicht gekennzeichnet", "non déclaré", "μη δηλωμ", "allergen"]):
        return "medium"
    return "medium"


async def _fetch_lebensmittelwarnung_entries() -> Dict[str, Any]:
    observability = _new_safety_observability()
    _bump_observability_bucket(observability, "fetch_count", "lebensmittelwarnung_de", 1)
    _bump_observability_bucket(observability, "page_count", "lebensmittelwarnung_de", 1)
    feed_text = await _fetch_safety_url_text(LEBENSMITTELWARNUNG_FEED_URL, timeout_sec=4.5)
    if not feed_text:
        _set_no_match_reason(observability, "lebensmittelwarnung_de", "source_unavailable")
        return {"checked": False, "entries": [], "observability": observability}
    try:
        root = ET.fromstring(feed_text)
    except Exception:
        _set_no_match_reason(observability, "lebensmittelwarnung_de", "source_unavailable")
        return {"checked": False, "entries": [], "observability": observability}

    entries: List[Dict[str, Any]] = []
    for item in root.findall(".//item"):
        title = _normalize_safety_text(item.findtext("title"))
        link = _normalize_safety_text(item.findtext("link"))
        description_html = item.findtext("description") or ""
        description = _strip_html_to_text(description_html)
        pub_date = _normalize_safety_text(item.findtext("pubDate"))
        text_blob = " ".join([title, description]).strip()
        entries.append({
            "title": title or "Lebensmittelwarnung",
            "summary": description,
            "url": urljoin("https://www.lebensmittelwarnung.de/", link) if link else None,
            "published_at": pub_date or None,
            "text_blob": text_blob,
            "barcodes": _extract_alert_barcodes(text_blob),
        })
    _bump_observability_bucket(observability, "source_checked", "lebensmittelwarnung_de", 1)
    if not entries:
        _set_no_match_reason(observability, "lebensmittelwarnung_de", "no_recent_entries")
    return {"checked": True, "entries": entries, "observability": observability}


async def _enrich_lebensmittelwarnung_recent_entries(entries: List[Dict[str, Any]], *, limit: int = 14) -> List[Dict[str, Any]]:
    enriched: List[Dict[str, Any]] = []
    for index, entry in enumerate(entries):
        current = dict(entry)
        if index < limit and current.get("url"):
            detail_html = await _fetch_safety_url_text(str(current.get("url")), timeout_sec=3.2)
            if detail_html:
                detail_text = _strip_html_to_text(detail_html)
                current["detail_text"] = detail_text
                merged_blob = " ".join([
                    str(current.get("title") or ""),
                    str(current.get("summary") or ""),
                    detail_text,
                ]).strip()
                current["text_blob"] = merged_blob
                current["barcodes"] = _extract_alert_barcodes(merged_blob)
                scope_details = _extract_safety_scope_details(merged_blob)
                current["batch_specific"] = bool(scope_details.get("batch_specific"))
                current["batch"] = scope_details.get("batch")
                current["lot"] = scope_details.get("lot")
                current["best_before"] = scope_details.get("best_before")
        enriched.append(current)
    return enriched


def _rasff_recent_start_iso(days_back: int = RASFF_LOOKBACK_DAYS) -> str:
    start_ts = time.time() - (max(1, int(days_back)) * 86400)
    return time.strftime("%Y-%m-%dT00:00:00Z", time.gmtime(start_ts))


def _build_rasff_public_api_url(next_link: Optional[str] = None) -> str:
    if next_link:
        return str(next_link)
    return (
        f"{RASFF_PUBLIC_API_URL}"
        f"?format=json"
        f"&NETWORK_DESC=RASFF"
        f"&NOTIF_DATE_FROM={_rasff_recent_start_iso()}"
        f"&api-version={RASFF_PUBLIC_API_VERSION}"
    )


def _normalize_rasff_public_entries(raw_entries: Any) -> List[Dict[str, Any]]:
    entries: List[Dict[str, Any]] = []
    if not isinstance(raw_entries, list):
        return entries
    for item in raw_entries:
        if not isinstance(item, dict):
            continue
        product_name = _normalize_safety_text(item.get("PRODUCT_NAME"))
        subject = _normalize_safety_text(item.get("NOTIF_SUBJECT"))
        category = _normalize_safety_text(item.get("PRODUCT_CATEGORY_DESC"))
        hazard = _normalize_safety_text(item.get("HAZARD_CATEGORY_NAME"))
        risk = _normalize_safety_text(item.get("RISK_DECISION_DESC"))
        origin = _normalize_safety_text(item.get("ORIGIN_COUNTRY_DESC"))
        distribution = _normalize_safety_text(item.get("DISTRIBUTION_COUNTRY_DESC"))
        reference = _normalize_safety_text(item.get("NOTIFICATION_REFERENCE"))
        published_at = _normalize_safety_text(item.get("NOTIF_DATE"))
        status = _normalize_safety_text(item.get("NOTIFICATION_STATUS_DESC"))
        classification = _normalize_safety_text(item.get("NOTIFICATION_CLASSIFICAT_DESC"))
        basis = _normalize_safety_text(item.get("NOTIFICATION_BASIS_DESC"))
        distribution_status = _normalize_safety_text(item.get("DISTRIBUTION_STATUS_DESC"))
        notifying_country = _normalize_safety_text(item.get("NOTIFYNG_COUNTRY_DESC"))
        title = product_name or subject or reference or "RASFF notification"
        summary_parts = [
            subject,
            hazard,
            f"Risk: {risk}" if risk else "",
            f"Category: {category}" if category else "",
            f"Origin: {origin}" if origin else "",
            f"Distribution: {distribution}" if distribution else "",
            f"Reference: {reference}" if reference else "",
        ]
        summary = " | ".join([part for part in summary_parts if part])
        text_blob = " ".join([
            product_name,
            subject,
            category,
            hazard,
            risk,
            origin,
            distribution,
            reference,
            classification,
            status,
            basis,
            distribution_status,
            notifying_country,
        ]).strip()
        entries.append({
            "notif_id": item.get("NOTIF_ID"),
            "reference": reference or None,
            "title": title,
            "summary": summary,
            "product_name": product_name,
            "product_variants": _product_name_variants(product_name or subject),
            "subject": subject,
            "category": category,
            "hazard": hazard,
            "risk": risk,
            "status": status or None,
            "classification": classification or None,
            "basis": basis or None,
            "distribution_status": distribution_status or None,
            "notifying_country": notifying_country or None,
            "origin_country": origin or None,
            "distribution_country": distribution or None,
            "published_at": published_at or None,
            "url": RASFF_PUBLIC_SEARCH_URL,
            "text_blob": text_blob,
            "overlap_key": _normalize_official_overlap_key("rasff_dg_sante_api", product_name or title, reference=reference),
            "source": "rasff_dg_sante_api",
            "source_label": "RASFF (DG SANTE API)",
        })
    return entries


async def _fetch_rasff_public_entries() -> Dict[str, Any]:
    entries: List[Dict[str, Any]] = []
    next_link: Optional[str] = None
    checked = False
    oldest_seen_ts: Optional[float] = None
    cutoff_ts = time.time() - (max(1, int(RASFF_LOOKBACK_DAYS)) * 86400)
    observability = _new_safety_observability()
    for _ in range(RASFF_MAX_PAGES):
        _bump_observability_bucket(observability, "fetch_count", "rasff_dg_sante_api", 1)
        _bump_observability_bucket(observability, "page_count", "rasff_dg_sante_api", 1)
        payload = await _fetch_safety_url_json(_build_rasff_public_api_url(next_link), timeout_sec=8.0)
        if not isinstance(payload, dict):
            if not checked:
                _set_no_match_reason(observability, "rasff_dg_sante_api", "source_unavailable")
            break
        checked = True
        normalized_entries = _normalize_rasff_public_entries(payload.get("value"))
        entries.extend(normalized_entries)
        for entry in normalized_entries:
            published_at = str(entry.get("published_at") or "").strip()
            if not published_at:
                continue
            try:
                published_ts = time.mktime(time.strptime(published_at[:19], "%Y-%m-%dT%H:%M:%S"))
            except Exception:
                continue
            oldest_seen_ts = published_ts if oldest_seen_ts is None else min(oldest_seen_ts, published_ts)
        next_link = _normalize_safety_text(payload.get("nextLink")) or None
        if not next_link:
            break
        if oldest_seen_ts is not None and oldest_seen_ts < cutoff_ts:
            break
    if checked:
        _bump_observability_bucket(observability, "source_checked", "rasff_dg_sante_api", 1)
        if not entries:
            _set_no_match_reason(observability, "rasff_dg_sante_api", "no_recent_entries")
    return {"checked": checked, "entries": entries, "observability": observability}


async def _fetch_efet_entries() -> Dict[str, Any]:
    entries: List[Dict[str, Any]] = []
    checked = False
    observability = _new_safety_observability()
    candidate_urls: List[str] = []
    for url in EFET_RECALL_LISTING_URLS[:EFET_MAX_DISCOVERY_PAGES]:
        _bump_observability_bucket(observability, "fetch_count", EFET_SOURCE_KEY, 1)
        _bump_observability_bucket(observability, "page_count", EFET_SOURCE_KEY, 1)
        listing_html = await _fetch_safety_url_text(url, timeout_sec=5.0)
        if not listing_html:
            continue
        checked = True
        for detail_url in _extract_efet_listing_links(listing_html):
            if detail_url not in candidate_urls:
                candidate_urls.append(detail_url)

    cutoff_ts = time.time() - (max(1, int(EFET_LOOKBACK_DAYS)) * 86400)
    for detail_url in candidate_urls[:EFET_MAX_DETAIL_PAGES]:
        _bump_observability_bucket(observability, "fetch_count", EFET_SOURCE_KEY, 1)
        _bump_observability_bucket(observability, "page_count", EFET_SOURCE_KEY, 1)
        detail_html = await _fetch_safety_url_text(detail_url, timeout_sec=4.5)
        if not detail_html:
            continue
        checked = True
        entry = _normalize_efet_entry(detail_url, detail_html)
        if not isinstance(entry, dict):
            continue
        published_ts = _parse_safety_published_ts(str(entry.get("published_at") or ""))
        if published_ts is not None and published_ts < cutoff_ts:
            continue
        entries.append(entry)

    if checked:
        _bump_observability_bucket(observability, "source_checked", EFET_SOURCE_KEY, 1)
        if not entries:
            _set_no_match_reason(observability, EFET_SOURCE_KEY, "no_recent_entries")
    else:
        _set_no_match_reason(observability, EFET_SOURCE_KEY, "source_unavailable")
    return {"checked": checked, "entries": entries, "observability": observability}


def _score_external_alert_candidate(entry: Dict[str, Any], barcode: str, product_name: str, brand: str, category: str) -> Tuple[int, str]:
    score = 0
    confidence = ""
    barcode = str(barcode or "").strip()
    product_tokens = set(_match_tokens(product_name))
    brand_norm = _normalize_match_text(brand)
    category_tokens = set(_match_tokens(category))
    text_blob = _normalize_match_text(" ".join([
        entry.get("title") or "",
        entry.get("summary") or "",
        entry.get("detail_text") or "",
    ]))

    entry_barcodes = set(_extract_alert_barcodes(text_blob))
    entry_barcodes.update([str(code or "").strip() for code in entry.get("barcodes") or [] if str(code or "").strip()])
    if barcode and barcode in entry_barcodes:
        return 100, "high"

    overlap = len(product_tokens.intersection(set(text_blob.split())))
    if brand_norm and brand_norm in text_blob:
        score += 20
    if overlap >= 2:
        score += 25
    elif overlap == 1:
        score += 10
    category_overlap = len(category_tokens.intersection(set(text_blob.split())))
    if category_overlap >= 1:
        score += 5

    if score >= 40:
        confidence = "medium"
    return score, confidence


def _score_rasff_public_alert_candidate(entry: Dict[str, Any], product_name: str, brand: str, category: str) -> Tuple[int, str]:
    score = 0
    confidence = ""
    product_norm = _normalize_match_text(product_name)
    brand_norm = _normalize_match_text(brand)
    category_norm = _normalize_match_text(category)
    product_variants = _product_name_variants(product_name)
    product_tokens = _match_tokens(product_name)
    brand_tokens = _match_tokens(brand)
    category_tokens = _match_tokens(category)
    if not product_tokens and not product_variants:
        return 0, confidence

    entry_product = _normalize_match_text(entry.get("product_name") or entry.get("title"))
    entry_subject = _normalize_match_text(entry.get("subject"))
    entry_category = _normalize_match_text(entry.get("category"))
    entry_hazard = _normalize_match_text(entry.get("hazard"))
    entry_risk = _normalize_match_text(entry.get("risk"))
    entry_reference = _normalize_match_text(entry.get("reference"))
    entry_variants = _as_list(entry.get("product_variants"))
    text_blob = _normalize_match_text(entry.get("text_blob"))
    entry_tokens = _match_tokens(text_blob)
    overlap = len(set(product_tokens).intersection(set(entry_tokens)))
    overlap_ratio = _token_overlap_ratio(product_tokens, entry_tokens)
    brand_overlap = len(set(brand_tokens).intersection(set(entry_tokens)))
    category_overlap = len(set(category_tokens).intersection(set(_match_tokens(entry_category or text_blob))))

    exact_variant_match = any(variant and variant in entry_variants for variant in product_variants)
    strong_variant_containment = any(
        variant and (
            (entry_product and variant in entry_product)
            or (entry_subject and variant in entry_subject)
        )
        for variant in product_variants
    )
    if exact_variant_match:
        score += 82
    elif strong_variant_containment:
        score += 68
    elif product_norm and entry_product and (product_norm == entry_product or product_norm in entry_product or entry_product in product_norm):
        score += 72

    if overlap >= 4:
        score += 22
    elif overlap == 3:
        score += 16
    elif overlap == 2:
        score += 10

    if overlap_ratio >= 0.9:
        score += 12
    elif overlap_ratio >= 0.66:
        score += 8

    if brand_norm and brand_norm in text_blob:
        score += 12
    elif brand_overlap >= 1:
        score += 8

    if category_norm and entry_category and (category_norm in entry_category or entry_category in category_norm):
        score += 10
    elif category_overlap >= 1:
        score += 6

    if entry_hazard:
        score += 2
    if entry_reference:
        score += 2
    if entry_risk in {"serious", "potentially serious", "potential risk"}:
        score += 2

    strong_high_match = bool(
        exact_variant_match
        or (strong_variant_containment and len(product_tokens) >= 2)
        or (overlap_ratio >= 0.9 and len(product_tokens) >= 2)
    )

    if score >= 96 and strong_high_match:
        confidence = "high"
    elif score >= 76:
        confidence = "medium"
    elif score >= 58:
        confidence = "low"
    return score, confidence


def _score_efet_alert_candidate(entry: Dict[str, Any], barcode: str, product_name: str, brand: str, category: str) -> Tuple[int, str]:
    score = 0
    confidence = ""
    barcode = str(barcode or "").strip()
    product_norm = _normalize_match_text(product_name)
    brand_norm = _normalize_match_text(brand)
    category_norm = _normalize_match_text(category)
    product_variants = _product_name_variants(product_name)
    product_tokens = _match_tokens(product_name)
    brand_tokens = _match_tokens(brand)
    category_tokens = _match_tokens(category)
    text_blob = _normalize_match_text(entry.get("text_blob"))
    if not text_blob:
        return 0, confidence

    entry_barcodes = {str(code or "").strip() for code in entry.get("barcodes") or [] if str(code or "").strip()}
    if barcode and barcode in entry_barcodes:
        return 100, "high"

    entry_product = _normalize_match_text(entry.get("product_name") or entry.get("title"))
    entry_company = _normalize_match_text(entry.get("company"))
    entry_variants = _as_list(entry.get("product_variants"))
    entry_tokens = _match_tokens(text_blob)
    overlap = len(set(product_tokens).intersection(set(entry_tokens)))
    overlap_ratio = _token_overlap_ratio(product_tokens, entry_tokens)
    brand_overlap = len(set(brand_tokens).intersection(set(entry_tokens)))
    category_overlap = len(set(category_tokens).intersection(set(entry_tokens)))
    exact_variant_match = any(variant and variant in entry_variants for variant in product_variants)
    strong_variant_containment = any(
        variant and ((entry_product and variant in entry_product) or variant in text_blob)
        for variant in product_variants
    )

    if exact_variant_match:
        score += 80
    elif strong_variant_containment:
        score += 66
    elif product_norm and entry_product and (product_norm == entry_product or product_norm in entry_product or entry_product in product_norm):
        score += 70

    if overlap >= 4:
        score += 22
    elif overlap == 3:
        score += 16
    elif overlap == 2:
        score += 10

    if overlap_ratio >= 0.9:
        score += 10
    elif overlap_ratio >= 0.66:
        score += 6

    if brand_norm and (brand_norm in text_blob or (entry_company and brand_norm in entry_company)):
        score += 12
    elif brand_overlap >= 1:
        score += 8

    if category_norm and category_norm in text_blob:
        score += 6
    elif category_overlap >= 1:
        score += 4

    if entry.get("hazard"):
        score += 2
    if entry.get("packaging"):
        score += 2

    strong_high_match = bool(
        exact_variant_match
        or (strong_variant_containment and len(product_tokens) >= 2)
        or (product_norm and entry_product and product_norm == entry_product)
    )
    if score >= 96 and strong_high_match:
        confidence = "high"
    elif score >= 76:
        confidence = "medium"
    elif score >= 58:
        confidence = "low"
    return score, confidence


def _merge_safety_alerts(alerts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    merged: Dict[Tuple[str, str, str, str, str], Dict[str, Any]] = {}
    for alert in alerts:
        title = _normalize_safety_text(alert.get("title")).lower()
        batch = _normalize_safety_text(alert.get("batch")).lower()
        lot = _normalize_safety_text(alert.get("lot")).lower()
        url = _normalize_safety_text(alert.get("url")).lower()
        reference = _normalize_safety_text(alert.get("reference")).lower()
        key = (title, batch, lot, url, reference)
        existing = merged.get(key)
        if not existing:
            for candidate in merged.values():
                if _alerts_likely_same_official_event(candidate, alert):
                    existing = candidate
                    break
        if not existing:
            merged[key] = dict(alert)
            sources = [str(alert.get("source") or "").strip()] if str(alert.get("source") or "").strip() else []
            merged[key]["sources"] = sources
            continue
        if len(_normalize_safety_text(alert.get("summary"))) > len(_normalize_safety_text(existing.get("summary"))):
            existing["summary"] = alert.get("summary")
        if not existing.get("url") and alert.get("url"):
            existing["url"] = alert.get("url")
        if not existing.get("batch") and alert.get("batch"):
            existing["batch"] = alert.get("batch")
        if not existing.get("lot") and alert.get("lot"):
            existing["lot"] = alert.get("lot")
        if not existing.get("best_before") and alert.get("best_before"):
            existing["best_before"] = alert.get("best_before")
        if not existing.get("reference") and alert.get("reference"):
            existing["reference"] = alert.get("reference")
        if not existing.get("product_name") and alert.get("product_name"):
            existing["product_name"] = alert.get("product_name")
        if not existing.get("packaging") and alert.get("packaging"):
            existing["packaging"] = alert.get("packaging")
        if not existing.get("overlap_key") and alert.get("overlap_key"):
            existing["overlap_key"] = alert.get("overlap_key")
        if str(alert.get("severity") or "") == "high":
            existing["severity"] = "high"
        if int(alert.get("match_score") or 0) > int(existing.get("match_score") or 0):
            existing["match_score"] = alert.get("match_score")
            existing["confidence"] = alert.get("confidence")
            existing["source"] = alert.get("source")
            existing["source_label"] = alert.get("source_label")
        existing["batch_specific"] = bool(existing.get("batch_specific")) or bool(alert.get("batch_specific"))
        existing["scope"] = "batch" if existing.get("batch_specific") else existing.get("scope") or "product"
        existing_sources = existing.get("sources")
        if not isinstance(existing_sources, list):
            existing_sources = []
            existing["sources"] = existing_sources
        source = str(alert.get("source") or "").strip()
        if source and source not in existing_sources:
            existing_sources.append(source)

    ordered = list(merged.values())
    ordered.sort(key=lambda item: (int(item.get("match_score") or 0), str(item.get("severity") or "")), reverse=True)
    return ordered[:8]


def _merged_safety_source_key(lookups: List[Dict[str, Any]], alerts: List[Dict[str, Any]]) -> Optional[str]:
    matched_source_keys: List[str] = []
    checked_source_keys: List[str] = []
    for lookup in lookups:
        if not isinstance(lookup, dict):
            continue
        source = str(lookup.get("source") or "").strip().lower()
        if source and bool(lookup.get("checked")) and source not in checked_source_keys:
            checked_source_keys.append(source)
        if source and bool(lookup.get("has_matches")) and source not in matched_source_keys:
            matched_source_keys.append(source)
    for alert in alerts:
        if not isinstance(alert, dict):
            continue
        source = str(alert.get("source") or "").strip().lower()
        if source and source not in matched_source_keys:
            matched_source_keys.append(source)

    source_keys = matched_source_keys or checked_source_keys
    if not source_keys:
        return None
    if len(source_keys) == 1:
        return source_keys[0]
    return "multi_source_safety"


def _merge_safety_lookup_payloads(*lookups: Dict[str, Any]) -> Dict[str, Any]:
    valid = [lookup for lookup in lookups if isinstance(lookup, dict)]
    alerts: List[Dict[str, Any]] = []
    checked = False
    raw_alert_count = 0
    for lookup in valid:
        checked = checked or bool(lookup.get("checked"))
        lookup_alerts = _as_list(lookup.get("alerts"))
        raw_alert_count += len(lookup_alerts)
        alerts.extend(lookup_alerts)
    merged_alerts = _merge_safety_alerts(alerts)
    observability = _merge_safety_observability(*[lookup.get("observability") for lookup in valid])
    observability["duplicate_collapsed"] = int(observability.get("duplicate_collapsed") or 0) + max(0, raw_alert_count - len(merged_alerts))
    observability["batch_scope_explicit"] = int(observability.get("batch_scope_explicit") or 0) + len([
        alert for alert in merged_alerts
        if bool(alert.get("batch_specific")) or str(alert.get("scope") or "").strip().lower() == "batch"
    ])
    for alert in merged_alerts:
        confidence = str(alert.get("confidence") or "").strip().lower()
        if confidence:
            _bump_observability_bucket(observability, "confidence_assigned", confidence, 1)
    return {
        "checked": checked,
        "source": _merged_safety_source_key(valid, merged_alerts),
        "source_label": None,
        "has_matches": len(merged_alerts) > 0,
        "alerts": merged_alerts,
        "observability": observability,
    }


async def _lookup_rasff_public_alerts(norm: Dict[str, Any]) -> Dict[str, Any]:
    product_name = str(norm.get("name") or "").strip()
    brand = str(norm.get("brand") or "").strip()
    category = " ".join([str(item).strip() for item in _as_list(norm.get("categories")) if str(item).strip()])
    feed_result = await _fetch_rasff_public_entries()
    observability = _merge_safety_observability(feed_result.get("observability"))
    if not feed_result.get("checked"):
        return {
            "checked": False,
            "source": None,
            "source_label": None,
            "has_matches": False,
            "alerts": [],
            "observability": observability,
        }

    alerts: List[Dict[str, Any]] = []
    for entry in feed_result.get("entries") or []:
        score, confidence = _score_rasff_public_alert_candidate(entry, product_name, brand, category)
        if score < 78:
            continue
        full_text = " ".join([
            str(entry.get("title") or ""),
            str(entry.get("summary") or ""),
            str(entry.get("text_blob") or ""),
        ]).strip()
        alerts.append({
            "title": _normalize_safety_text(entry.get("title") or "RASFF notification"),
            "summary": _normalize_safety_text(entry.get("summary")),
            "url": entry.get("url"),
            "severity": _severity_from_text(full_text),
            "scope": "product",
            "batch_specific": False,
            "batch": None,
            "lot": None,
            "best_before": None,
            "source": "rasff_dg_sante_api",
            "source_label": "RASFF (DG SANTE API)",
            "match_score": int(score),
            "confidence": confidence or "low",
            "published_at": entry.get("published_at"),
            "product_name": entry.get("product_name"),
            "overlap_key": entry.get("overlap_key"),
            "reference": entry.get("reference"),
        })

    if alerts:
        _bump_observability_bucket(observability, "source_matched", "rasff_dg_sante_api", len(alerts))
    elif feed_result.get("entries"):
        _set_no_match_reason(observability, "rasff_dg_sante_api", "no_candidate_above_threshold")

    return {
        "checked": True,
        "source": "rasff_dg_sante_api",
        "source_label": "RASFF (DG SANTE API)",
        "has_matches": len(alerts) > 0,
        "alerts": _merge_safety_alerts(alerts),
        "observability": observability,
    }


async def _lookup_efet_alerts(key: str, norm: Dict[str, Any]) -> Dict[str, Any]:
    barcode = str(key or "").strip()
    product_name = str(norm.get("name") or "").strip()
    brand = str(norm.get("brand") or "").strip()
    category = " ".join([str(item).strip() for item in _as_list(norm.get("categories")) if str(item).strip()])
    feed_result = await _fetch_efet_entries()
    observability = _merge_safety_observability(feed_result.get("observability"))
    if not feed_result.get("checked"):
        return {
            "checked": False,
            "source": None,
            "source_label": None,
            "has_matches": False,
            "alerts": [],
            "observability": observability,
        }

    alerts: List[Dict[str, Any]] = []
    for entry in feed_result.get("entries") or []:
        score, confidence = _score_efet_alert_candidate(entry, barcode, product_name, brand, category)
        if score < 76:
            continue
        full_text = " ".join([
            str(entry.get("title") or ""),
            str(entry.get("summary") or ""),
            str(entry.get("text_blob") or ""),
        ]).strip()
        scope_details = {
            "batch_specific": bool(entry.get("batch_specific")),
            "batch": entry.get("batch"),
            "lot": entry.get("lot"),
            "best_before": entry.get("best_before"),
        }
        if scope_details.get("batch_specific"):
            confidence = "conditional"
        alerts.append({
            "title": _normalize_safety_text(entry.get("title") or "EFET recall"),
            "summary": _normalize_safety_text(entry.get("summary")),
            "url": entry.get("url"),
            "severity": _severity_from_text(full_text),
            "scope": "batch" if scope_details.get("batch_specific") else "product",
            "batch_specific": bool(scope_details.get("batch_specific")),
            "batch": scope_details.get("batch"),
            "lot": scope_details.get("lot"),
            "best_before": scope_details.get("best_before"),
            "source": EFET_SOURCE_KEY,
            "source_label": EFET_SOURCE_LABEL,
            "match_score": int(score),
            "confidence": confidence or "low",
            "published_at": entry.get("published_at"),
            "product_name": entry.get("product_name"),
            "packaging": entry.get("packaging"),
            "overlap_key": entry.get("overlap_key"),
            "reference": entry.get("reference"),
        })

    if alerts:
        _bump_observability_bucket(observability, "source_matched", EFET_SOURCE_KEY, len(alerts))
    elif feed_result.get("entries"):
        _set_no_match_reason(observability, EFET_SOURCE_KEY, "no_candidate_above_threshold")

    return {
        "checked": True,
        "source": EFET_SOURCE_KEY,
        "source_label": EFET_SOURCE_LABEL,
        "has_matches": len(alerts) > 0,
        "alerts": _merge_safety_alerts(alerts),
        "observability": observability,
    }


async def _lookup_lebensmittelwarnung_alerts(key: str, norm: Dict[str, Any]) -> Dict[str, Any]:
    barcode = str(key or "").strip()
    product_name = str(norm.get("name") or "").strip()
    brand = str(norm.get("brand") or "").strip()
    category = " ".join([str(item).strip() for item in _as_list(norm.get("categories")) if str(item).strip()])

    feed_result = await _fetch_lebensmittelwarnung_entries()
    lebensmittelwarnung_observability = _merge_safety_observability(feed_result.get("observability"))
    if not (lebensmittelwarnung_observability.get("fetch_count") or {}).get("lebensmittelwarnung_de"):
        _bump_observability_bucket(lebensmittelwarnung_observability, "fetch_count", "lebensmittelwarnung_de", 1)
    if not (lebensmittelwarnung_observability.get("page_count") or {}).get("lebensmittelwarnung_de"):
        _bump_observability_bucket(lebensmittelwarnung_observability, "page_count", "lebensmittelwarnung_de", 1)
    if not feed_result.get("checked"):
        if not str((lebensmittelwarnung_observability.get("no_match_reason") or {}).get("lebensmittelwarnung_de") or "").strip():
            _set_no_match_reason(lebensmittelwarnung_observability, "lebensmittelwarnung_de", "source_unavailable")
        return {
            "checked": False,
            "source": None,
            "source_label": None,
            "has_matches": False,
            "alerts": [],
            "observability": lebensmittelwarnung_observability,
        }

    if not (lebensmittelwarnung_observability.get("source_checked") or {}).get("lebensmittelwarnung_de"):
        _bump_observability_bucket(lebensmittelwarnung_observability, "source_checked", "lebensmittelwarnung_de", 1)

    raw_entries = list(feed_result.get("entries") or [])
    if not raw_entries:
        _set_no_match_reason(lebensmittelwarnung_observability, "lebensmittelwarnung_de", "no_feed_entries")
        return {
            "checked": True,
            "source": "lebensmittelwarnung_de",
            "source_label": "lebensmittelwarnung.de",
            "has_matches": False,
            "alerts": [],
            "observability": lebensmittelwarnung_observability,
        }

    scored_entries: List[Dict[str, Any]] = []
    for entry in raw_entries:
        score, confidence = _score_external_alert_candidate(entry, barcode, product_name, brand, category)
        enriched = dict(entry)
        enriched["_candidate_score"] = score
        enriched["_candidate_confidence"] = confidence
        scored_entries.append(enriched)

    scored_entries.sort(key=lambda item: int(item.get("_candidate_score") or 0), reverse=True)

    shortlist: List[Dict[str, Any]] = []
    seen_shortlist_urls: set[str] = set()
    for entry in scored_entries[:6]:
        url = str(entry.get("url") or "").strip()
        if url and url in seen_shortlist_urls:
            continue
        shortlist.append(entry)
        if url:
            seen_shortlist_urls.add(url)
    for entry in raw_entries[:4]:
        url = str(entry.get("url") or "").strip()
        if url and url in seen_shortlist_urls:
            continue
        shortlist.append(entry)
        if url:
            seen_shortlist_urls.add(url)

    enriched_shortlist = await _enrich_lebensmittelwarnung_recent_entries(shortlist, limit=len(shortlist))
    enriched_by_url: Dict[str, Dict[str, Any]] = {}
    for entry in enriched_shortlist:
        url = str(entry.get("url") or "").strip()
        if url:
            enriched_by_url[url] = entry

    candidate_entries: List[Dict[str, Any]] = []
    seen_candidate_urls: set[str] = set()
    for entry in scored_entries[:8]:
        url = str(entry.get("url") or "").strip()
        candidate = dict(enriched_by_url.get(url) or entry)
        score, confidence = _score_external_alert_candidate(candidate, barcode, product_name, brand, category)
        if score <= 0:
            continue
        candidate["_candidate_score"] = score
        candidate["_candidate_confidence"] = confidence
        candidate_entries.append(candidate)
        if url:
            seen_candidate_urls.add(url)

    for entry in enriched_shortlist:
        url = str(entry.get("url") or "").strip()
        if url and url in seen_candidate_urls:
            continue
        score, confidence = _score_external_alert_candidate(entry, barcode, product_name, brand, category)
        if score <= 0 and not (barcode and barcode in set(_extract_alert_barcodes(entry.get("text_blob") or ""))):
            continue
        extra = dict(entry)
        extra["_candidate_score"] = max(score, 100 if barcode and barcode in set(_extract_alert_barcodes(entry.get("text_blob") or "")) else score)
        extra["_candidate_confidence"] = "high" if extra["_candidate_score"] == 100 else confidence
        candidate_entries.append(extra)
        if url:
            seen_candidate_urls.add(url)

    candidate_entries.sort(key=lambda item: int(item.get("_candidate_score") or 0), reverse=True)
    candidate_entries = candidate_entries[:8]

    alerts: List[Dict[str, Any]] = []
    for entry in candidate_entries:
        detail_text = str(entry.get("detail_text") or "")
        full_text = " ".join([str(entry.get("title") or ""), str(entry.get("summary") or ""), detail_text]).strip()
        score, confidence = _score_external_alert_candidate(
            {**entry, "detail_text": detail_text},
            barcode,
            product_name,
            brand,
            category,
        )
        if score < 40 and confidence != "high":
            continue
        scope_details = _extract_safety_scope_details(full_text)
        if scope_details.get("batch_specific"):
            confidence = "conditional"
        alerts.append({
            "title": _normalize_safety_text(entry.get("title") or "Lebensmittelwarnung"),
            "summary": _normalize_safety_text(entry.get("summary") or detail_text),
            "url": entry.get("url"),
            "severity": _severity_from_text(full_text),
            "scope": "batch" if scope_details.get("batch_specific") else "product",
            "batch_specific": bool(scope_details.get("batch_specific")),
            "batch": scope_details.get("batch"),
            "lot": scope_details.get("lot"),
            "best_before": scope_details.get("best_before"),
            "source": "lebensmittelwarnung_de",
            "source_label": "lebensmittelwarnung.de",
            "match_score": int(score),
            "confidence": confidence or "medium",
            "published_at": entry.get("published_at"),
        })

    if alerts:
        _bump_observability_bucket(lebensmittelwarnung_observability, "source_matched", "lebensmittelwarnung_de", len(alerts))
    else:
        _set_no_match_reason(lebensmittelwarnung_observability, "lebensmittelwarnung_de", "no_candidate_above_threshold")
    return {
        "checked": True,
        "source": "lebensmittelwarnung_de",
        "source_label": "lebensmittelwarnung.de",
        "has_matches": len(alerts) > 0,
        "alerts": _merge_safety_alerts(alerts),
        "observability": lebensmittelwarnung_observability,
    }


async def _lookup_external_safety_alerts(key: str, norm: Dict[str, Any]) -> Dict[str, Any]:
    barcode = str(key or "").strip()
    product_name = str(norm.get("name") or "").strip()
    brand = str(norm.get("brand") or "").strip()
    category = " ".join([str(item).strip() for item in _as_list(norm.get("categories")) if str(item).strip()])
    cache_key = f"{barcode}::{_normalize_match_text(product_name)}::{_normalize_match_text(brand)}"
    cached = _safety_lookup_cache_get(cache_key)
    if cached is not None:
        return cached

    async def _timed_lookup(field: str, coroutine: Any) -> Dict[str, Any]:
        started = time.perf_counter()
        result = await coroutine
        result_copy = copy.deepcopy(result) if isinstance(result, dict) else {}
        result_copy["_timing_field"] = field
        result_copy["_timing_ms"] = int(round((time.perf_counter() - started) * 1000.0))
        return result_copy

    lebensmittelwarnung_lookup, rasff_lookup, efet_lookup = await asyncio.gather(
        _timed_lookup("safety_lebensmittelwarnung_ms", _lookup_lebensmittelwarnung_alerts(key, norm)),
        _timed_lookup("safety_rasff_ms", _lookup_rasff_public_alerts(norm)),
        _timed_lookup("safety_efet_ms", _lookup_efet_alerts(key, norm)),
    )
    merge_started = time.perf_counter()
    result = _merge_safety_lookup_payloads(lebensmittelwarnung_lookup, rasff_lookup, efet_lookup)
    result["_timing"] = {
        str(lebensmittelwarnung_lookup.get("_timing_field") or "safety_lebensmittelwarnung_ms"): int(lebensmittelwarnung_lookup.get("_timing_ms") or 0),
        str(rasff_lookup.get("_timing_field") or "safety_rasff_ms"): int(rasff_lookup.get("_timing_ms") or 0),
        str(efet_lookup.get("_timing_field") or "safety_efet_ms"): int(efet_lookup.get("_timing_ms") or 0),
        "safety_merge_ms": int(round((time.perf_counter() - merge_started) * 1000.0)),
    }
    _safety_lookup_cache_set(cache_key, result)
    return result


def _apply_safety_lookup_to_result(result: Dict[str, Any], safety_lookup: Dict[str, Any]) -> Dict[str, Any]:
    payload = copy.deepcopy(result) if isinstance(result, dict) else {}
    alerts = safety_lookup.get("alerts") or []
    payload["alerts"] = [str(item.get("title") or "").strip() for item in alerts if str(item.get("title") or "").strip()]
    payload["safety_alerts_checked"] = bool(safety_lookup.get("checked"))
    payload["safety_alerts_source"] = safety_lookup.get("source")
    payload["safety_alerts_has_matches"] = bool(safety_lookup.get("has_matches"))
    payload["safety_alerts"] = alerts
    payload["safety_observability"] = copy.deepcopy(safety_lookup.get("observability") or _new_safety_observability())
    meta = payload.get("meta")
    if not isinstance(meta, dict):
        meta = {}
        payload["meta"] = meta
    meta["safety_alerts_checked"] = bool(safety_lookup.get("checked"))
    meta["safety_alerts_source"] = safety_lookup.get("source")
    meta["safety_alerts_has_matches"] = bool(safety_lookup.get("has_matches"))
    meta["safety_observability"] = copy.deepcopy(payload.get("safety_observability") or _new_safety_observability())
    return payload


async def _finalize_scan_result_with_safety(result: Dict[str, Any], key: str, norm: Dict[str, Any], timing: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    if not isinstance(result, dict):
        return result
    lookup_started = time.perf_counter()
    safety_lookup = await _lookup_external_safety_alerts(key, norm if isinstance(norm, dict) else {})
    existing_lookup = {
        "checked": bool(result.get("safety_alerts_checked") or _get_path(result, "meta", "safety_alerts_checked")),
        "source": result.get("safety_alerts_source") or _get_path(result, "meta", "safety_alerts_source"),
        "has_matches": bool(result.get("safety_alerts_has_matches") or _get_path(result, "meta", "safety_alerts_has_matches")),
        "alerts": _as_list(result.get("safety_alerts")),
        "observability": result.get("safety_observability") or _get_path(result, "meta", "safety_observability"),
    }
    merged_lookup = _merge_safety_lookup_payloads(existing_lookup, safety_lookup)
    merged_observability = _merge_safety_observability(merged_lookup.get("observability"))
    merged_observability["fallback_used"] = bool(
        str(result.get("analysis_state") or _get_path(result, "meta", "analysis_state") or "").strip().lower() == "limited_estimate"
        or str(result.get("lookup_state") or _get_path(result, "meta", "lookup_state") or "").strip().lower() in {"found_but_incomplete", "not_found"}
    )
    merged_lookup["observability"] = merged_observability
    if isinstance(timing, dict):
        timing["safety_lookup_ms"] = int(round((time.perf_counter() - lookup_started) * 1000.0))
        timing["safety_alerts_checked"] = bool(merged_lookup.get("checked"))
        timing["safety_alerts_has_matches"] = bool(merged_lookup.get("has_matches"))
        per_source_timing = merged_lookup.get("_timing")
        if not isinstance(per_source_timing, dict):
            per_source_timing = safety_lookup.get("_timing")
        if isinstance(per_source_timing, dict):
            timing["safety_lebensmittelwarnung_ms"] = int(per_source_timing.get("safety_lebensmittelwarnung_ms") or 0)
            timing["safety_rasff_ms"] = int(per_source_timing.get("safety_rasff_ms") or 0)
            timing["safety_efet_ms"] = int(per_source_timing.get("safety_efet_ms") or 0)
            timing["safety_merge_ms"] = int(per_source_timing.get("safety_merge_ms") or 0)
    return _apply_safety_lookup_to_result(result, merged_lookup)


def _complete_simple_cheese_ingredients(normalized: Dict[str, Any], ingredients: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    product_text = _normalized_product_text(normalized)
    if not _contains_any(product_text, _CHEESE_MARKERS):
        return ingredients
    if _contains_any(product_text, _CHEESE_EXCLUSION_MARKERS):
        return ingredients

    existing = _sanitize_ingredients_minimal(ingredients)
    existing_keys = {_ingredient_confidence_text(item.get("name")) for item in existing if isinstance(item, dict)}
    simple_existing = len(existing_keys)
    has_milk = any(key and any(term in key for term in ("milk", "milch", "lait", "γάλα", "γαλα")) for key in existing_keys)
    has_salt = any(
        key and (
            "salt" in key
            or key == "salz"
            or key == "sel"
            or "αλάτι" in key
            or "αλατι" in key
        )
        for key in existing_keys
    )
    has_culture = any(key and ("culture" in key or "kultur" in key or "ferment" in key) for key in existing_keys)
    has_rennet = any(key and ("rennet" in key or "lab" in key or "présure" in key or "presure" in key or "πυτιά" in key or "πυτια" in key) for key in existing_keys)

    if simple_existing >= 3 and has_milk:
        return existing

    additions: List[Dict[str, Any]] = []
    if not has_milk:
        additions.append({"name": "milk", "class": "U", "note": ""})
    if not has_salt:
        additions.append({"name": "salt", "class": "U", "note": ""})
    if not has_culture:
        additions.append({"name": "cultures", "class": "U", "note": ""})
    if not has_rennet:
        additions.append({"name": "rennet", "class": "U", "note": ""})

    if not existing and not additions:
        return existing
    return existing + additions

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
    traditional_simple_cheese = bool(
        (_contains_any(product_text, _TRADITIONAL_CHEESE_MARKERS) or simple_cheese)
        and not _contains_any(product_text, _CHEESE_EXCLUSION_MARKERS)
        and ingredient_count <= 5
        and processing_score <= 3
        and int(markers.get("sweeteners", 0)) == 0
        and int(markers.get("flavourings", 0)) == 0
        and int(markers.get("colorants", 0)) == 0
        and int(markers.get("preservatives", 0)) == 0
        and e_count == 0
    )
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
        "plain_nuts_seed_pure": {
            "el": "Το προϊόν είναι 100% ξηρός καρπός ή σπόρος χωρίς πρόσθετα.",
            "en": "The product is 100% plain nuts or seeds with no additives.",
            "de": "Das Produkt besteht zu 100% aus einfachen Nüssen oder Samen ohne Zusatzstoffe.",
            "fr": "Le produit est composé à 100% de noix ou graines simples, sans additifs.",
        },
        "plain_nuts_seed_clean": {
            "el": "Η απουσία αλατιού και προσθέτων βελτιώνει σημαντικά τη συνολική εικόνα.",
            "en": "The absence of salt and additives significantly improves the overall picture.",
            "de": "Das Fehlen von Salz und Zusatzstoffen verbessert das Gesamtbild deutlich.",
            "fr": "L’absence de sel et d’additifs améliore nettement l’ensemble.",
        },
        "traditional_cheese_simple": {
            "el": "Η απλή παραδοσιακή σύνθεση λειτουργεί θετικά.",
            "en": "The simple traditional composition helps the assessment.",
            "de": "Die einfache traditionelle Zusammensetzung wirkt sich positiv aus.",
            "fr": "La composition traditionnelle simple aide l’évaluation.",
        },
        "traditional_cheese_low_processed": {
            "el": "Το προϊόν είναι χαμηλής επεξεργασίας.",
            "en": "The product is low-processed.",
            "de": "Das Produkt ist wenig verarbeitet.",
            "fr": "Le produit est peu transformé.",
        },
        "traditional_cheese_short_list": {
            "el": "Η σύντομη λίστα συστατικών βελτιώνει τη συνολική εικόνα.",
            "en": "The short ingredient list improves the overall picture.",
            "de": "Die kurze Zutatenliste verbessert das Gesamtbild.",
            "fr": "La liste courte d’ingrédients améliore l’ensemble.",
        },
        "traditional_cheese_caution": {
            "el": "Παρότι η κατηγορία έχει αλάτι και κορεσμένα, η απλή σύνθεση λειτουργεί θετικά.",
            "en": "Although this category carries salt and saturated fat, the simple composition helps the assessment.",
            "de": "Obwohl diese Kategorie Salz und gesättigte Fettsäuren enthält, wirkt sich die einfache Zusammensetzung positiv aus.",
            "fr": "Même si cette catégorie comporte du sel et des acides gras saturés, la composition simple aide l’évaluation.",
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
        applied.append({"rule_id": "plain_nuts_seed_pure", "delta": 2, "impact_weight": 63})
        if simple_single:
            applied.append({"rule_id": "plain_nuts_seed_simple", "delta": 2, "impact_weight": 60})
        if salt is not None and salt <= 0.05:
            applied.append({"rule_id": "plain_nuts_seed_clean", "delta": 1, "impact_weight": 59})
    if traditional_simple_cheese:
        applied.append({"rule_id": "traditional_cheese_simple", "delta": 2, "impact_weight": 58})
        applied.append({"rule_id": "traditional_cheese_low_processed", "delta": 1, "impact_weight": 56})
        if ingredient_count <= 4:
            applied.append({"rule_id": "traditional_cheese_short_list", "delta": 1, "impact_weight": 55})
        if (salt is not None and salt >= 1.0) or (satfat is not None and satfat >= 8.0):
            applied.append({"rule_id": "traditional_cheese_caution", "delta": 1, "impact_weight": 54})

    total_delta = sum(int(item.get("delta", 0)) for item in applied)
    if salt is not None and salt >= 1.8:
        total_delta -= 2
    if satfat is not None and satfat >= 10:
        total_delta -= 1
    if energy is not None and energy >= 650 and not nuts_or_seeds:
        total_delta -= 1

    total_cap = 16 if plain_nuts_seed_candidate else (8 if traditional_simple_cheese else 7)
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
            "traditional_simple_cheese": traditional_simple_cheese,
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
    traditional_simple_cheese = (
        (_contains_any(product_text, _TRADITIONAL_CHEESE_MARKERS) or _contains_any(product_text, _CHEESE_MARKERS))
        and not _contains_any(product_text, _CHEESE_EXCLUSION_MARKERS)
        and ingredient_count <= 5 and minimally_processed and not excluded
        and int(markers.get("sweeteners", 0)) == 0
        and int(markers.get("flavourings", 0)) == 0
        and int(markers.get("colorants", 0)) == 0
        and int(markers.get("preservatives", 0)) == 0
        and (salt is None or salt <= 3.2)
    )

    floor_score: Optional[int] = None
    applied: List[Dict[str, Any]] = []

    if plain_nuts_seed:
        floor_score = 82
    elif plain_legumes:
        floor_score = 63
    elif plain_tomato_veg:
        floor_score = 62
    elif simple_oats_grains:
        floor_score = 60
    elif plain_fruit:
        floor_score = 61
    elif traditional_simple_cheese:
        floor_score = 72

    if floor_score is None:
        return {"applied": [], "floor_score": None, "floor_delta": 0}

    if salt is not None and salt >= 1.0:
        floor_score -= 4
    if sugar is not None and sugar >= 15.0 and not plain_fruit:
        floor_score -= 4
    if satfat is not None and satfat >= 15.0 and not plain_nuts_seed:
        floor_score -= 3
    if traditional_simple_cheese and salt is not None and salt >= 2.5:
        floor_score -= 3
    if traditional_simple_cheese and satfat is not None and satfat >= 18.0:
        floor_score -= 2

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
    if traditional_simple_cheese:
        reason_ids = ["whole_food_category", "minimal_processing_floor", "simple_category_floor"]

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
            "traditional_simple_cheese": traditional_simple_cheese,
        },
    }


def _whole_food_cap_adjustments(
    normalized: Dict[str, Any],
    per100: Dict[str, Optional[float]],
    intelligence: Dict[str, Any],
    *,
    is_beverage: bool,
    analysis_state: str,
    current_score: int,
) -> Dict[str, Any]:
    if is_beverage:
        return {"applied": [], "cap_score": None, "cap_delta": 0}

    markers = intelligence.get("markers", {}) if isinstance(intelligence, dict) else {}
    if not isinstance(markers, dict):
        markers = {}
    product_text = _normalized_product_text(normalized)
    ingredient_count = len(_as_list(normalized.get("ingredients")))
    processing_score = int(_to_float(intelligence.get("processing_score")) or 0) if isinstance(intelligence, dict) else 0
    salt = _to_float(per100.get("salt_g"))
    sugar = _to_float(per100.get("sugar_g"))

    no_additives = all(int(markers.get(k) or 0) == 0 for k in (
        "sweeteners", "flavourings", "colorants", "preservatives", "emulsifiers_stabilizers", "e_numbers"
    ))
    minimally_processed = processing_score <= 2
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
    simple_oats_grains = (
        _contains_any(product_text, _OATS_GRAINS_MARKERS)
        and simple_short and minimally_processed and no_additives and not excluded
        and (salt is None or salt <= 0.2)
        and (sugar is None or sugar <= 10.0)
    )
    traditional_simple_cheese = (
        (_contains_any(product_text, _TRADITIONAL_CHEESE_MARKERS) or _contains_any(product_text, _CHEESE_MARKERS))
        and not _contains_any(product_text, _CHEESE_EXCLUSION_MARKERS)
        and ingredient_count <= 5 and minimally_processed and not excluded
        and int(markers.get("sweeteners", 0)) == 0
        and int(markers.get("flavourings", 0)) == 0
        and int(markers.get("colorants", 0)) == 0
        and int(markers.get("preservatives", 0)) == 0
        and (salt is None or salt <= 3.2)
    )

    cap_score: Optional[int] = None
    state = str(analysis_state or "").lower()
    if plain_nuts_seed:
        cap_score = 87 if state == "full_analysis" else 85
    elif plain_legumes:
        cap_score = 76 if state == "full_analysis" else 74
    elif plain_tomato_veg:
        cap_score = 74 if state == "full_analysis" else 72
    elif simple_oats_grains:
        cap_score = 72 if state == "full_analysis" else 70
    elif traditional_simple_cheese:
        cap_score = 81 if state == "full_analysis" else 78

    if cap_score is None or current_score <= cap_score:
        return {"applied": [], "cap_score": cap_score, "cap_delta": 0}

    return {
        "applied": [],
        "cap_score": int(cap_score),
        "cap_delta": int(cap_score - current_score),
        "flags": {
            "plain_nuts_seed": plain_nuts_seed,
            "plain_legumes": plain_legumes,
            "plain_tomato_veg": plain_tomato_veg,
            "simple_oats_grains": simple_oats_grains,
            "traditional_simple_cheese": traditional_simple_cheese,
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
    seen_names: set[str] = set()

    # 1) start with E-numbers from additives_tags
    for e in additives_e_numbers or []:
        e = _e_base(e)
        if e and e not in all_e_numbers:
            all_e_numbers.append(e)

    # 2) enrich ingredients and also collect E from text
    for ing in ingredients or []:
        name = ing.get("name") if isinstance(ing, dict) else str(ing)
        name = _sanitize_ingredient_candidate(str(name or ""))
        if not name:
            continue
        if _is_noisy_ingredient_text(name):
            continue

        name_key = _ingredient_confidence_text(name)
        if not name_key or name_key in seen_names:
            continue
        seen_names.add(name_key)

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
        "sanitized_ingredients": enriched,
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


def _recalibrate_processing_intelligence(
    normalized: Dict[str, Any],
    per100: Dict[str, Optional[float]],
    ingredients: List[Dict[str, Any]],
    intelligence: Dict[str, Any],
) -> Dict[str, Any]:
    if not isinstance(intelligence, dict):
        return intelligence

    updated = dict(intelligence)
    markers = dict(updated.get("markers") or {})
    product_text = _normalized_product_text(normalized)
    ingredient_count = len(ingredients or [])
    sugar = _to_float(per100.get("sugar_g"))
    energy = _to_float(per100.get("energy_kcal"))
    base_score = int(_to_float(updated.get("processing_score")) or 0)
    additive_complexity = sum(
        int(markers.get(key) or 0)
        for key in ("flavourings", "emulsifiers_stabilizers", "colorants", "preservatives", "sweeteners", "e_numbers")
    )

    score = float(base_score)
    if sugar is not None and sugar >= 20 and additive_complexity >= 2:
        score += 1.5
    if sugar is not None and sugar >= 35 and additive_complexity >= 3:
        score += 1.5
    if ingredient_count >= 8 and additive_complexity >= 2:
        score += 1.0
    if _contains_any(product_text, _CONFECTIONERY_MARKERS) and additive_complexity >= 2:
        score += 1.5
    if energy is not None and energy >= 450 and sugar is not None and sugar >= 30 and additive_complexity >= 2:
        score += 0.5

    score_i = int(round(_clamp(score, 0.0, 10.0)))
    if score_i <= 2:
        proc_label = "Minimally processed"
    elif score_i <= 5:
        proc_label = "Processed"
    else:
        proc_label = "Highly processed"

    updated["processing_score"] = score_i
    updated["processing_label"] = proc_label
    return updated


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

    if present <= 1:
        confidence = min(confidence, 0.35)
    elif present == 2:
        confidence = min(confidence, 0.5)

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


def _core_nutrition_guard(per100: Dict[str, Optional[float]]) -> Dict[str, Any]:
    required_keys = ["energy_kcal", "sugar_g", "salt_g", "saturated_fat_g"]
    missing = [key for key in required_keys if per100.get(key) is None]
    present = len(required_keys) - len(missing)
    if not missing:
        return {
            "required_keys": required_keys,
            "present_count": present,
            "missing_keys": [],
            "base_score_cap": None,
            "applied": False,
            "reason": "complete_core_nutrition",
        }
    if present <= 1:
        cap = 72
        reason = "core_nutrition_mostly_missing"
    elif present == 2:
        cap = 82
        reason = "core_nutrition_partially_missing"
    else:
        cap = 92
        reason = "single_core_nutrition_missing"
    return {
        "required_keys": required_keys,
        "present_count": present,
        "missing_keys": missing,
        "base_score_cap": cap,
        "applied": True,
        "reason": reason,
    }


def _confidence_rank(value: str) -> int:
    key = str(value or "").strip().lower()
    if key == "high":
        return 3
    if key == "medium":
        return 2
    return 1


def _confidence_from_data_quality(dq: Dict[str, Any]) -> str:
    confidence_value = _to_float((dq or {}).get("confidence"))
    if confidence_value is None:
        return "low"
    if confidence_value >= 0.75:
        return "high"
    if confidence_value >= 0.45:
        return "medium"
    return "low"


def _align_analysis_confidence(
    analysis_state: str,
    analysis_confidence: str,
    dq: Dict[str, Any],
) -> str:
    state_key = str(analysis_state or "").strip().lower()
    current = str(analysis_confidence or "").strip().lower() or "low"
    missing_core_count = len(_as_list((dq or {}).get("missing_core_fields")))
    dq_tier = _confidence_from_data_quality(dq)

    if state_key == "limited_estimate":
        return "low"
    if missing_core_count >= 3:
        return "low"
    if missing_core_count >= 2 and current == "high":
        current = "medium"
    return dq_tier if _confidence_rank(dq_tier) < _confidence_rank(current) else current



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


def _has_renderable_product_identity_payload(result: Dict[str, Any]) -> bool:
    product = result.get("product") if isinstance(result, dict) else {}
    if not isinstance(product, dict):
        product = {}
    name = str(product.get("name") or "").strip()
    if name and not is_placeholder_product_name(name):
        return True
    if str(product.get("brand") or "").strip():
        return True
    if str(product.get("image_url") or "").strip():
        return True
    return False


def _has_renderable_nutrition_payload(result: Dict[str, Any]) -> bool:
    nutrition = result.get("nutrition_per_100") if isinstance(result, dict) else {}
    if not isinstance(nutrition, dict):
        nutrition = {}
    return any(
        _to_float(nutrition.get(key)) is not None
        for key in ("sugar_g", "salt_g", "sat_fat_g", "protein_g", "energy_kcal")
    )


def _scan_resolution_metadata(result: Dict[str, Any]) -> Dict[str, Any]:
    lookup_state = str(result.get("lookup_state") or _get_path(result, "meta", "lookup_state") or "").strip().lower()
    analysis_state = str(result.get("analysis_state") or _get_path(result, "meta", "analysis_state") or "").strip().lower()
    has_identity = _has_renderable_product_identity_payload(result)
    has_nutrition = _has_renderable_nutrition_payload(result)
    has_stable_source_result = lookup_state == "found_and_analyzable" or analysis_state in {"full_analysis", "partial_analysis"}

    if has_identity and (has_nutrition or has_stable_source_result) and analysis_state not in {"insufficient_data", ""}:
        return {
            "scan_resolution_state": "final_resolved_product",
            "final_render_allowed": True,
            "final_render_reason": "resolved_product_payload",
            "product_identity_missing": False,
            "nutrition_missing": not has_nutrition,
        }
    if analysis_state == "limited_estimate" and not has_identity and not has_nutrition:
        return {
            "scan_resolution_state": "fallback_estimate_only",
            "final_render_allowed": False,
            "final_render_reason": "missing_identity_and_nutrition",
            "product_identity_missing": True,
            "nutrition_missing": True,
        }
    if lookup_state in {"found_but_incomplete", "not_found"} or analysis_state == "insufficient_data" or not has_identity:
        return {
            "scan_resolution_state": "unresolved_scan",
            "final_render_allowed": False,
            "final_render_reason": "incomplete_or_unresolved_scan",
            "product_identity_missing": not has_identity,
            "nutrition_missing": not has_nutrition,
        }
    return {
        "scan_resolution_state": "fallback_estimate_only",
        "final_render_allowed": False,
        "final_render_reason": "fallback_estimate_requires_enrichment",
        "product_identity_missing": not has_identity,
        "nutrition_missing": not has_nutrition,
    }


def _attach_scan_resolution_metadata(result: Dict[str, Any], timing: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    if not isinstance(result, dict):
        return result
    metadata = _scan_resolution_metadata(result)
    payload = copy.deepcopy(result)
    payload.update({
        "scan_resolution_state": metadata["scan_resolution_state"],
        "final_render_allowed": metadata["final_render_allowed"],
        "final_render_reason": metadata["final_render_reason"],
    })
    meta = payload.get("meta")
    if not isinstance(meta, dict):
        meta = {}
        payload["meta"] = meta
    meta["scan_resolution_state"] = metadata["scan_resolution_state"]
    meta["final_render_allowed"] = metadata["final_render_allowed"]
    meta["final_render_reason"] = metadata["final_render_reason"]
    meta["product_identity_missing"] = metadata["product_identity_missing"]
    meta["nutrition_missing"] = metadata["nutrition_missing"]
    if isinstance(timing, dict):
        timing["scan_resolution_state"] = metadata["scan_resolution_state"]
        timing["final_render_allowed"] = metadata["final_render_allowed"]
        timing["final_render_reason"] = metadata["final_render_reason"]
    logger.info(
        "scan resolution key=%s lookup_source=%s off_ok=%s off_status=%s fallback_generated=%s identity_missing=%s nutrition_missing=%s render_gate_allowed=%s resolution_state=%s reason=%s",
        str(payload.get("key") or (timing or {}).get("key") or ""),
        str((timing or {}).get("lookup_source") or payload.get("source") or ""),
        bool((timing or {}).get("off_fetch_ok")),
        (timing or {}).get("openfoodfacts_status"),
        bool((timing or {}).get("fallback_generated")),
        metadata["product_identity_missing"],
        metadata["nutrition_missing"],
        metadata["final_render_allowed"],
        metadata["scan_resolution_state"],
        metadata["final_render_reason"],
    )
    return payload


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


def _ingredient_merge_key(value: Any) -> str:
    text = _sanitize_ingredient_candidate(str(value or ""))
    return _ingredient_confidence_text(text)


def _ingredient_name_list(items: Any) -> List[str]:
    out: List[str] = []
    for item in _as_list(items):
        if isinstance(item, dict):
            name = str(item.get("name") or "").strip()
        else:
            name = str(item or "").strip()
        if name:
            out.append(name)
    return out


def _prefer_photo_ingredients(existing_ingredients: Any, extracted_ingredients: Any, extracted_text: Any) -> bool:
    existing_names = _ingredient_name_list(existing_ingredients)
    extracted_names = _ingredient_name_list(extracted_ingredients)
    existing_keys = {_ingredient_merge_key(name) for name in existing_names if _ingredient_merge_key(name)}
    extracted_keys = {_ingredient_merge_key(name) for name in extracted_names if _ingredient_merge_key(name)}
    existing_text = ", ".join(existing_names).strip()
    extracted_text_clean = str(extracted_text or "").strip()
    if not extracted_keys:
        return False
    if len(extracted_keys) > len(existing_keys):
        return True
    if len(extracted_text_clean) > len(existing_text) + 20:
        return True
    return False


def _merge_ingredient_objects(existing_ingredients: Any, extracted_text: Any, *, note: str = "From enrichment") -> List[Dict[str, Any]]:
    existing_items = [item for item in _as_list(existing_ingredients) if isinstance(item, dict) and str(item.get("name") or "").strip()]
    extracted_items = _manual_ingredients_from_text(extracted_text, note=note)
    if not extracted_items:
        return copy.deepcopy(existing_items)

    prefer_photo = _prefer_photo_ingredients(existing_items, extracted_items, extracted_text)
    merged: List[Dict[str, Any]] = []
    seen = set()

    def _push(item: Dict[str, Any]) -> None:
        key = _ingredient_merge_key(item.get("name"))
        if not key or key in seen:
            return
        seen.add(key)
        merged.append(copy.deepcopy(item))

    ordered_sources = (extracted_items, existing_items) if prefer_photo else (existing_items, extracted_items)
    for source in ordered_sources:
        for item in source:
            _push(item)
    return merged


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _load_product_enrichments() -> List[Dict[str, Any]]:
    raw = _load_json(PRODUCT_ENRICHMENTS_FILE, {"enrichments": []})
    if isinstance(raw, dict) and isinstance(raw.get("enrichments"), list):
        return raw.get("enrichments") or []
    if isinstance(raw, list):
        return raw
    return []


def _save_product_enrichments(items: List[Dict[str, Any]]) -> None:
    _save_json(PRODUCT_ENRICHMENTS_FILE, {"enrichments": items})


def _get_product_enrichment(barcode: str) -> Optional[Dict[str, Any]]:
    code = str(barcode or "").strip()
    if not code:
        return None
    matches = [
        item for item in _load_product_enrichments()
        if isinstance(item, dict) and str(item.get("barcode") or "").strip() == code
    ]
    if not matches:
        return None
    matches.sort(key=lambda item: str(item.get("updated_at") or item.get("created_at") or ""))
    return copy.deepcopy(matches[-1])


def _persist_product_enrichment(
    *,
    barcode: str,
    product_identity: Dict[str, Any],
    extracted: Dict[str, Any],
    merged_payload: Dict[str, Any],
    used_ingredient_photo: bool,
    used_nutrition_photo: bool,
) -> Optional[Dict[str, Any]]:
    code = str(barcode or "").strip()
    if not code:
        return None
    now_iso = _utc_now_iso()
    items = _load_product_enrichments()
    current = next((item for item in items if isinstance(item, dict) and str(item.get("barcode") or "").strip() == code), None)
    if current is None:
        current = {
            "barcode": code,
            "created_at": now_iso,
        }
        items.append(current)

    captured_payload = {
        "ingredients_text": str(merged_payload.get("ingredients_text") or "").strip() or None,
        "nutrition_per_100": {
            "sugar_g": _to_float(merged_payload.get("sugar_g")),
            "salt_g": _to_float(merged_payload.get("salt_g")),
            "sat_fat_g": _to_float(merged_payload.get("sat_fat_g")),
            "protein_g": _to_float(merged_payload.get("protein_g")),
            "serving_size": _to_float(merged_payload.get("serving_size")),
            "unit": str(merged_payload.get("unit") or "").strip().lower() or None,
        },
        "categories": _as_list(merged_payload.get("categories")),
    }

    current.update({
        "barcode": code,
        "product_identity": {
            "name": str(product_identity.get("name") or "").strip() or None,
            "brand": str(product_identity.get("brand") or "").strip() or None,
            "quantity": str(product_identity.get("quantity") or "").strip() or None,
        },
        "captured_payload": captured_payload,
        "extracted_structured": copy.deepcopy(extracted if isinstance(extracted, dict) else {}),
        "source": "user_photo_enrichment",
        "confidence": str((extracted or {}).get("confidence") or "low").strip().lower() or "low",
        "extracted_fields": _as_list((extracted or {}).get("extracted_fields")),
        "verification_flags": {
            "ingredient_photo": bool(used_ingredient_photo),
            "nutrition_photo": bool(used_nutrition_photo),
        },
        "review_status": "unreviewed",
        "updated_at": now_iso,
    })
    _save_product_enrichments(items)
    return copy.deepcopy(current)


def _apply_product_enrichment(norm: Dict[str, Any], enrichment: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(norm, dict) or not isinstance(enrichment, dict):
        return norm
    out = copy.deepcopy(norm)
    captured = enrichment.get("captured_payload") if isinstance(enrichment.get("captured_payload"), dict) else {}
    nutrition = captured.get("nutrition_per_100") if isinstance(captured.get("nutrition_per_100"), dict) else {}
    per100 = out.get("nutrition_per_100") if isinstance(out.get("nutrition_per_100"), dict) else {}
    if not isinstance(per100, dict):
        per100 = {}
    for field in ("sugar_g", "salt_g", "sat_fat_g", "protein_g", "energy_kcal", "serving_size"):
        if per100.get(field) is None and nutrition.get(field) is not None:
            per100[field] = nutrition.get(field)
    if not per100.get("unit") and nutrition.get("unit"):
        per100["unit"] = nutrition.get("unit")
    out["nutrition_per_100"] = per100

    ingredients_text = str(captured.get("ingredients_text") or "").strip()
    ingredients_photo_preferred = False
    if ingredients_text:
        existing_ingredients = out.get("ingredients")
        extracted_ingredients = _manual_ingredients_from_text(ingredients_text, note="From enrichment")
        ingredients_photo_preferred = _prefer_photo_ingredients(existing_ingredients, extracted_ingredients, ingredients_text)
        merged_ingredients = _merge_ingredient_objects(existing_ingredients, ingredients_text, note="From enrichment")
        if merged_ingredients:
            out["ingredients"] = merged_ingredients

    captured_categories = _as_list(captured.get("categories"))
    if captured_categories and not _as_list(out.get("categories")):
        out["categories"] = captured_categories

    ingredients_meta = out.get("ingredients_meta") if isinstance(out.get("ingredients_meta"), dict) else {}
    if ingredients_text:
        ingredients_meta["enriched_from_photo"] = True
    if ingredients_meta:
        out["ingredients_meta"] = ingredients_meta

    meta = out.get("meta") if isinstance(out.get("meta"), dict) else {}
    meta["enrichment_layer"] = {
        "source": str(enrichment.get("source") or "user_photo_enrichment"),
        "confidence": str(enrichment.get("confidence") or "low"),
        "updated_at": str(enrichment.get("updated_at") or ""),
        "verification_flags": enrichment.get("verification_flags") if isinstance(enrichment.get("verification_flags"), dict) else {},
        "stored_fields": {
            "ingredients_text": bool(ingredients_text),
            "nutrition_per_100": {
                key: nutrition.get(key) is not None
                for key in ("sugar_g", "salt_g", "sat_fat_g", "protein_g", "serving_size")
            },
        },
        "ingredients_merge_applied": bool(ingredients_text),
        "ingredients_photo_preferred": ingredients_photo_preferred,
    }
    out["meta"] = meta
    return out


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


def _photo_parsing_failed() -> Dict[str, Any]:
    err = _scan_error("PHOTO_PARSING_FAILED", "The photo was uploaded, but the label data could not be extracted reliably.", 422)
    err.update(_lookup_state_payload("found_but_incomplete"))
    err["analysis_state"] = "insufficient_data"
    err["analysis_confidence"] = "low"
    return err


def _normalize_photo_extracted_payload(parsed: Dict[str, Any], payload: Dict[str, Any]) -> Dict[str, Any]:
    out = copy.deepcopy(parsed) if isinstance(parsed, dict) else {}
    payload = payload if isinstance(payload, dict) else {}
    existing_product = payload.get("existing_product") if isinstance(payload.get("existing_product"), dict) else {}
    existing_analysis = payload.get("existing_analysis") if isinstance(payload.get("existing_analysis"), dict) else {}
    existing_meta = existing_analysis.get("meta") if isinstance(existing_analysis.get("meta"), dict) else {}
    existing_serving = existing_meta.get("serving") if isinstance(existing_meta.get("serving"), dict) else {}
    nutrition = out.get("nutrition_per_100") if isinstance(out.get("nutrition_per_100"), dict) else {}
    if not isinstance(nutrition, dict):
        nutrition = {}
    out["nutrition_per_100"] = nutrition

    extracted_fields = [str(item).strip() for item in _as_list(out.get("extracted_fields")) if str(item).strip()]
    notes = str(out.get("notes") or "").strip()
    label_kind = str(out.get("label_kind") or "").strip().lower()
    composition_table_text = str(out.get("composition_table_text") or "").strip()
    categories = out.get("categories")
    if isinstance(categories, str):
        categories = [c.strip() for c in categories.split(",") if c.strip()]
    elif not isinstance(categories, list):
        categories = []
    product_name = str(out.get("product_name") or existing_product.get("name") or "").strip()
    brand = str(out.get("brand") or existing_product.get("brand") or "").strip()
    ingredients_text = str(out.get("ingredients_text") or "").strip()

    evidence_blob = " ".join([
        product_name,
        brand,
        " ".join([str(item).strip() for item in categories if str(item).strip()]),
        notes,
        composition_table_text,
        " ".join(extracted_fields),
        str(existing_product.get("name") or ""),
        " ".join([str(item).strip() for item in _as_list(existing_product.get("categories")) if str(item).strip()]),
    ]).strip().lower()
    has_water_markers = _contains_any(evidence_blob, _CLEAN_WATER_MARKERS)
    composition_markers = (
        "composition table", "mineral composition", "mineralisation", "mineralization", "analysis", "analyse",
        "hydrogencarbonat", "hydrogencarbonate", "bicarbonate", "calcium", "magnesium", "natrium", "sodium",
        "sulfat", "sulfate", "chlorid", "chloride", "fluorid", "fluoride", "trockenrückstand", "dry residue",
        "kohlensäure", "co2", "medium"
    )
    has_composition_markers = any(marker in evidence_blob for marker in composition_markers) or label_kind == "composition_table"
    excluded_water_markers = ("juice", "saft", "soft drink", "soda", "cola", "energy", "flavour", "flavor", "limonade", "sirup", "syrup")
    has_excluded_markers = any(marker in evidence_blob for marker in excluded_water_markers)
    has_nutrition = any(nutrition.get(k) is not None for k in ("energy_kcal", "sugar_g", "salt_g", "sat_fat_g", "protein_g"))
    if has_water_markers and has_composition_markers and not has_nutrition and not has_excluded_markers:
        if not product_name:
            product_name = str(existing_product.get("name") or "").strip()
        if not brand:
            brand = str(existing_product.get("brand") or "").strip()
        existing_categories = [str(item).strip() for item in _as_list(existing_product.get("categories")) if str(item).strip()]
        if not categories:
            categories = existing_categories
        if not categories:
            categories = ["Mineral water"]
        if not ingredients_text and product_name:
            # Use the source-native water identity text only for composition-table water labels.
            ingredients_text = product_name
            extracted_fields.append("ingredients_text")
        if nutrition.get("unit") is None:
            inferred_unit = _first_present(
                payload.get("unit"),
                nutrition.get("unit"),
                existing_analysis.get("nutrition_per_100", {}).get("unit") if isinstance(existing_analysis.get("nutrition_per_100"), dict) else None,
                existing_serving.get("unit"),
                "ml",
            )
            nutrition["unit"] = str(inferred_unit or "ml").strip().lower() or "ml"
        for field in ("energy_kcal", "sugar_g", "salt_g", "sat_fat_g", "protein_g"):
            if nutrition.get(field) is None:
                nutrition[field] = 0.0
        if not out.get("composition_table_text") and composition_table_text:
            out["composition_table_text"] = composition_table_text
        out["label_kind"] = "composition_table"
        extracted_fields.append("composition_table")
        extracted_fields.append("nutrition_per_100")
        if not notes:
            notes = "Composition-table water fallback applied with conservative zero nutrition for plain mineral water."
        elif "composition-table water fallback applied" not in notes.lower():
            notes = f"{notes} Composition-table water fallback applied with conservative zero nutrition for plain mineral water.".strip()

    out["product_name"] = product_name or None
    out["brand"] = brand or None
    out["ingredients_text"] = ingredients_text or None
    out["categories"] = [str(item).strip() for item in categories if str(item).strip()]
    out["extracted_fields"] = list(dict.fromkeys(extracted_fields))
    out["notes"] = notes or None
    out["nutrition_per_100"] = nutrition
    return out


def _build_photo_context_water_fallback(payload: Dict[str, Any], base: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    payload = payload if isinstance(payload, dict) else {}
    existing_product = payload.get("existing_product") if isinstance(payload.get("existing_product"), dict) else {}
    ingredient_image = str(payload.get("ingredient_image_data_url") or "").strip()
    nutrition_image = str(payload.get("nutrition_image_data_url") or "").strip()
    if not ingredient_image and not nutrition_image:
        return None

    evidence_blob = " ".join([
        str(existing_product.get("name") or ""),
        " ".join([str(item).strip() for item in _as_list(existing_product.get("categories")) if str(item).strip()]),
    ]).strip().lower()
    has_water_markers = _contains_any(evidence_blob, _CLEAN_WATER_MARKERS)
    mineral_water_markers = (
        "mineral water", "natural mineral water", "sparkling water", "carbonated water",
        "mineralwasser", "natürliches mineralwasser", "natuerliches mineralwasser",
        "kohlensäure", "kohlensaeure", "medium",
    )
    excluded_water_markers = ("juice", "saft", "soft drink", "soda", "cola", "energy", "flavour", "flavor", "limonade", "sirup", "syrup")
    has_mineral_water_markers = any(marker in evidence_blob for marker in mineral_water_markers)
    if not has_water_markers or not has_mineral_water_markers or any(marker in evidence_blob for marker in excluded_water_markers):
        return None

    base_payload = copy.deepcopy(base) if isinstance(base, dict) else {}
    nutrition = base_payload.get("nutrition_per_100") if isinstance(base_payload.get("nutrition_per_100"), dict) else {}
    if not isinstance(nutrition, dict):
        nutrition = {}
    for field in ("energy_kcal", "sugar_g", "salt_g", "sat_fat_g", "protein_g"):
        nutrition.setdefault(field, None)
    base_payload.setdefault("product_name", str(existing_product.get("name") or "").strip() or None)
    base_payload.setdefault("brand", str(existing_product.get("brand") or "").strip() or None)
    base_payload.setdefault("categories", _as_list(existing_product.get("categories")))
    base_payload.setdefault("ingredients_text", str(existing_product.get("name") or "").strip() or None)
    base_payload["nutrition_per_100"] = nutrition
    base_payload["confidence"] = str(base_payload.get("confidence") or "low").strip().lower() or "low"
    extracted_fields = [str(item).strip() for item in _as_list(base_payload.get("extracted_fields")) if str(item).strip()]
    extracted_fields.extend(["product_name", "brand", "categories", "ingredients_text", "nutrition_per_100", "composition_table_context"])
    base_payload["extracted_fields"] = list(dict.fromkeys(extracted_fields))
    notes = str(base_payload.get("notes") or "").strip()
    fallback_note = "Composition-table water fallback derived from existing product context after photo upload."
    base_payload["notes"] = f"{notes} {fallback_note}".strip() if notes else fallback_note
    base_payload["label_kind"] = "composition_table"
    base_payload.setdefault("composition_table_text", None)
    return _normalize_photo_extracted_payload(base_payload, payload)


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
                "confidence, extracted_fields, notes, label_kind, composition_table_text. "
                "Use null for unknown values. categories must be an array of short strings. "
                "ingredients_text must be a single cleaned string. confidence must be high, medium, or low. "
                "label_kind must be one of ingredients, nutrition, composition_table, front_label, mixed, unknown. "
                "If the product is water or mineral water and the label shows a mineral composition table instead of a classic nutrition or ingredients panel, "
                "interpret that as composition_table, extract the product_name and brand exactly as written without translating them, "
                "extract any visible composition_table_text, and infer short source-native categories when they are clearly visible. "
                "If no ingredients list is present but the label clearly indicates plain mineral water or mineral water with carbonation, "
                "you may return a short source-native water description in ingredients_text. "
                "Do not translate the product_name. If the image is unclear, still extract what is visible and note uncertainty."
            ),
        }
    ]
    if ingredient_image:
        content.append({"type": "input_text", "text": "Ingredient label / composition-table photo:"})
        content.append({"type": "input_image", "image_url": ingredient_image})
    if nutrition_image:
        content.append({"type": "input_text", "text": "Nutrition table / composition-table photo:"})
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
        return _photo_parsing_failed()
    return _normalize_photo_extracted_payload(parsed, payload)


def _analysis_mode(
    *,
    lookup_state: str,
    per100: Dict[str, Optional[float]],
    ingredients: List[Dict[str, Any]],
    ingredients_intelligence: Dict[str, Any],
    categories: Any,
) -> Tuple[str, str]:
    core_required = ("energy_kcal", "sugar_g", "salt_g", "saturated_fat_g")
    core_present = sum(1 for k in core_required if per100.get(k) is not None)
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

    if core_present <= 1:
        return "limited_estimate", "low"
    if lookup_state == "found_and_analyzable" and nutriments_present >= 3 and ingredients_present:
        return "full_analysis", "high"
    if evidence_points >= 4:
        return "partial_analysis", "high"
    if evidence_points >= 3:
        return "partial_analysis", "medium"
    if evidence_points >= 2:
        return "partial_analysis", "low"
    return "limited_estimate", "low"


def _clean_water_score_floor(
    normalized: Dict[str, Any],
    per100: Dict[str, Optional[float]],
    ingredients: List[Dict[str, Any]],
    ingredients_intelligence: Dict[str, Any],
    *,
    is_beverage: bool,
    analysis_state: str,
    confidence: str,
) -> Optional[int]:
    if not is_beverage:
        return None

    markers = ingredients_intelligence.get("markers") if isinstance(ingredients_intelligence, dict) else {}
    if not isinstance(markers, dict):
        markers = {}
    if any(int(markers.get(key) or 0) > 0 for key in ("sweeteners", "flavourings", "colorants", "preservatives", "emulsifiers_stabilizers", "caffeine", "e_numbers")):
        return None

    product_text = _normalized_product_text(normalized)
    if not _contains_any(product_text, _CLEAN_WATER_MARKERS):
        return None

    sugar = _to_float(per100.get("sugar_g"))
    salt = _to_float(per100.get("salt_g"))
    satfat = _to_float(per100.get("saturated_fat_g"))
    energy = _to_float(per100.get("energy_kcal"))
    if sugar is not None and sugar > 0.5:
        return None
    if salt is not None and salt > 0.1:
        return None
    if satfat is not None and satfat > 0.1:
        return None
    if energy is not None and energy > 5:
        return None

    allowed_ingredient_terms = ("water", "wasser", "eau", "νερό", "mineral", "carbon", "kohlensäure", "gaz", "διοξείδιο", "ανθρακ")
    cleaned_ingredients = [str(item.get("name") or "").strip().lower() for item in _as_list(ingredients) if isinstance(item, dict) and str(item.get("name") or "").strip()]
    if cleaned_ingredients and any(not any(term in item for term in allowed_ingredient_terms) for item in cleaned_ingredients):
        return None

    state = str(analysis_state or "").lower()
    tier = str(confidence or "").lower()
    if state == "full_analysis":
        return 99
    if tier == "high":
        return 97
    if tier == "medium":
        return 95
    return 93


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
    safety_lookup = _build_local_safety_lookup_payload(key, norm)
    product_categories = norm.get("categories") or norm.get("categories_tags") or []
    if isinstance(product_categories, str):
      product_categories = [c.strip() for c in product_categories.split(",") if c.strip()]
    ingredients_raw = _as_list(norm.get("ingredients"))
    ingredients_raw = _complete_simple_cheese_ingredients(norm, ingredients_raw)
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
        ingredients = _sanitize_ingredients_minimal(ingredients_raw)
        ingredients_intelligence = {
            "processing_score": None,
            "processing_label": "",
            "markers": {},
            "flags": [],
            "e_number_details": [],
            "sanitized_ingredients": ingredients,
        }
    try:
        per100 = _nutrients_per_100(norm)
    except Exception:
        per100 = {"energy_kcal": None, "sugar_g": None, "salt_g": None, "saturated_fat_g": None, "fiber_g": None, "protein_g": None, "fruits_veg_percent": None}
    ingredients_intelligence = _recalibrate_processing_intelligence(norm, per100, ingredients, ingredients_intelligence)
    score = _limited_estimate_score(per100, ingredients, ingredients_intelligence)
    clean_water_floor = _clean_water_score_floor(
        norm,
        per100,
        ingredients,
        ingredients_intelligence,
        is_beverage=is_bev,
        analysis_state="limited_estimate",
        confidence="low",
    )
    if isinstance(clean_water_floor, int):
        score = max(score, clean_water_floor)
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
    cap_adjustments = _whole_food_cap_adjustments(
        norm,
        per100,
        ingredients_intelligence,
        is_beverage=is_bev,
        analysis_state="limited_estimate",
        current_score=score,
    )
    cap_score = cap_adjustments.get("cap_score")
    if isinstance(cap_score, int):
        score = min(score, cap_score)
    score = int(round(_clamp(score, 1.0, 100.0)))
    lookup_missing = _lookup_missing_fields(norm, raw)
    qty = norm.get("quantity")
    if isinstance(qty, str) and qty.strip().startswith("0"):
        qty = None
    serving_amount = _to_float(_get_path(norm, "nutrition_per_100", "serving_size") or _get_path(norm, "serving", "value"))
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
        "alerts": [str(item.get("title") or "").strip() for item in safety_lookup.get("alerts", []) if str(item.get("title") or "").strip()],
        "safety_alerts_checked": bool(safety_lookup.get("checked")),
        "safety_alerts_source": safety_lookup.get("source"),
        "safety_alerts_has_matches": bool(safety_lookup.get("has_matches")),
        "safety_alerts": safety_lookup.get("alerts", []),
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
            "cap_adjustments": cap_adjustments,
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
            "safety_alerts_checked": bool(safety_lookup.get("checked")),
            "safety_alerts_source": safety_lookup.get("source"),
            "safety_alerts_has_matches": bool(safety_lookup.get("has_matches")),
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
    safety_lookup = _build_local_safety_lookup_payload(key, norm)
    alerts = [str(item.get("title") or "").strip() for item in safety_lookup.get("alerts", []) if str(item.get("title") or "").strip()]
    ingredients_raw = _complete_simple_cheese_ingredients(norm, _as_list(norm.get("ingredients")))

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
        ingredients_intelligence = _recalibrate_processing_intelligence(norm, per100, ingredients, ingredients_intelligence)
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
        nutrition_guard = _core_nutrition_guard(per100)
        base_score_available = True
        base_score_cap = nutrition_guard.get("base_score_cap")
        if isinstance(base_score_cap, int):
            base_score = min(base_score, base_score_cap)
            if isinstance(who_breakdown, dict):
                raw_who_score = who_breakdown.get("score")
                who_breakdown["raw_score"] = raw_who_score
                if isinstance(raw_who_score, (int, float)):
                    who_breakdown["score"] = min(int(raw_who_score), base_score_cap)
                who_breakdown["score_guard_applied"] = True
                who_breakdown["score_guard_reason"] = nutrition_guard.get("reason")
                who_breakdown["score_guard_cap"] = base_score_cap
        elif isinstance(who_breakdown, dict):
            who_breakdown["raw_score"] = who_breakdown.get("score")
            who_breakdown["score_guard_applied"] = False
            who_breakdown["score_guard_reason"] = nutrition_guard.get("reason")
            who_breakdown["score_guard_cap"] = None
        if int(nutrition_guard.get("present_count") or 0) <= 1:
            base_score_available = False
            if isinstance(who_breakdown, dict):
                who_breakdown["score"] = None
        pattern_adjustments = _pattern_score_adjustments(norm, per100, ingredients_intelligence, is_beverage=is_bev)
        balance_adjustments = _traditional_balance_adjustments(norm, per100, ingredients_intelligence, is_beverage=is_bev, lang=lang)
        score = base_score + int(pattern_adjustments.get("total_delta", 0) or 0) + int(balance_adjustments.get("total_delta", 0) or 0)
        score_cap = pattern_adjustments.get("score_cap")
        if isinstance(score_cap, int):
            score = min(score, score_cap)
        score = int(round(_clamp(score, 1.0, 100.0)))
        clean_water_floor = _clean_water_score_floor(
            norm,
            per100,
            ingredients,
            ingredients_intelligence,
            is_beverage=is_bev,
            analysis_state=analysis_state,
            confidence=analysis_confidence,
        )
        if analysis_state == "partial_analysis":
            if isinstance(clean_water_floor, int):
                score = max(score, clean_water_floor)
            else:
                score = _conservative_partial_score(score, analysis_confidence)
        elif analysis_state == "limited_estimate":
            score = _limited_estimate_score(per100, ingredients, ingredients_intelligence)
            if isinstance(clean_water_floor, int):
                score = max(score, clean_water_floor)

        breakdown["who_baseline"] = who_breakdown
        breakdown["who_weights"] = {"who": w_who, "hybrid": w_hyb}
        breakdown["pre_pattern_score"] = base_score if base_score_available else None
        breakdown["pre_pattern_score_raw"] = base_score
        breakdown["nutrition_completeness_guard"] = nutrition_guard
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
        cap_adjustments = _whole_food_cap_adjustments(
            norm,
            per100,
            ingredients_intelligence,
            is_beverage=is_bev,
            analysis_state=analysis_state,
            current_score=score,
        )
        cap_score = cap_adjustments.get("cap_score")
        if isinstance(cap_score, int):
            score = min(score, cap_score)
        breakdown["floor_adjustments"] = floor_adjustments
        breakdown["cap_adjustments"] = cap_adjustments
        breakdown["analysis_mode"] = {
            "state": analysis_state,
            "confidence": analysis_confidence,
        }

        why, tips = _build_explanations(per100, breakdown, is_bev, lang=lang)
        dq = _localize_data_quality_notes(_data_quality(norm, per100, bev_meta), lang)
        analysis_confidence = _align_analysis_confidence(analysis_state, analysis_confidence, dq)
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
        "safety_alerts_checked": bool(safety_lookup.get("checked")),
        "safety_alerts_source": safety_lookup.get("source"),
        "safety_alerts_has_matches": bool(safety_lookup.get("has_matches")),
        "safety_alerts": safety_lookup.get("alerts", []),
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
            "safety_alerts_checked": bool(safety_lookup.get("checked")),
            "safety_alerts_source": safety_lookup.get("source"),
            "safety_alerts_has_matches": bool(safety_lookup.get("has_matches")),
        },
    }


async def scan_product(key: str, lang: str = "en") -> Dict[str, Any]:
    started_at = time.perf_counter()
    timing: Dict[str, Any] = {
        "source": "scan_product",
        "key": str(key or "").strip(),
        "off_fetch_ok": False,
        "fallback_generated": False,
    }
    lang = lang if lang in SUPPORTED_LANGS else "en"
    key = (key or "").strip()
    if not key:
        err = _scan_error("INVALID_BARCODE", "Missing product id or barcode.", 400)
        err.update(_lookup_state_payload("invalid_barcode"))
        err["analysis_state"] = "insufficient_data"
        err["analysis_confidence"] = "low"
        timing["total_ms"] = int(round((time.perf_counter() - started_at) * 1000.0))
        return _attach_scan_timing(err, timing)
    if not _is_supported_lookup_key(key):
        err = _scan_error("INVALID_BARCODE", "Invalid barcode.", 400)
        err.update(_lookup_state_payload("invalid_barcode"))
        err["analysis_state"] = "insufficient_data"
        err["analysis_confidence"] = "low"
        timing["total_ms"] = int(round((time.perf_counter() - started_at) * 1000.0))
        return _attach_scan_timing(err, timing)

    cached_result = _scan_result_cache_get(key, lang)
    if isinstance(cached_result, dict):
        cached_timing = dict(cached_result.get("meta", {}).get("performance") or {})
        cached_timing.update({
            "cache_hit": True,
            "cache_layer": "scan_result",
            "total_ms": int(round((time.perf_counter() - started_at) * 1000.0)),
        })
        logger.info("scan timing key=%s cache=scan_result total_ms=%s", key, cached_timing["total_ms"])
        return _attach_scan_timing(cached_result, cached_timing)

    local_load_started = time.perf_counter()
    products = _load_json(PRODUCTS_FILE, [])
    if isinstance(products, dict) and isinstance(products.get("products"), list):
        products = products["products"]
    elif not isinstance(products, list):
        products = []
    rasff = _load_json(RASFF_FILE, [])
    timing["local_data_load_ms"] = int(round((time.perf_counter() - local_load_started) * 1000.0))

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
        timing["lookup_source"] = "local"
        local_serving_unit = str(_get_path(local, "serving_size", "unit") or "").strip().lower()
        local_nutrition_unit = str(_get_path(local, "nutrition_per_100", "unit") or "").strip().lower()
        if local_serving_unit == "ml" or local_nutrition_unit == "ml":
            curated_is_beverage = True
            curated_beverage_signal = "curated"

    if raw is None:
        off_fetch_started = time.perf_counter()
        try:
            off_result = await fetch_off_product(key)
            timing["openfoodfacts_fetch_ms"] = int(round((time.perf_counter() - off_fetch_started) * 1000.0))
            if off_result.ok and isinstance(off_result.payload, dict):
                raw = off_result.payload
                source = "openfoodfacts"
                matched_by = "barcode_or_key"
                timing["lookup_source"] = "openfoodfacts"
                timing["off_fetch_ok"] = True
            else:
                off_error = {
                    "status": int(off_result.status or 0),
                    "error": str(off_result.error or "").strip(),
                }
                timing["openfoodfacts_status"] = int(off_result.status or 0)
        except Exception:
            timing["openfoodfacts_fetch_ms"] = int(round((time.perf_counter() - off_fetch_started) * 1000.0))
            raw = None
            off_error = {"status": 502, "error": "OpenFoodFacts request failed"}

    if raw is None:
        status = int((off_error or {}).get("status") or 0)
        if status == 404:
            timing["fallback_generated"] = True
            result = _fallback_assessment_response(
                key=key,
                norm={"name": "Unknown product", "barcode": key},
                raw=None,
                source="openfoodfacts",
                matched_by="barcode_or_key",
                lang=lang,
                lookup_state="not_found",
            )
            result = await _finalize_scan_result_with_safety(result, key, {"barcode": key}, timing)
            timing["total_ms"] = int(round((time.perf_counter() - started_at) * 1000.0))
            logger.info("scan timing key=%s source=openfoodfacts status=404 total_ms=%s", key, timing["total_ms"])
            return _attach_scan_timing(result, timing)
        if status == 400:
            err = _scan_error("INVALID_BARCODE", "Invalid barcode.", 400)
            err.update(_lookup_state_payload("invalid_barcode"))
            err["analysis_state"] = "insufficient_data"
            err["analysis_confidence"] = "low"
            timing["total_ms"] = int(round((time.perf_counter() - started_at) * 1000.0))
            return _attach_scan_timing(err, timing)
        timing["fallback_generated"] = True
        result = _fallback_assessment_response(
            key=key,
            norm={"name": "Unknown product", "barcode": key},
            raw=None,
            source="openfoodfacts",
            matched_by="barcode_or_key",
            lang=lang,
            lookup_state="found_but_incomplete",
        )
        result = await _finalize_scan_result_with_safety(result, key, {"barcode": key}, timing)
        timing["total_ms"] = int(round((time.perf_counter() - started_at) * 1000.0))
        logger.info("scan timing key=%s fallback=incomplete total_ms=%s", key, timing["total_ms"])
        return _attach_scan_timing(result, timing)

    normalize_started = time.perf_counter()
    try:
        norm = _normalize(raw, source=source)
    except Exception:
        timing["fallback_generated"] = True
        result = _fallback_assessment_response(
            key=key,
            norm={"name": "Unknown product", "barcode": key},
            raw=raw if isinstance(raw, dict) else None,
            source=source,
            matched_by=matched_by,
            lang=lang,
            lookup_state="found_but_incomplete",
        )
        result = await _finalize_scan_result_with_safety(result, key, {"barcode": key}, timing)
        timing["normalize_ms"] = int(round((time.perf_counter() - normalize_started) * 1000.0))
        timing["total_ms"] = int(round((time.perf_counter() - started_at) * 1000.0))
        logger.info("scan timing key=%s normalize_error total_ms=%s", key, timing["total_ms"])
        return _attach_scan_timing(result, timing)
    timing["normalize_ms"] = int(round((time.perf_counter() - normalize_started) * 1000.0))
    if not isinstance(norm, dict) or not norm:
        timing["fallback_generated"] = True
        result = _fallback_assessment_response(
            key=key,
            norm={"name": "Unknown product", "barcode": key},
            raw=raw if isinstance(raw, dict) else None,
            source=source,
            matched_by=matched_by,
            lang=lang,
            lookup_state="found_but_incomplete",
        )
        result = await _finalize_scan_result_with_safety(result, key, {"barcode": key}, timing)
        timing["total_ms"] = int(round((time.perf_counter() - started_at) * 1000.0))
        return _attach_scan_timing(result, timing)
    enrichment_record = _get_product_enrichment(key)
    if enrichment_record:
        norm = _apply_product_enrichment(norm, enrichment_record)
        timing["enrichment_layer_applied"] = True
    else:
        timing["enrichment_layer_applied"] = False
    if not _has_minimum_product_data(norm):
        timing["fallback_generated"] = True
        result = _fallback_assessment_response(
            key=key,
            norm=norm,
            raw=raw if isinstance(raw, dict) else None,
            source=source,
            matched_by=matched_by,
            lang=lang,
            lookup_state="found_but_incomplete",
        )
        result = await _finalize_scan_result_with_safety(result, key, norm, timing)
        timing["total_ms"] = int(round((time.perf_counter() - started_at) * 1000.0))
        logger.info("scan timing key=%s min_data_incomplete total_ms=%s", key, timing["total_ms"])
        return _attach_scan_timing(result, timing)
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

    analyze_started = time.perf_counter()
    result = _analyze_normalized_product(
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
    result = await _finalize_scan_result_with_safety(result, key, norm, timing)
    timing["analysis_ms"] = int(round((time.perf_counter() - analyze_started) * 1000.0))
    timing["total_ms"] = int(round((time.perf_counter() - started_at) * 1000.0))
    timing["cache_hit"] = False
    logger.info(
        "scan timing key=%s source=%s total_ms=%s fetch_ms=%s normalize_ms=%s analysis_ms=%s local_ms=%s",
        key,
        str(source or "unknown"),
        timing.get("total_ms"),
        timing.get("openfoodfacts_fetch_ms"),
        timing.get("normalize_ms"),
        timing.get("analysis_ms"),
        timing.get("local_data_load_ms"),
    )
    final_result = _attach_scan_timing(result, timing)
    if isinstance(final_result, dict) and not final_result.get("error"):
        _scan_result_cache_set(key, lang, final_result)
    return final_result


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
        context_fallback = _build_photo_context_water_fallback(payload)
        if context_fallback is None:
            return extracted
        extracted = context_fallback
    else:
        context_fallback = _build_photo_context_water_fallback(payload, extracted if isinstance(extracted, dict) else None)
        if isinstance(context_fallback, dict):
            extracted = context_fallback

    existing_analysis = payload.get("existing_analysis") if isinstance(payload.get("existing_analysis"), dict) else {}
    existing_product = payload.get("existing_product") if isinstance(payload.get("existing_product"), dict) else {}
    existing_nutrition = existing_analysis.get("nutrition_per_100") if isinstance(existing_analysis.get("nutrition_per_100"), dict) else {}
    existing_meta = existing_analysis.get("meta") if isinstance(existing_analysis.get("meta"), dict) else {}
    existing_serving = existing_meta.get("serving") if isinstance(existing_meta.get("serving"), dict) else {}
    existing_key = str(existing_analysis.get("key") or payload.get("existing_key") or "").strip()
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
        enrichment_record = _persist_product_enrichment(
            barcode=existing_key,
            product_identity={
                "name": merged_payload.get("name") or existing_product.get("name"),
                "brand": merged_payload.get("brand") or existing_product.get("brand"),
                "quantity": merged_payload.get("quantity") or existing_product.get("quantity"),
            },
            extracted=extracted if isinstance(extracted, dict) else {},
            merged_payload=merged_payload,
            used_ingredient_photo=bool(str(payload.get("ingredient_image_data_url") or "").strip()),
            used_nutrition_photo=bool(str(payload.get("nutrition_image_data_url") or "").strip()),
        )
        result["key"] = existing_key or result.get("key")
        result["source"] = existing_analysis.get("source") or "photo"
        result["matched_by"] = "photo_enrichment"
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
            if enrichment_record:
                result["meta"]["enrichment_layer"] = {
                    "source": str(enrichment_record.get("source") or "user_photo_enrichment"),
                    "confidence": str(enrichment_record.get("confidence") or "low"),
                    "updated_at": str(enrichment_record.get("updated_at") or ""),
                    "stored_fields": (
                        enrichment_record.get("captured_payload")
                        if isinstance(enrichment_record.get("captured_payload"), dict)
                        else {}
                    ),
                }
            if isinstance(result.get("lookup_missing_fields"), list):
                result["meta"]["lookup_missing_fields"] = result["lookup_missing_fields"]
    return result
