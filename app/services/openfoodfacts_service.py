
# app/services/openfoodfacts_service.py

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional
import time

import httpx

# OpenFoodFacts endpoint (v2)
OFF_BASE = "https://world.openfoodfacts.org"
OFF_TIMEOUT_SEC = 12.0

# Πολύ απλό in-memory cache (για dev)
_CACHE: Dict[str, Dict[str, Any]] = {}
_CACHE_TTL_SEC = 10 * 60  # 10 λεπτά


@dataclass
class OFFResult:
    ok: bool
    status: int
    payload: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


def _now() -> float:
    return time.time()


def _cache_get(key: str) -> Optional[Dict[str, Any]]:
    item = _CACHE.get(key)
    if not item:
        return None
    if _now() - item["ts"] > _CACHE_TTL_SEC:
        _CACHE.pop(key, None)
        return None
    return item["data"]


def _cache_set(key: str, data: Dict[str, Any]) -> None:
    _CACHE[key] = {"ts": _now(), "data": data}


async def fetch_off_product(
    barcode: str,
    user_agent: str = "NoesisFood/0.1 (dev; contact: local)",
) -> OFFResult:
    """
    Fetch product data from OpenFoodFacts by barcode (EAN/UPC).
    Returns OFFResult(ok, status, payload, error).

    Note:
    - OFF v2 returns a JSON with keys like: "status", "product", "code"
    - status == 1 means found
    """
    barcode = str(barcode).strip()
    if not barcode:
        return OFFResult(ok=False, status=400, error="Empty barcode")

    cache_key = f"off:{barcode}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return OFFResult(ok=True, status=200, payload=cached)

    url = f"{OFF_BASE}/api/v2/product/{barcode}.json"

    headers = {
        "User-Agent": user_agent,
        "Accept": "application/json",
    }

    try:
        async with httpx.AsyncClient(timeout=OFF_TIMEOUT_SEC, follow_redirects=True) as client:
            r = await client.get(url, headers=headers)
    except httpx.TimeoutException:
        return OFFResult(ok=False, status=504, error="OpenFoodFacts timeout")
    except Exception as e:
        return OFFResult(ok=False, status=502, error=f"OpenFoodFacts request failed: {e}")

    if r.status_code != 200:
        return OFFResult(ok=False, status=r.status_code, error=f"OpenFoodFacts HTTP {r.status_code}")

    try:
        data = r.json()
    except Exception:
        return OFFResult(ok=False, status=502, error="Invalid JSON from OpenFoodFacts")

    # OFF: status 1 = found, status 0 = not found
    if str(data.get("status")) != "1" and data.get("product") is None:
        # Μερικές φορές το OFF μπορεί να μην έχει "product" όταν δεν βρει αποτέλεσμα
        return OFFResult(ok=False, status=404, error="Product not found in OpenFoodFacts")

    _cache_set(cache_key, data)
    return OFFResult(ok=True, status=200, payload=data)