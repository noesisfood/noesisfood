import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from app.services import scanner_service as ss


class SafetyObservabilityMatrixTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        ss._SAFETY_LOOKUP_CACHE.clear()
        ss._SAFETY_HTTP_CACHE.clear()

    async def test_exact_barcode_batch_specific_lebensmittelwarnung_case_signals(self) -> None:
        barcode = "4337256716499"
        lmw_entry = {
            "title": "REWE Beste Wahl Italienisches Pfannengemüse, 500 Gramm",
            "summary": "Verified barcode batch recall.",
            "url": "https://example.test/lmw/rewe",
            "published_at": "Mon, 16 Mar 2026 14:45:00 +0100",
            "detail_text": (
                "EAN 4337256716499 Chargennummer / Los-Kennzeichnung: L 6009 "
                "Grund der Meldung: Fremdkörper Mindestens haltbar bis Ende: 01.2028"
            ),
            "text_blob": (
                "EAN 4337256716499 Chargennummer / Los-Kennzeichnung: L 6009 "
                "Grund der Meldung: Fremdkörper Mindestens haltbar bis Ende: 01.2028"
            ),
            "barcodes": [barcode],
            "batch_specific": True,
            "batch": "L 6009",
            "lot": "L 6009",
            "best_before": "01.2028",
        }
        with (
            patch.object(ss, "_fetch_lebensmittelwarnung_entries", AsyncMock(return_value={"checked": True, "entries": [lmw_entry]})),
            patch.object(ss, "_enrich_lebensmittelwarnung_recent_entries", AsyncMock(return_value=[lmw_entry])),
            patch.object(
                ss,
                "_lookup_rasff_public_alerts",
                AsyncMock(
                    return_value={
                        "checked": True,
                        "source": "rasff_dg_sante_api",
                        "has_matches": False,
                        "alerts": [],
                        "observability": {
                            "source_checked": {"rasff_dg_sante_api": 1},
                            "source_matched": {},
                            "confidence_assigned": {},
                            "batch_scope_explicit": 0,
                            "duplicate_collapsed": 0,
                            "fallback_used": False,
                            "fetch_count": {"rasff_dg_sante_api": 1},
                            "page_count": {"rasff_dg_sante_api": 1},
                            "no_match_reason": {"rasff_dg_sante_api": "no_candidate_above_threshold"},
                        },
                    }
                ),
            ),
        ):
            result = await ss._lookup_external_safety_alerts(barcode, {"name": "Gemüsepfanne", "brand": "Rewe", "categories": []})

        self.assertEqual(result["source"], "lebensmittelwarnung_de")
        self.assertEqual(len(result["alerts"]), 1)
        obs = result["observability"]
        self.assertEqual(obs["source_checked"]["lebensmittelwarnung_de"], 1)
        self.assertEqual(obs["source_checked"]["rasff_dg_sante_api"], 1)
        self.assertEqual(obs["source_matched"]["lebensmittelwarnung_de"], 1)
        self.assertEqual(obs["confidence_assigned"]["conditional"], 1)
        self.assertEqual(obs["batch_scope_explicit"], 1)
        self.assertEqual(obs["duplicate_collapsed"], 0)
        self.assertEqual(obs["no_match_reason"]["rasff_dg_sante_api"], "no_candidate_above_threshold")

    async def test_high_confidence_rasff_official_match_bucket(self) -> None:
        entry = {
            "title": "Dried figs",
            "summary": "Ochratoxin A in dried figs from Turkiye",
            "product_name": "Dried figs",
            "product_variants": ["dried figs"],
            "subject": "Ochratoxin A in dried figs from Turkiye",
            "category": "fruits and vegetables",
            "hazard": "ochratoxin A - {mycotoxins}",
            "risk": "serious",
            "reference": "2026.0095",
            "published_at": "2026-01-07T00:00:00",
            "url": "https://example.test/rasff/dried-figs",
            "text_blob": "Dried figs Ochratoxin A in dried figs from Turkiye fruits and vegetables 2026.0095",
        }
        with patch.object(
            ss,
            "_fetch_rasff_public_entries",
            AsyncMock(
                return_value={
                    "checked": True,
                    "entries": [entry],
                    "observability": {
                        "source_checked": {"rasff_dg_sante_api": 1},
                        "source_matched": {},
                        "confidence_assigned": {},
                        "batch_scope_explicit": 0,
                        "duplicate_collapsed": 0,
                        "fallback_used": False,
                        "fetch_count": {"rasff_dg_sante_api": 1},
                        "page_count": {"rasff_dg_sante_api": 1},
                        "no_match_reason": {},
                    },
                }
            ),
        ):
            lookup = await ss._lookup_rasff_public_alerts({"name": "Dried figs", "brand": "", "categories": ["fruits and vegetables"]})
            merged = ss._merge_safety_lookup_payloads(lookup)

        self.assertTrue(lookup["has_matches"])
        self.assertEqual(lookup["alerts"][0]["confidence"], "high")
        self.assertEqual(merged["observability"]["confidence_assigned"]["high"], 1)
        self.assertEqual(merged["observability"]["source_matched"]["rasff_dg_sante_api"], 1)

    async def test_medium_confidence_rasff_probable_match_bucket(self) -> None:
        entry = {
            "title": "Bananas",
            "summary": "Chlorpyrifos in bananas from Ecuador",
            "product_name": "Bananas",
            "product_variants": ["bananas"],
            "subject": "Chlorpyrifos in bananas from Ecuador",
            "category": "fruits and vegetables",
            "hazard": "chlorpyrifos unauthorised substance",
            "risk": "potential risk",
            "reference": "2025.8922",
            "published_at": "2025-11-14T00:00:00",
            "url": "https://example.test/rasff/bananas",
            "text_blob": "Bananas chlorpyrifos in bananas from Ecuador fruits and vegetables 2025.8922",
        }
        with patch.object(
            ss,
            "_fetch_rasff_public_entries",
            AsyncMock(return_value={"checked": True, "entries": [entry], "observability": ss._new_safety_observability()}),
        ):
            lookup = await ss._lookup_rasff_public_alerts({"name": "bulk bananas", "brand": "", "categories": ["fruits and vegetables"]})
            merged = ss._merge_safety_lookup_payloads(lookup)

        self.assertTrue(lookup["has_matches"])
        self.assertEqual(lookup["alerts"][0]["confidence"], "medium")
        self.assertEqual(merged["observability"]["confidence_assigned"]["medium"], 1)

    async def test_low_confidence_rasff_non_visible_case_bucket(self) -> None:
        weak_entry = {
            "title": "Protein bar",
            "summary": "Packaging issue only.",
            "product_name": "Protein bar",
            "product_variants": ["protein bar"],
            "subject": "Packaging issue in protein bar",
            "category": "dietetic foods food supplements fortified foods",
            "hazard": "",
            "risk": "no risk",
            "reference": "2025.0002",
            "published_at": "2026-02-01T00:00:00",
            "url": "https://example.test/rasff/protein-bar",
            "text_blob": "Protein bar packaging issue dietetic foods 2025.0002",
        }
        with patch.object(
            ss,
            "_fetch_rasff_public_entries",
            AsyncMock(return_value={"checked": True, "entries": [weak_entry], "observability": ss._new_safety_observability()}),
        ):
            lookup = await ss._lookup_rasff_public_alerts({"name": "chicken", "brand": "", "categories": ["poultry meat and poultry meat products"]})

        self.assertTrue(lookup["checked"])
        self.assertFalse(lookup["has_matches"])
        self.assertEqual(lookup["alerts"], [])
        self.assertEqual(lookup["observability"]["no_match_reason"]["rasff_dg_sante_api"], "no_candidate_above_threshold")

    def test_merged_source_duplicate_collapse_case_bucket(self) -> None:
        lookup_a = {
            "checked": True,
            "source": "lebensmittelwarnung_de",
            "has_matches": True,
            "alerts": [
                {
                    "title": "Shared Alert",
                    "summary": "Source A",
                    "url": "https://example.test/alert",
                    "scope": "product",
                    "batch_specific": False,
                    "source": "lebensmittelwarnung_de",
                    "reference": "2026.0001",
                    "match_score": 100,
                    "confidence": "high",
                    "severity": "high",
                }
            ],
            "observability": {"source_checked": {"lebensmittelwarnung_de": 1}, "source_matched": {"lebensmittelwarnung_de": 1}, "confidence_assigned": {}, "batch_scope_explicit": 0, "duplicate_collapsed": 0, "fallback_used": False, "fetch_count": {}, "page_count": {}, "no_match_reason": {}},
        }
        lookup_b = {
            "checked": True,
            "source": "rasff_dg_sante_api",
            "has_matches": True,
            "alerts": [
                {
                    "title": "Shared Alert",
                    "summary": "Source B",
                    "url": "https://example.test/alert",
                    "scope": "product",
                    "batch_specific": False,
                    "source": "rasff_dg_sante_api",
                    "reference": "2026.0001",
                    "match_score": 80,
                    "confidence": "medium",
                    "severity": "medium",
                }
            ],
            "observability": {"source_checked": {"rasff_dg_sante_api": 1}, "source_matched": {"rasff_dg_sante_api": 1}, "confidence_assigned": {}, "batch_scope_explicit": 0, "duplicate_collapsed": 0, "fallback_used": False, "fetch_count": {}, "page_count": {}, "no_match_reason": {}},
        }
        merged = ss._merge_safety_lookup_payloads(lookup_a, lookup_b)

        self.assertEqual(len(merged["alerts"]), 1)
        self.assertEqual(merged["observability"]["duplicate_collapsed"], 1)
        self.assertEqual(merged["source"], "multi_source_safety")

    async def test_clean_no_alert_case_bucket(self) -> None:
        with (
            patch.object(ss, "_fetch_lebensmittelwarnung_entries", AsyncMock(return_value={"checked": True, "entries": []})),
            patch.object(ss, "_enrich_lebensmittelwarnung_recent_entries", AsyncMock(return_value=[])),
            patch.object(
                ss,
                "_lookup_rasff_public_alerts",
                AsyncMock(return_value={"checked": True, "source": "rasff_dg_sante_api", "has_matches": False, "alerts": [], "observability": {"source_checked": {"rasff_dg_sante_api": 1}, "source_matched": {}, "confidence_assigned": {}, "batch_scope_explicit": 0, "duplicate_collapsed": 0, "fallback_used": False, "fetch_count": {}, "page_count": {}, "no_match_reason": {"rasff_dg_sante_api": "no_candidate_above_threshold"}}}),
            ),
        ):
            lookup = await ss._lookup_external_safety_alerts("1111111111111", {"name": "Clean Product", "brand": "", "categories": []})
            finalized = await ss._finalize_scan_result_with_safety(
                {"lookup_state": "found_but_incomplete", "analysis_state": "limited_estimate", "meta": {}},
                "1111111111111",
                {"name": "Clean Product", "brand": "", "categories": []},
                {},
            )

        self.assertTrue(lookup["checked"])
        self.assertFalse(lookup["has_matches"])
        self.assertEqual(lookup["alerts"], [])
        self.assertEqual(lookup["source"], "multi_source_safety")
        self.assertTrue(finalized["safety_observability"]["fallback_used"])

    async def test_pagination_recency_boundary_case_bucket(self) -> None:
        calls = []

        async def fake_fetch(url: str, *, timeout_sec: float = 5.5):
            calls.append(url)
            if len(calls) == 1:
                return {
                    "value": [
                        {
                            "NOTIF_ID": 1,
                            "NOTIFICATION_REFERENCE": "2026.1000",
                            "NOTIF_DATE": "2026-03-10T00:00:00",
                            "PRODUCT_NAME": "Recent product",
                            "PRODUCT_CATEGORY_DESC": "confectionery",
                            "NOTIF_SUBJECT": "Recent subject",
                            "RISK_DECISION_DESC": "serious",
                            "HAZARD_CATEGORY_NAME": "hazard",
                        }
                    ],
                    "nextLink": "page-2",
                }
            return {
                "value": [
                    {
                        "NOTIF_ID": 2,
                        "NOTIFICATION_REFERENCE": "2025.01",
                        "NOTIF_DATE": "2025-06-01T00:00:00",
                        "PRODUCT_NAME": "Boundary product",
                        "PRODUCT_CATEGORY_DESC": "confectionery",
                        "NOTIF_SUBJECT": "Boundary subject",
                        "RISK_DECISION_DESC": "serious",
                        "HAZARD_CATEGORY_NAME": "hazard",
                    }
                ],
                "nextLink": "page-3",
            }

        fixed_now = 1773705600.0  # 2026-03-17T00:00:00 local timestamp equivalent for deterministic cutoff
        with (
            patch.object(ss, "_fetch_safety_url_json", AsyncMock(side_effect=fake_fetch)),
            patch("app.services.scanner_service.time.time", return_value=fixed_now),
        ):
            result = await ss._fetch_rasff_public_entries()

        self.assertTrue(result["checked"])
        self.assertEqual(len(calls), 2)
        self.assertEqual(result["observability"]["fetch_count"]["rasff_dg_sante_api"], 2)
        self.assertEqual(result["observability"]["page_count"]["rasff_dg_sante_api"], 2)


class SafetyLocalizationMatrixTests(unittest.TestCase):
    def test_el_en_de_fr_localization_invariant_case(self) -> None:
        content = Path("app/frontend/index.html").read_text(encoding="utf-8")
        self.assertEqual(content.count("safety_source_local_index:"), 4)
        self.assertEqual(content.count("safety_source_rasff_dg_sante_api:"), 4)
        self.assertEqual(content.count("safety_source_lebensmittelwarnung_de:"), 4)
        self.assertEqual(content.count("safety_source_multi_source_safety:"), 4)
        self.assertEqual(content.count("safety_match_batch:"), 4)


if __name__ == "__main__":
    unittest.main()
