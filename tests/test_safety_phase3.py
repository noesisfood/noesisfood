import time as pytime
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from app.services import scanner_service as ss


class SafetyRegressionTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        ss._SAFETY_LOOKUP_CACHE.clear()
        ss._SAFETY_HTTP_CACHE.clear()

    async def test_barcode_4337256716499_remains_batch_specific_lebensmittelwarnung_match(self) -> None:
        barcode = "4337256716499"
        entry = {
            "title": "REWE Beste Wahl Italienisches Pfannengemüse, 500 Gramm",
            "summary": "Recall entry for verified barcode path.",
            "url": "https://example.test/lebensmittelwarnung/rewe",
            "published_at": "Mon, 16 Mar 2026 14:45:00 +0100",
            "detail_text": (
                "EAN 4337256716499 Chargennummer / Los-Kennzeichnung: L 6009 "
                "Grund der Meldung: Fremdkörper Mindestens haltbar bis Ende: 01.2028"
            ),
            "text_blob": (
                "REWE Beste Wahl Italienisches Pfannengemüse, 500 Gramm "
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
            patch.object(ss, "_fetch_lebensmittelwarnung_entries", AsyncMock(return_value={"checked": True, "entries": [entry]})),
            patch.object(ss, "_enrich_lebensmittelwarnung_recent_entries", AsyncMock(return_value=[entry])),
            patch.object(
                ss,
                "_lookup_rasff_public_alerts",
                AsyncMock(return_value={"checked": True, "source": "rasff_dg_sante_api", "has_matches": False, "alerts": []}),
            ),
        ):
            result = await ss._lookup_external_safety_alerts(
                barcode,
                {"name": "Gemüsepfanne", "brand": "Rewe", "categories": []},
            )

        self.assertTrue(result["checked"])
        self.assertTrue(result["has_matches"])
        self.assertEqual(result["source"], "lebensmittelwarnung_de")
        self.assertEqual(len(result["alerts"]), 1)
        alert = result["alerts"][0]
        self.assertTrue(alert["batch_specific"])
        self.assertEqual(alert["scope"], "batch")
        self.assertEqual(alert["batch"], "L 6009")
        self.assertEqual(alert["lot"], "L 6009")
        self.assertEqual(alert["best_before"], "01.2028")
        self.assertEqual(alert["confidence"], "conditional")

    async def test_batch_specific_wording_only_when_supported_by_source_evidence(self) -> None:
        barcode = "4337256716499"
        entry = {
            "title": "Generic product-level recall",
            "summary": "EAN 4337256716499 product recall notice.",
            "url": "https://example.test/lebensmittelwarnung/product",
            "published_at": "Mon, 16 Mar 2026 14:45:00 +0100",
            "detail_text": "EAN 4337256716499 product recall notice with no batch data.",
            "text_blob": "Generic product-level recall EAN 4337256716499 with no lot details.",
            "barcodes": [barcode],
        }
        with (
            patch.object(ss, "_fetch_lebensmittelwarnung_entries", AsyncMock(return_value={"checked": True, "entries": [entry]})),
            patch.object(ss, "_enrich_lebensmittelwarnung_recent_entries", AsyncMock(return_value=[entry])),
            patch.object(
                ss,
                "_lookup_rasff_public_alerts",
                AsyncMock(return_value={"checked": True, "source": "rasff_dg_sante_api", "has_matches": False, "alerts": []}),
            ),
        ):
            result = await ss._lookup_external_safety_alerts(
                barcode,
                {"name": "Gemüsepfanne", "brand": "Rewe", "categories": []},
            )

        alert = result["alerts"][0]
        self.assertFalse(alert["batch_specific"])
        self.assertEqual(alert["scope"], "product")
        self.assertNotEqual(alert["confidence"], "conditional")

    def test_duplicate_alerts_from_merged_sources_collapse_to_one_visible_alert(self) -> None:
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
        }

        merged = ss._merge_safety_lookup_payloads(lookup_a, lookup_b)

        self.assertTrue(merged["checked"])
        self.assertTrue(merged["has_matches"])
        self.assertEqual(merged["source"], "multi_source_safety")
        self.assertEqual(len(merged["alerts"]), 1)
        self.assertEqual(set(merged["alerts"][0]["sources"]), {"lebensmittelwarnung_de", "rasff_dg_sante_api"})

    def test_rasff_confidence_guardrails_cover_high_medium_low(self) -> None:
        exact_entry = {
            "title": "Dried figs",
            "product_name": "Dried figs",
            "product_variants": ["dried figs"],
            "subject": "Ochratoxin A in dried figs from Turkiye",
            "category": "fruits and vegetables",
            "hazard": "ochratoxin A - {mycotoxins}",
            "risk": "serious",
            "reference": "2026.0095",
            "text_blob": "Dried figs Ochratoxin A in dried figs from Turkiye fruits and vegetables 2026.0095",
        }
        family_entry = {
            "title": "Bananas",
            "product_name": "Bananas",
            "product_variants": ["bananas"],
            "subject": "Chlorpyrifos in bananas from Ecuador",
            "category": "fruits and vegetables",
            "hazard": "chlorpyrifos unauthorised substance",
            "risk": "potential risk",
            "reference": "2025.8922",
            "text_blob": "Bananas chlorpyrifos in bananas from Ecuador fruits and vegetables 2025.8922",
        }
        weak_entry = {
            "title": "Protein bar",
            "product_name": "Protein bar",
            "product_variants": ["protein bar"],
            "subject": "Packaging issue in protein bar",
            "category": "dietetic foods food supplements fortified foods",
            "hazard": "",
            "risk": "no risk",
            "reference": "2025.0002",
            "text_blob": "Protein bar packaging issue dietetic foods 2025.0002",
        }

        exact_score, exact_confidence = ss._score_rasff_public_alert_candidate(exact_entry, "Dried figs", "", "fruits and vegetables")
        family_score, family_confidence = ss._score_rasff_public_alert_candidate(family_entry, "bulk bananas", "", "fruits and vegetables")
        weak_score, weak_confidence = ss._score_rasff_public_alert_candidate(weak_entry, "chicken", "", "poultry meat and poultry meat products")

        self.assertGreaterEqual(exact_score, 96)
        self.assertEqual(exact_confidence, "high")
        self.assertGreaterEqual(family_score, 76)
        self.assertEqual(family_confidence, "medium")
        self.assertLess(weak_score, 58)
        self.assertEqual(weak_confidence, "")

    async def test_low_confidence_rasff_match_does_not_render_visible_alert(self) -> None:
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
        with patch.object(ss, "_fetch_rasff_public_entries", AsyncMock(return_value={"checked": True, "entries": [weak_entry]})):
            result = await ss._lookup_rasff_public_alerts(
                {"name": "chicken", "brand": "", "categories": ["poultry meat and poultry meat products"]}
            )

        self.assertTrue(result["checked"])
        self.assertFalse(result["has_matches"])
        self.assertEqual(result["alerts"], [])

    async def test_bounded_rasff_pagination_and_recency_behavior_remains_intact(self) -> None:
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
                        "NOTIFICATION_REFERENCE": "2025.1000",
                        "NOTIF_DATE": "2025-06-01T00:00:00",
                        "PRODUCT_NAME": "Old product",
                        "PRODUCT_CATEGORY_DESC": "confectionery",
                        "NOTIF_SUBJECT": "Old subject",
                        "RISK_DECISION_DESC": "serious",
                        "HAZARD_CATEGORY_NAME": "hazard",
                    }
                ],
                "nextLink": "page-3",
            }

        fixed_now = pytime.mktime(pytime.strptime("2026-03-17T00:00:00", "%Y-%m-%dT%H:%M:%S"))
        with (
            patch.object(ss, "_fetch_safety_url_json", AsyncMock(side_effect=fake_fetch)),
            patch("app.services.scanner_service.time.time", return_value=fixed_now),
        ):
            result = await ss._fetch_rasff_public_entries()

        self.assertTrue(result["checked"])
        self.assertEqual(len(calls), 2)
        self.assertEqual(len(result["entries"]), 2)


class SafetyLocalizationRegressionTests(unittest.TestCase):
    def test_el_en_de_fr_safety_labels_remain_intact(self) -> None:
        content = Path("app/frontend/index.html").read_text(encoding="utf-8")
        self.assertEqual(content.count("safety_source_local_index:"), 4)
        self.assertEqual(content.count("safety_source_rasff_dg_sante_api:"), 4)
        self.assertEqual(content.count("safety_source_efet_gr:"), 4)
        self.assertEqual(content.count("safety_source_lebensmittelwarnung_de:"), 4)
        self.assertEqual(content.count("safety_source_multi_source_safety:"), 4)
        self.assertEqual(content.count("safety_match_batch:"), 4)


if __name__ == "__main__":
    unittest.main()
