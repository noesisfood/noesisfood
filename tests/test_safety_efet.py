import unittest
from unittest.mock import AsyncMock, patch

from app.services import scanner_service as ss


class EfetSafetyIntegrationTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        ss._SAFETY_LOOKUP_CACHE.clear()
        ss._SAFETY_HTTP_CACHE.clear()

    def test_normalize_efet_entry_extracts_expected_fields(self) -> None:
        html = """
        <html>
          <head>
            <meta property="og:title" content="Δελτίο Τύπου - Ανάκληση προϊόντος Μπάρες Δημητριακών" />
            <meta property="article:published_time" content="2026-03-10T09:30:00Z" />
          </head>
          <body>
            <article>
              Ο ΕΦΕΤ και η εταιρείας ACME FOODS ανακαλεί το προϊόν Μπάρες Δημητριακών 120 g
              λόγω παρουσίας αλλεργιογόνου ουσίας. Παρτίδα: LOT123.
              Ανάλωση κατά προτίμηση πριν από 10/12/2026. EAN 5201234567890.
            </article>
          </body>
        </html>
        """

        entry = ss._normalize_efet_entry("https://www.efet.gr/example", html)

        self.assertIsInstance(entry, dict)
        self.assertEqual(entry["source"], "efet_gr")
        self.assertEqual(entry["source_label"], "EFET")
        self.assertEqual(entry["published_at"], "2026-03-10T09:30:00Z")
        self.assertEqual(entry["product_name"], "Μπάρες Δημητριακών")
        self.assertIn("ACME FOODS", entry["company"] or "")
        self.assertEqual(entry["packaging"], "120 g")
        self.assertTrue(entry["batch_specific"])
        self.assertTrue(bool(entry["batch"]) or bool(entry["lot"]) or bool(entry["best_before"]))
        self.assertEqual(entry["best_before"], "10/12/2026")
        self.assertIn("5201234567890", entry["barcodes"])
        self.assertIn("::", str(entry.get("overlap_key") or ""))

    def test_normalize_efet_entry_extracts_reference_and_company_boundary(self) -> None:
        html = """
        <html>
          <head>
            <title>Ανάκληση προϊόντος Ζελεδάκια Φρούτων</title>
          </head>
          <body>
            Η εταιρεία SWEET GOODS A.E. ανακαλεί το προϊόν Ζελεδάκια Φρούτων 250 g
            λόγω παρουσίας μη εγκεκριμένου πρόσθετου. Η ενημέρωση έγινε μέσω RASFF 2026.4321.
          </body>
        </html>
        """

        entry = ss._normalize_efet_entry("https://www.efet.gr/example-ref", html)

        self.assertEqual(entry["reference"], "2026.4321")
        self.assertIn("SWEET GOODS A", entry["company"] or "")
        self.assertEqual(entry["packaging"], "250 g")

    async def test_lookup_efet_alerts_returns_medium_match_conservatively(self) -> None:
        entry = {
            "title": "Ανάκληση προϊόντος Greek Sesame Bar",
            "summary": "ACME FOODS | 120 g | λόγω παρουσίας αλλεργιογόνου ουσίας",
            "url": "https://www.efet.gr/example-1",
            "published_at": "2026-03-10T09:30:00Z",
            "product_name": "Greek Sesame Bar",
            "product_variants": ["greek sesame bar"],
            "company": "ACME FOODS",
            "packaging": "120 g",
            "hazard": "παρουσία αλλεργιογόνου ουσίας",
            "batch_specific": False,
            "batch": None,
            "lot": None,
            "best_before": None,
            "barcodes": [],
            "text_blob": "Greek Sesame Bar ACME FOODS 120 g παρουσία αλλεργιογόνου ουσίας snack bars",
            "source": "efet_gr",
            "source_label": "EFET",
        }
        with patch.object(
            ss,
            "_fetch_efet_entries",
            AsyncMock(return_value={"checked": True, "entries": [entry], "observability": ss._new_safety_observability()}),
        ):
            lookup = await ss._lookup_efet_alerts("0000000000000", {"name": "Greek Sesame Bar", "brand": "ACME", "categories": ["snack bars"]})

        self.assertTrue(lookup["checked"])
        self.assertTrue(lookup["has_matches"])
        self.assertEqual(lookup["source"], "efet_gr")
        self.assertEqual(len(lookup["alerts"]), 1)
        alert = lookup["alerts"][0]
        self.assertEqual(alert["scope"], "product")
        self.assertFalse(alert["batch_specific"])
        self.assertIn(alert["confidence"], {"high", "medium"})

    async def test_lookup_efet_alerts_uses_conditional_only_for_explicit_batch_scope(self) -> None:
        entry = {
            "title": "Ανάκληση προϊόντος Sesame Spread",
            "summary": "LOT A55 | Best before 11/11/2026",
            "url": "https://www.efet.gr/example-2",
            "published_at": "2026-03-11T09:30:00Z",
            "product_name": "Sesame Spread",
            "product_variants": ["sesame spread"],
            "company": "ACME FOODS",
            "packaging": "350 g",
            "hazard": "presence of foreign body",
            "batch_specific": True,
            "batch": "A55",
            "lot": "A55",
            "best_before": "11/11/2026",
            "barcodes": [],
            "text_blob": "Sesame Spread ACME FOODS 350 g batch A55 best before 11/11/2026 foreign body",
            "source": "efet_gr",
            "source_label": "EFET",
        }
        with patch.object(
            ss,
            "_fetch_efet_entries",
            AsyncMock(return_value={"checked": True, "entries": [entry], "observability": ss._new_safety_observability()}),
        ):
            lookup = await ss._lookup_efet_alerts("0000000000000", {"name": "Sesame Spread", "brand": "ACME", "categories": ["spreads"]})

        self.assertTrue(lookup["has_matches"])
        alert = lookup["alerts"][0]
        self.assertEqual(alert["scope"], "batch")
        self.assertTrue(alert["batch_specific"])
        self.assertEqual(alert["confidence"], "conditional")

    async def test_lookup_efet_alerts_sets_no_match_reason_for_weak_similarity(self) -> None:
        entry = {
            "title": "Ανάκληση προϊόντος Protein Bar",
            "summary": "generic packaging issue",
            "url": "https://www.efet.gr/example-3",
            "published_at": "2026-03-12T09:30:00Z",
            "product_name": "Protein Bar",
            "product_variants": ["protein bar"],
            "company": "ACME FOODS",
            "packaging": "60 g",
            "hazard": "",
            "batch_specific": False,
            "batch": None,
            "lot": None,
            "best_before": None,
            "barcodes": [],
            "text_blob": "Protein Bar ACME FOODS packaging issue 60 g",
            "source": "efet_gr",
            "source_label": "EFET",
        }
        with patch.object(
            ss,
            "_fetch_efet_entries",
            AsyncMock(return_value={"checked": True, "entries": [entry], "observability": ss._new_safety_observability()}),
        ):
            lookup = await ss._lookup_efet_alerts("0000000000000", {"name": "chicken", "brand": "", "categories": ["poultry meat and poultry meat products"]})

        self.assertTrue(lookup["checked"])
        self.assertFalse(lookup["has_matches"])
        self.assertEqual(lookup["alerts"], [])
        self.assertEqual(lookup["observability"]["no_match_reason"]["efet_gr"], "no_candidate_above_threshold")

    async def test_external_lookup_preserves_verified_barcode_path_with_efet_active(self) -> None:
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
            patch.object(ss, "_lookup_rasff_public_alerts", AsyncMock(return_value={"checked": True, "source": "rasff_dg_sante_api", "has_matches": False, "alerts": [], "observability": ss._new_safety_observability()})),
            patch.object(ss, "_lookup_efet_alerts", AsyncMock(return_value={"checked": True, "source": "efet_gr", "has_matches": False, "alerts": [], "observability": ss._new_safety_observability()})),
        ):
            result = await ss._lookup_external_safety_alerts(barcode, {"name": "Gemüsepfanne", "brand": "Rewe", "categories": []})

        self.assertEqual(result["source"], "lebensmittelwarnung_de")
        self.assertEqual(len(result["alerts"]), 1)
        alert = result["alerts"][0]
        self.assertTrue(alert["batch_specific"])
        self.assertEqual(alert["scope"], "batch")
        self.assertEqual(alert["confidence"], "conditional")

    def test_efet_rasff_overlap_dedupes_to_one_visible_alert(self) -> None:
        rasff_lookup = {
            "checked": True,
            "source": "rasff_dg_sante_api",
            "has_matches": True,
            "alerts": [
                {
                    "title": "Fruit gummies",
                    "summary": "RASFF alert",
                    "url": "https://webgate.ec.europa.eu/rasff-window/screen/search",
                    "scope": "product",
                    "batch_specific": False,
                    "source": "rasff_dg_sante_api",
                    "reference": "2026.4321",
                    "product_name": "Fruit gummies",
                    "overlap_key": "fruit gummies::2026 4321",
                    "match_score": 98,
                    "confidence": "high",
                    "severity": "high",
                }
            ],
            "observability": ss._new_safety_observability(),
        }
        efet_lookup = {
            "checked": True,
            "source": "efet_gr",
            "has_matches": True,
            "alerts": [
                {
                    "title": "Ανάκληση προϊόντος Fruit gummies",
                    "summary": "EFET notice",
                    "url": "https://www.efet.gr/example-overlap",
                    "scope": "product",
                    "batch_specific": False,
                    "source": "efet_gr",
                    "reference": "2026.4321",
                    "product_name": "Fruit gummies",
                    "overlap_key": "fruit gummies::2026 4321",
                    "match_score": 88,
                    "confidence": "medium",
                    "severity": "medium",
                }
            ],
            "observability": ss._new_safety_observability(),
        }

        merged = ss._merge_safety_lookup_payloads(rasff_lookup, efet_lookup)

        self.assertEqual(len(merged["alerts"]), 1)
        self.assertEqual(set(merged["alerts"][0]["sources"]), {"rasff_dg_sante_api", "efet_gr"})
        self.assertEqual(merged["observability"]["duplicate_collapsed"], 1)


if __name__ == "__main__":
    unittest.main()
