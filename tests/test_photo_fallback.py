import unittest
from unittest.mock import AsyncMock, patch

from app.services import scanner_service as ss


class PhotoFallbackCompositionTests(unittest.IsolatedAsyncioTestCase):
    def test_normalize_photo_payload_applies_composition_table_water_fallback(self) -> None:
        parsed = {
            "product_name": "NatÃ¼rliches Mineralwasser mit KohlensÃ¤ure (medium)",
            "brand": "K-Classic",
            "ingredients_text": None,
            "categories": [],
            "nutrition_per_100": {"unit": None, "energy_kcal": None, "sugar_g": None, "salt_g": None, "sat_fat_g": None, "protein_g": None},
            "confidence": "medium",
            "extracted_fields": ["product_name", "brand"],
            "notes": "Mineral composition table visible.",
            "label_kind": "composition_table",
            "composition_table_text": "Calcium Magnesium Hydrogencarbonat",
        }
        payload = {
            "existing_product": {
                "name": "NatÃ¼rliches Mineralwasser mit KohlensÃ¤ure (medium)",
                "brand": "K-Classic",
                "categories": ["NatÃ¼rliches Mineralwasser", "NatÃ¼rliches Mineralwasser mit wenig KohlensÃ¤ure versetzt"],
            },
            "existing_analysis": {
                "nutrition_per_100": {"unit": "ml"},
                "meta": {"serving": {"unit": "ml"}},
            },
        }

        normalized = ss._normalize_photo_extracted_payload(parsed, payload)

        self.assertEqual(normalized["product_name"], "NatÃ¼rliches Mineralwasser mit KohlensÃ¤ure (medium)")
        self.assertEqual(normalized["ingredients_text"], "NatÃ¼rliches Mineralwasser mit KohlensÃ¤ure (medium)")
        self.assertEqual(normalized["label_kind"], "composition_table")
        self.assertEqual(normalized["nutrition_per_100"]["unit"], "ml")
        self.assertEqual(normalized["nutrition_per_100"]["energy_kcal"], 0.0)
        self.assertEqual(normalized["nutrition_per_100"]["sugar_g"], 0.0)
        self.assertEqual(normalized["nutrition_per_100"]["salt_g"], 0.0)
        self.assertIn("composition_table", normalized["extracted_fields"])
        self.assertIn("nutrition_per_100", normalized["extracted_fields"])

    def test_build_photo_context_water_fallback_recovers_from_existing_product_context(self) -> None:
        payload = {
            "ingredient_image_data_url": "data:image/jpeg;base64,AAA",
            "existing_product": {
                "name": "NatÃ¼rliches Mineralwasser mit KohlensÃ¤ure (medium)",
                "brand": "K-Classic",
                "categories": ["NatÃ¼rliches Mineralwasser", "NatÃ¼rliches Mineralwasser mit wenig KohlensÃ¤ure versetzt"],
            },
            "existing_analysis": {
                "nutrition_per_100": {"unit": "ml"},
                "meta": {"serving": {"unit": "ml"}},
            },
        }

        normalized = ss._build_photo_context_water_fallback(payload)

        self.assertIsInstance(normalized, dict)
        self.assertEqual(normalized["product_name"], "NatÃ¼rliches Mineralwasser mit KohlensÃ¤ure (medium)")
        self.assertEqual(normalized["ingredients_text"], "NatÃ¼rliches Mineralwasser mit KohlensÃ¤ure (medium)")
        self.assertEqual(normalized["label_kind"], "composition_table")
        self.assertEqual(normalized["nutrition_per_100"]["unit"], "ml")
        self.assertEqual(normalized["nutrition_per_100"]["energy_kcal"], 0.0)
        self.assertEqual(normalized["nutrition_per_100"]["sugar_g"], 0.0)
        self.assertIn("composition_table_context", normalized["extracted_fields"])

    def test_build_photo_context_water_fallback_skips_non_water_products(self) -> None:
        payload = {
            "ingredient_image_data_url": "data:image/jpeg;base64,AAA",
            "existing_product": {
                "name": "Apfelschorle",
                "brand": "K-Classic",
                "categories": ["Soft drink"],
            },
        }

        normalized = ss._build_photo_context_water_fallback(payload)

        self.assertIsNone(normalized)

    async def test_analyze_photo_product_resolves_mineral_water_composition_case(self) -> None:
        extracted = {
            "product_name": "NatÃ¼rliches Mineralwasser mit KohlensÃ¤ure (medium)",
            "brand": "K-Classic",
            "ingredients_text": "NatÃ¼rliches Mineralwasser mit KohlensÃ¤ure (medium)",
            "categories": ["NatÃ¼rliches Mineralwasser", "NatÃ¼rliches Mineralwasser mit wenig KohlensÃ¤ure versetzt"],
            "nutrition_per_100": {
                "unit": "ml",
                "energy_kcal": 0.0,
                "sugar_g": 0.0,
                "salt_g": 0.0,
                "sat_fat_g": 0.0,
                "protein_g": 0.0,
            },
            "confidence": "medium",
            "extracted_fields": ["product_name", "brand", "ingredients_text", "nutrition_per_100", "composition_table"],
            "notes": "Composition-table water fallback applied with conservative zero nutrition for plain mineral water.",
            "label_kind": "composition_table",
        }
        payload = {
            "ingredient_image_data_url": "data:image/jpeg;base64,AAA",
            "existing_key": "4337185151651",
            "existing_analysis": {
                "key": "4337185151651",
                "source": "openfoodfacts",
                "product": {"barcode": "4337185151651"},
                "meta": {"serving": {"unit": "ml"}},
            },
            "existing_product": {
                "name": "NatÃ¼rliches Mineralwasser mit KohlensÃ¤ure (medium)",
                "brand": "K-Classic",
                "categories": ["NatÃ¼rliches Mineralwasser", "NatÃ¼rliches Mineralwasser mit wenig KohlensÃ¤ure versetzt"],
                "image_url": "",
                "quantity": "",
            },
        }

        with (
            patch.object(ss, "_extract_photo_payload_with_ai", AsyncMock(return_value=extracted)),
            patch.object(ss, "_persist_product_enrichment", return_value={}),
        ):
            result = await ss.analyze_photo_product(payload, lang="en")

        self.assertFalse(result.get("error"))
        self.assertEqual(result["product"]["name"], "NatÃ¼rliches Mineralwasser mit KohlensÃ¤ure (medium)")
        self.assertEqual(result["analysis_state"], "partial_analysis")
        self.assertEqual(result["lookup_state"], "found_but_incomplete")
        self.assertEqual(result["source"], "openfoodfacts")
        self.assertEqual(result["matched_by"], "photo_enrichment")

    async def test_analyze_photo_product_uses_context_fallback_when_ai_extraction_fails(self) -> None:
        extracted_error = {
            "error": {"code": "PHOTO_EXTRACTION_FAILED", "message": "Could not extract enough data from the photo."},
            "status": 422,
            "lookup_state": "found_but_incomplete",
            "analysis_state": "insufficient_data",
            "analysis_confidence": "low",
        }
        payload = {
            "ingredient_image_data_url": "data:image/jpeg;base64,AAA",
            "existing_key": "4337185151651",
            "existing_analysis": {
                "key": "4337185151651",
                "source": "openfoodfacts",
                "product": {"barcode": "4337185151651"},
                "nutrition_per_100": {"unit": "ml"},
                "meta": {"serving": {"unit": "ml"}},
            },
            "existing_product": {
                "name": "NatÃ¼rliches Mineralwasser mit KohlensÃ¤ure (medium)",
                "brand": "K-Classic",
                "categories": ["NatÃ¼rliches Mineralwasser", "NatÃ¼rliches Mineralwasser mit wenig KohlensÃ¤ure versetzt"],
                "image_url": "",
                "quantity": "",
            },
        }

        with (
            patch.object(ss, "_extract_photo_payload_with_ai", AsyncMock(return_value=extracted_error)),
            patch.object(ss, "_persist_product_enrichment", return_value={}),
        ):
            result = await ss.analyze_photo_product(payload, lang="en")

        self.assertFalse(result.get("error"))
        self.assertEqual(result["product"]["name"], "NatÃ¼rliches Mineralwasser mit KohlensÃ¤ure (medium)")
        self.assertEqual(result["analysis_state"], "partial_analysis")
        self.assertEqual(result["lookup_state"], "found_but_incomplete")
        self.assertEqual(result["source"], "openfoodfacts")
        self.assertEqual(result["matched_by"], "photo_enrichment")
        self.assertEqual(result["photo_extraction"]["confidence"], "low")
        self.assertIn("composition_table_context", result["photo_extraction"]["extracted_fields"])


if __name__ == "__main__":
    unittest.main()
