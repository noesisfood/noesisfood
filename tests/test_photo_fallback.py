import unittest
from unittest.mock import AsyncMock, patch

from app.services import scanner_service as ss


class PhotoFallbackCompositionTests(unittest.IsolatedAsyncioTestCase):
    def test_normalize_photo_payload_applies_composition_table_water_fallback(self) -> None:
        parsed = {
            "product_name": "Natürliches Mineralwasser mit Kohlensäure (medium)",
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
                "name": "Natürliches Mineralwasser mit Kohlensäure (medium)",
                "brand": "K-Classic",
                "categories": ["Natürliches Mineralwasser", "Natürliches Mineralwasser mit wenig Kohlensäure versetzt"],
            },
            "existing_analysis": {
                "nutrition_per_100": {"unit": "ml"},
                "meta": {"serving": {"unit": "ml"}},
            },
        }

        normalized = ss._normalize_photo_extracted_payload(parsed, payload)

        self.assertEqual(normalized["product_name"], "Natürliches Mineralwasser mit Kohlensäure (medium)")
        self.assertEqual(normalized["ingredients_text"], "Natürliches Mineralwasser mit Kohlensäure (medium)")
        self.assertEqual(normalized["label_kind"], "composition_table")
        self.assertEqual(normalized["nutrition_per_100"]["unit"], "ml")
        self.assertEqual(normalized["nutrition_per_100"]["energy_kcal"], 0.0)
        self.assertEqual(normalized["nutrition_per_100"]["sugar_g"], 0.0)
        self.assertEqual(normalized["nutrition_per_100"]["salt_g"], 0.0)
        self.assertIn("composition_table", normalized["extracted_fields"])
        self.assertIn("nutrition_per_100", normalized["extracted_fields"])

    async def test_analyze_photo_product_resolves_mineral_water_composition_case(self) -> None:
        extracted = {
            "product_name": "Natürliches Mineralwasser mit Kohlensäure (medium)",
            "brand": "K-Classic",
            "ingredients_text": "Natürliches Mineralwasser mit Kohlensäure (medium)",
            "categories": ["Natürliches Mineralwasser", "Natürliches Mineralwasser mit wenig Kohlensäure versetzt"],
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
                "name": "Natürliches Mineralwasser mit Kohlensäure (medium)",
                "brand": "K-Classic",
                "categories": ["Natürliches Mineralwasser", "Natürliches Mineralwasser mit wenig Kohlensäure versetzt"],
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
        self.assertEqual(result["product"]["name"], "Natürliches Mineralwasser mit Kohlensäure (medium)")
        self.assertEqual(result["analysis_state"], "partial_analysis")
        self.assertEqual(result["lookup_state"], "found_but_incomplete")
        self.assertEqual(result["source"], "openfoodfacts")
        self.assertEqual(result["matched_by"], "photo_enrichment")


if __name__ == "__main__":
    unittest.main()
