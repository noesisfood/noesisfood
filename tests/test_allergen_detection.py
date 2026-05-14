import unittest
from unittest.mock import AsyncMock, patch

from app.services import scanner_service as ss


class AllergenDetectionTests(unittest.IsolatedAsyncioTestCase):
    def test_official_allergen_fields_create_high_coverage_detection(self) -> None:
        norm = {
            "name": "Test Product",
            "brand": "Test",
            "barcode": "4000000000001",
            "image_url": None,
            "quantity": "100 g",
            "categories": ["snack"],
            "categories_tags": [],
            "ingredients": [{"name": "sugar", "class": "U", "note": "From OpenFoodFacts"}],
            "ingredients_text": "sugar",
            "allergen_info": {
                "allergens": "milk, soybeans",
                "allergens_tags": ["en:milk", "en:soybeans"],
                "allergens_from_ingredients": "",
                "traces": "may contain peanuts",
                "traces_tags": ["en:peanuts"],
                "ingredients_text": "sugar",
            },
            "nutrition_per_100": {
                "unit": "g",
                "energy_kcal": 400.0,
                "sugar_g": 18.0,
                "salt_g": 0.5,
                "sat_fat_g": 3.0,
                "protein_g": 6.0,
                "serving_size": 30.0,
            },
            "meta": {"is_beverage": False},
        }

        result = ss._analyze_normalized_product(
            key="4000000000001",
            norm=norm,
            raw={"product": {"allergens_tags": ["en:milk", "en:soybeans"], "traces_tags": ["en:peanuts"]}},
            source="openfoodfacts",
            matched_by="barcode_or_key",
            lang="en",
            rasff=[],
        )

        detection = result["allergen_detection"]
        self.assertEqual(detection["coverage"], "high")
        self.assertEqual([item["id"] for item in detection["detected"]], ["soybeans", "milk_lactose"])
        self.assertEqual([item["id"] for item in detection["possible_signals"]], ["peanuts"])
        self.assertEqual({item["source"] for item in detection["detected"]}, {"barcode_product_data"})

    def test_multilingual_ingredient_text_maps_to_eu14_groups(self) -> None:
        cases = [
            ("de", "Weizenmehl, Milchpulver, Erdnüsse, Sesam", {"gluten", "milk_lactose", "peanuts", "sesame"}),
            ("fr", "lait, arachides, moutarde", {"milk_lactose", "peanuts", "mustard"}),
            ("el", "γάλα, σόγια, σέλινο", {"milk_lactose", "soybeans", "celery"}),
            ("en", "egg, fish, lupin flour", {"eggs", "fish", "lupin"}),
        ]

        for lang, text, expected_ids in cases:
            detection = ss._build_allergen_detection(
                {"source": "openfoodfacts", "matched_by": "barcode_or_key"},
                {"ingredients_text": text, "allergen_info": {}},
                {},
                lang,
            )
            self.assertEqual(detection["coverage"], "medium")
            self.assertTrue(expected_ids.issubset({item["id"] for item in detection["detected"]}))

    async def test_manual_ingredient_text_is_low_confidence_and_cautious(self) -> None:
        result = await ss.analyze_manual_product(
            {
                "name": "Manual Bar",
                "brand": "Test",
                "unit": "g",
                "ingredients_text": "milk, hazelnuts, oats",
                "energy_kcal": 430,
                "sugar_g": 20,
                "salt_g": 0.2,
                "sat_fat_g": 5,
                "protein_g": 7,
                "serving_size": 40,
            },
            lang="en",
        )

        detection = result["allergen_detection"]
        self.assertEqual(detection["coverage"], "low")
        self.assertEqual({item["source"] for item in detection["detected"]}, {"manual_input"})
        self.assertTrue({"milk_lactose", "nuts", "gluten"}.issubset({item["id"] for item in detection["detected"]}))
        rendered = " ".join(
            [detection["coverage_note"], detection["warning"]]
            + [item["label"] for item in detection["detected"]]
            + [item["label"] for item in detection["possible_signals"]]
        ).lower()
        self.assertNotIn("allergen-free", rendered)
        self.assertNotIn("safe for allergy sufferers", rendered)
        self.assertNotIn("complete coverage", rendered)

    def test_precautionary_text_becomes_possible_not_detected(self) -> None:
        detection = ss._build_allergen_detection(
            {"source": "manual", "matched_by": "manual_entry"},
            {"ingredients_text": "sugar. may contain milk, nuts and sesame.", "allergen_info": {}},
            {"ingredients_text": "sugar. may contain milk, nuts and sesame."},
            "en",
        )

        self.assertFalse(detection["detected"])
        self.assertEqual({item["id"] for item in detection["possible_signals"]}, {"milk_lactose", "nuts", "sesame"})

    def test_missing_allergen_data_returns_incomplete_warning(self) -> None:
        detection = ss._build_allergen_detection(
            {"source": "manual", "matched_by": "manual_entry"},
            {"ingredients_text": "", "allergen_info": {}},
            {},
            "en",
        )

        self.assertEqual(detection["coverage"], "low")
        self.assertIn("limited", detection["coverage_note"].lower())
        self.assertIn("official product label", detection["warning"].lower())

    async def test_photo_ingredient_text_is_low_confidence_ocr_source(self) -> None:
        extracted = {
            "product_name": "Photo Soup",
            "brand": "Test",
            "ingredients_text": "lait, soja",
            "categories": [],
            "nutrition_per_100": {
                "unit": "g",
                "energy_kcal": 55.0,
                "sugar_g": 2.0,
                "salt_g": 0.6,
                "sat_fat_g": 1.0,
                "protein_g": 2.0,
            },
            "confidence": "medium",
            "extracted_fields": ["ingredients_text", "nutrition_per_100"],
            "notes": "Ingredient text extracted from photo.",
            "label_kind": "ingredients",
        }
        payload = {
            "ingredient_image_data_url": "data:image/jpeg;base64,AAA",
            "existing_key": "4000000000002",
            "existing_analysis": {
                "key": "4000000000002",
                "source": "openfoodfacts",
                "product": {"barcode": "4000000000002"},
                "nutrition_per_100": {"unit": "g"},
                "meta": {"serving": {"unit": "g"}},
            },
            "existing_product": {
                "name": "Photo Soup",
                "brand": "Test",
                "categories": ["soup"],
                "image_url": "",
                "quantity": "",
            },
        }

        with (
            patch.object(ss, "_extract_photo_payload_with_ai", AsyncMock(return_value=extracted)),
            patch.object(ss, "_persist_product_enrichment", return_value={}),
        ):
            result = await ss.analyze_photo_product(payload, lang="fr")

        detection = result["allergen_detection"]
        self.assertEqual(detection["coverage"], "low")
        self.assertEqual({item["source"] for item in detection["detected"]}, {"ocr_ingredient_photo"})
        self.assertEqual({item["id"] for item in detection["detected"]}, {"soybeans", "milk_lactose"})


if __name__ == "__main__":
    unittest.main()
