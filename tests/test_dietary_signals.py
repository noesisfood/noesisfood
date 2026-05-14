import unittest
from unittest.mock import AsyncMock, patch

from app.services import scanner_service as ss
from app.services.product_normalizer import normalize_openfoodfacts


class DietarySignalsTests(unittest.IsolatedAsyncioTestCase):
    def test_explicit_halal_certification_maps_to_certified(self) -> None:
        norm = normalize_openfoodfacts(
            {
                "product": {
                    "product_name": "Soup",
                    "brands": "Test",
                    "labels": "Halal, Organic",
                    "labels_tags": ["en:halal"],
                    "ingredients_text": "water, spices",
                    "nutriments": {"energy-kcal_100g": 25, "salt_100g": 0.8, "proteins_100g": 1.0},
                }
            }
        )

        signals = ss._build_dietary_signals({"source": "openfoodfacts"}, norm, {"product": {"labels_tags": ["en:halal"]}}, "en")
        self.assertEqual(signals["halal"]["status"], "certified")
        self.assertTrue(signals["halal"]["detected_labels"])
        self.assertEqual(signals["halal"]["confidence"], "high")

    async def test_pork_and_alcohol_mark_halal_as_possible_not_suitable(self) -> None:
        result = await ss.analyze_manual_product(
            {
                "name": "Ham Snack",
                "brand": "Test",
                "unit": "g",
                "ingredients_text": "pork, salt, beer extract",
                "energy_kcal": 210,
                "sugar_g": 1,
                "salt_g": 1.8,
                "sat_fat_g": 4,
                "protein_g": 12,
                "serving_size": 50,
            },
            lang="en",
        )

        halal = result["dietary_signals"]["halal"]
        self.assertEqual(halal["status"], "possible_not_suitable")
        self.assertTrue({"pork", "alcohol"}.issubset({item["id"] for item in halal["possible_concerns"]}))

    async def test_halal_defaults_to_unclear_without_certification(self) -> None:
        result = await ss.analyze_manual_product(
            {
                "name": "Vegetable Soup",
                "brand": "Test",
                "unit": "g",
                "ingredients_text": "water, vegetables, spices",
                "energy_kcal": 35,
                "sugar_g": 2,
                "salt_g": 0.7,
                "sat_fat_g": 0.2,
                "protein_g": 1.2,
                "serving_size": 250,
            },
            lang="en",
        )

        halal = result["dietary_signals"]["halal"]
        self.assertEqual(halal["status"], "unclear")
        self.assertFalse(halal["detected_labels"])

    async def test_vegan_label_maps_to_labeled(self) -> None:
        result = await ss.analyze_manual_product(
            {
                "name": "Plant Bar",
                "brand": "Test",
                "unit": "g",
                "labels": "Vegan",
                "labels_tags": ["en:vegan"],
                "ingredients_text": "oats, cocoa butter",
                "energy_kcal": 430,
                "sugar_g": 18,
                "salt_g": 0.15,
                "sat_fat_g": 3.5,
                "protein_g": 8,
                "serving_size": 40,
            },
            lang="en",
        )

        vegan = result["dietary_signals"]["vegan"]
        self.assertEqual(vegan["status"], "labeled")
        self.assertTrue(vegan["detected_labels"])

    async def test_milk_egg_honey_and_gelatin_mark_vegan_as_possible_not_suitable(self) -> None:
        result = await ss.analyze_manual_product(
            {
                "name": "Dessert",
                "brand": "Test",
                "unit": "g",
                "ingredients_text": "milk, egg white, honey, gelatin",
                "energy_kcal": 240,
                "sugar_g": 20,
                "salt_g": 0.2,
                "sat_fat_g": 3,
                "protein_g": 4,
                "serving_size": 90,
            },
            lang="en",
        )

        vegan = result["dietary_signals"]["vegan"]
        self.assertEqual(vegan["status"], "possible_not_suitable")
        self.assertTrue({"milk", "egg", "honey", "gelatin"}.issubset({item["id"] for item in vegan["possible_concerns"]}))

    async def test_vegetarian_label_maps_to_labeled(self) -> None:
        result = await ss.analyze_manual_product(
            {
                "name": "Soup",
                "brand": "Test",
                "unit": "g",
                "labels": "Vegetarian",
                "labels_tags": ["en:vegetarian"],
                "ingredients_text": "water, potatoes, herbs",
                "energy_kcal": 48,
                "sugar_g": 2.5,
                "salt_g": 0.6,
                "sat_fat_g": 0.2,
                "protein_g": 1.5,
                "serving_size": 250,
            },
            lang="en",
        )

        vegetarian = result["dietary_signals"]["vegetarian"]
        self.assertEqual(vegetarian["status"], "labeled")
        self.assertTrue(vegetarian["detected_labels"])

    async def test_meat_fish_and_gelatin_mark_vegetarian_as_possible_not_suitable(self) -> None:
        result = await ss.analyze_manual_product(
            {
                "name": "Pasta Sauce",
                "brand": "Test",
                "unit": "g",
                "ingredients_text": "beef stock, anchovy, gelatin",
                "energy_kcal": 80,
                "sugar_g": 4,
                "salt_g": 1.1,
                "sat_fat_g": 0.8,
                "protein_g": 3,
                "serving_size": 125,
            },
            lang="en",
        )

        vegetarian = result["dietary_signals"]["vegetarian"]
        self.assertEqual(vegetarian["status"], "possible_not_suitable")
        self.assertTrue({"meat_or_fish", "gelatin"}.issubset({item["id"] for item in vegetarian["possible_concerns"]}))

    def test_uncertain_origin_terms_do_not_force_positive_or_religious_claim(self) -> None:
        signals = ss._build_dietary_signals(
            {"source": "manual", "matched_by": "manual_entry"},
            {"ingredients_text": "rennet, enzymes", "allergen_info": {}},
            {"ingredients_text": "rennet, enzymes"},
            "en",
        )

        self.assertEqual(signals["halal"]["status"], "unclear")
        self.assertEqual(signals["vegetarian"]["status"], "unclear")
        self.assertTrue(signals["halal"]["possible_concerns"])
        self.assertTrue(signals["vegetarian"]["possible_concerns"])

    async def test_photo_path_keeps_low_confidence_for_ingredient_based_signals(self) -> None:
        extracted = {
            "product_name": "Photo Candy",
            "brand": "Test",
            "ingredients_text": "gelatin, sugar",
            "categories": [],
            "nutrition_per_100": {
                "unit": "g",
                "energy_kcal": 350.0,
                "sugar_g": 60.0,
                "salt_g": 0.1,
                "sat_fat_g": 0.2,
                "protein_g": 6.0,
            },
            "confidence": "medium",
            "extracted_fields": ["ingredients_text", "nutrition_per_100"],
            "notes": "Ingredient text extracted from photo.",
            "label_kind": "ingredients",
        }
        payload = {
            "ingredient_image_data_url": "data:image/jpeg;base64,AAA",
            "existing_key": "4000000000010",
            "existing_analysis": {
                "key": "4000000000010",
                "source": "openfoodfacts",
                "product": {"barcode": "4000000000010"},
                "nutrition_per_100": {"unit": "g"},
                "meta": {"serving": {"unit": "g"}},
            },
            "existing_product": {
                "name": "Photo Candy",
                "brand": "Test",
                "categories": ["candy"],
                "image_url": "",
                "quantity": "",
            },
        }

        with (
            patch.object(ss, "_extract_photo_payload_with_ai", AsyncMock(return_value=extracted)),
            patch.object(ss, "_persist_product_enrichment", return_value={}),
        ):
            result = await ss.analyze_photo_product(payload, lang="en")

        vegan = result["dietary_signals"]["vegan"]
        self.assertEqual(vegan["status"], "possible_not_suitable")
        self.assertEqual(vegan["confidence"], "low")


if __name__ == "__main__":
    unittest.main()
