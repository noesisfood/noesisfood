import unittest
from pathlib import Path

from app.services.scanner_service import analyze_manual_product


class SessionCorrectionRecalcTests(unittest.IsolatedAsyncioTestCase):
    async def test_corrected_payload_recalculates_score_and_marks_session_correction(self) -> None:
        base_payload = {
            "name": "Crunchy Cereal",
            "brand": "Test",
            "barcode": "4000000000001",
            "key": "4000000000001",
            "image_url": "https://example.com/cereal.jpg",
            "quantity": "500 g",
            "categories": ["breakfast cereals"],
            "ingredients_text": "oats, sugar, cocoa",
            "unit": "g",
            "energy_kcal": 390,
            "fat_g": 7,
            "carb_g": 68,
            "sugar_g": 14,
            "salt_g": 0.4,
            "sat_fat_g": 1.2,
            "protein_g": 9,
            "serving_size": 40,
        }
        corrected_payload = {
            **base_payload,
            "sugar_g": 6,
            "salt_g": 0.1,
            "corrected_in_session": True,
        }

        original = await analyze_manual_product(base_payload, lang="en")
        corrected = await analyze_manual_product(corrected_payload, lang="en")

        self.assertNotEqual(original["vitascore"], corrected["vitascore"])
        self.assertNotEqual(
            original["vitascore_explanation"]["basic_nutrition_score"],
            corrected["vitascore_explanation"]["basic_nutrition_score"],
        )
        self.assertTrue(corrected["corrected_in_session"])
        self.assertTrue(corrected["meta"]["corrected_in_session"])

    async def test_corrected_payload_preserves_product_identity_and_context(self) -> None:
        result = await analyze_manual_product(
            {
                "name": "Protein Yogurt",
                "brand": "Test Brand",
                "barcode": "5449000000996",
                "key": "5449000000996",
                "image_url": "https://example.com/yogurt.jpg",
                "quantity": "200 g",
                "categories": ["yogurts", "high protein"],
                "categories_tags": ["en:yogurts", "en:high-protein"],
                "ingredients_text": "milk, protein, cultures",
                "unit": "g",
                "energy_kcal": 98,
                "fat_g": 1.5,
                "carb_g": 6.0,
                "sugar_g": 5.4,
                "salt_g": 0.1,
                "sat_fat_g": 0.9,
                "protein_g": 10.0,
                "serving_size": 150,
                "corrected_in_session": True,
                "meta": {"lookup_state": "found", "custom_flag": "keep-me"},
            },
            lang="en",
        )

        self.assertEqual(result["key"], "5449000000996")
        self.assertEqual(result["product"]["barcode"], "5449000000996")
        self.assertEqual(result["product"]["name"], "Protein Yogurt")
        self.assertEqual(result["product"]["brand"], "Test Brand")
        self.assertEqual(result["product"]["image_url"], "https://example.com/yogurt.jpg")
        self.assertEqual(result["product"]["quantity"], "200 g")
        self.assertIn("high protein", result["product"]["categories"])

    async def test_explanation_and_confidence_refresh_after_correction(self) -> None:
        result = await analyze_manual_product(
            {
                "name": "Soup",
                "brand": "Test",
                "ingredients_text": "water, vegetables, salt",
                "unit": "g",
                "energy_kcal": 42,
                "sugar_g": 2.0,
                "salt_g": 1.8,
                "protein_g": 1.0,
                "corrected_in_session": True,
            },
            lang="en",
        )

        self.assertIn("vitascore_explanation", result)
        self.assertIn("confidence_reasons", result)
        self.assertTrue(result["confidence_reasons"])
        self.assertIn("confidence_notes", result["vitascore_explanation"])

    async def test_missing_optional_fields_do_not_break_recalculation(self) -> None:
        result = await analyze_manual_product(
            {
                "name": "Simple Snack",
                "brand": "Test",
                "barcode": "1234567890123",
                "key": "1234567890123",
                "ingredients_text": "corn, salt",
                "unit": "g",
                "energy_kcal": 120,
                "sugar_g": 1.0,
                "salt_g": 0.7,
                "corrected_in_session": True,
            },
            lang="en",
        )

        self.assertEqual(result["key"], "1234567890123")
        self.assertTrue(result["corrected_in_session"])
        self.assertIsNone(result["nutrition_per_100"]["fat_g"])
        self.assertIsNone(result["nutrition_per_100"]["sat_fat_g"])

    def test_frontend_has_correction_refresh_hooks(self) -> None:
        content = Path("app/frontend/index.html").read_text(encoding="utf-8")

        self.assertIn("corrected_in_session", content)
        self.assertIn("Updated with your corrected values", content)
        self.assertIn("function buildCorrectionPayload(currentData, form)", content)


if __name__ == "__main__":
    unittest.main()
