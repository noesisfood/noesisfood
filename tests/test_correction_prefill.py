import unittest
from pathlib import Path

from app.services.scanner_service import analyze_manual_product


class CorrectionPrefillTests(unittest.IsolatedAsyncioTestCase):
    async def test_manual_analysis_returns_optional_fat_and_carb_when_provided(self) -> None:
        result = await analyze_manual_product(
            {
                "name": "Manual Cereal",
                "brand": "Test",
                "unit": "g",
                "ingredients_text": "oats, cocoa, sugar",
                "energy_kcal": 390,
                "fat_g": 7,
                "carb_g": 68,
                "sugar_g": 14,
                "salt_g": 0.4,
                "sat_fat_g": 1.2,
                "protein_g": 9,
                "serving_size": 40,
            },
            lang="en",
        )

        nutrition = result["nutrition_per_100"]
        self.assertEqual(nutrition["fat_g"], 7.0)
        self.assertEqual(nutrition["carb_g"], 68.0)

    async def test_manual_analysis_leaves_optional_fat_and_carb_blank_when_missing(self) -> None:
        result = await analyze_manual_product(
            {
                "name": "Simple Drink",
                "brand": "Test",
                "unit": "ml",
                "ingredients_text": "water, sugar",
                "energy_kcal": 20,
                "sugar_g": 5,
                "salt_g": 0.1,
                "sat_fat_g": 0.0,
                "protein_g": 0.0,
                "serving_size": 250,
            },
            lang="en",
        )

        nutrition = result["nutrition_per_100"]
        self.assertIsNone(nutrition["fat_g"])
        self.assertIsNone(nutrition["carb_g"])

    def test_frontend_correction_prefill_has_aliases_and_blank_fallbacks(self) -> None:
        content = Path("app/frontend/index.html").read_text(encoding="utf-8")

        self.assertIn('function correctionValue(source, keys, fallback = "")', content)
        self.assertIn('["fat_g", "fat"]', content)
        self.assertIn('["carb_g", "carbs_g", "carbohydrates_g"]', content)
        self.assertIn('["sat_fat_g", "saturated_fat_g"]', content)


if __name__ == "__main__":
    unittest.main()
