import unittest

from app.services import scanner_service as ss


class UsageContextTests(unittest.IsolatedAsyncioTestCase):
    async def test_extreme_salt_product_triggers_usage_context(self) -> None:
        result = await ss.analyze_manual_product(
            {
                "name": "Salz, Alpen Jod salz + Fluorid",
                "brand": "Test",
                "unit": "g",
                "ingredients_text": "salt",
                "energy_kcal": 0,
                "sugar_g": 0,
                "salt_g": 99.9,
                "sat_fat_g": 0,
                "protein_g": 0,
                "serving_size": 1,
            },
            lang="en",
        )

        usage = result["usage_context"]
        self.assertTrue(usage["applies"])
        self.assertEqual(usage["type"], "seasoning")
        self.assertEqual(usage["severity"], "high_salt_seasoning")
        self.assertIn("seasoning", usage["message"].lower())
        self.assertTrue(any("small amounts" in note.lower() for note in usage["notes"]))

    async def test_product_name_salz_supports_usage_context_detection(self) -> None:
        result = await ss.analyze_manual_product(
            {
                "name": "Salz",
                "brand": "Test",
                "unit": "g",
                "ingredients_text": "salt",
                "energy_kcal": 0,
                "sugar_g": 0,
                "salt_g": 55.0,
                "sat_fat_g": 0,
                "protein_g": 0,
                "serving_size": 1,
            },
            lang="de",
        )

        self.assertTrue(result["usage_context"]["applies"])
        self.assertEqual(result["usage_context"]["severity"], "high_salt_seasoning")

    async def test_usage_context_does_not_change_scores(self) -> None:
        payload = {
            "name": "Salz, Alpen Jod salz + Fluorid",
            "brand": "Test",
            "unit": "g",
            "ingredients_text": "salt",
            "energy_kcal": 0,
            "sugar_g": 0,
            "salt_g": 99.9,
            "sat_fat_g": 0,
            "protein_g": 0,
            "serving_size": 1,
        }
        result = await ss.analyze_manual_product(payload, lang="en")

        self.assertTrue(result["usage_context"]["applies"])
        self.assertEqual(result["vitascore"], 82)
        self.assertEqual(result["vitascore_explanation"]["basic_nutrition_score"], 91)

    async def test_normal_food_is_not_marked_as_seasoning(self) -> None:
        result = await ss.analyze_manual_product(
            {
                "name": "Tomato Soup",
                "brand": "Test",
                "unit": "g",
                "ingredients_text": "water, tomatoes, salt",
                "energy_kcal": 35,
                "sugar_g": 4,
                "salt_g": 0.8,
                "sat_fat_g": 0.1,
                "protein_g": 1.2,
                "serving_size": 250,
            },
            lang="en",
        )

        self.assertFalse(result["usage_context"]["applies"])
        self.assertEqual(result["usage_context"]["type"], "")


if __name__ == "__main__":
    unittest.main()
