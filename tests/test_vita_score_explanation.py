import unittest

from app.services.scanner_service import analyze_manual_product


class VitaScoreExplanationTests(unittest.IsolatedAsyncioTestCase):
    async def test_explanation_exists_for_manual_analysis(self) -> None:
        result = await analyze_manual_product(
            {
                "name": "Zero Cola",
                "brand": "Test",
                "unit": "ml",
                "categories": ["soft drink"],
                "ingredients_text": "carbonated water, sweetener aspartame, sweetener acesulfame k, colour caramel, flavouring, preservative sodium benzoate",
                "sugar_g": 0.0,
                "salt_g": 0.05,
                "sat_fat_g": 0.0,
                "protein_g": 0.0,
                "energy_kcal": 1,
                "serving_size": 330,
            },
            lang="en",
        )

        explanation = result.get("vitascore_explanation")
        self.assertIsInstance(explanation, dict)
        self.assertEqual(explanation["basic_nutrition_score"], 99)
        self.assertEqual(explanation["vita_score"], result["vitascore"])

    async def test_explanation_uses_applied_adjustments_between_baseline_and_final(self) -> None:
        result = await analyze_manual_product(
            {
                "name": "Zero Cola",
                "brand": "Test",
                "unit": "ml",
                "categories": ["soft drink"],
                "ingredients_text": "carbonated water, sweetener aspartame, sweetener acesulfame k, colour caramel, flavouring, preservative sodium benzoate",
                "sugar_g": 0.0,
                "salt_g": 0.05,
                "sat_fat_g": 0.0,
                "protein_g": 0.0,
                "energy_kcal": 1,
                "serving_size": 330,
            },
            lang="en",
        )

        explanation = result["vitascore_explanation"]
        adjustments = {item["code"]: item for item in explanation["score_adjustments"]}

        self.assertGreater(explanation["basic_nutrition_score"], explanation["vita_score"])
        self.assertIn("non_sugar_sweetener_presence", adjustments)
        self.assertEqual(adjustments["non_sugar_sweetener_presence"]["impact"], -10)
        self.assertEqual(
            adjustments["non_sugar_sweetener_presence"]["reason"],
            "Non-sugar sweeteners lowered the final score",
        )

    async def test_positive_and_cap_adjustments_are_explained(self) -> None:
        result = await analyze_manual_product(
            {
                "name": "Plain Almonds",
                "brand": "Test",
                "unit": "g",
                "categories": ["nuts"],
                "ingredients_text": "almonds",
                "sugar_g": 4.0,
                "salt_g": 0.01,
                "sat_fat_g": 3.8,
                "protein_g": 21.0,
                "energy_kcal": 610,
                "serving_size": 30,
            },
            lang="en",
        )

        explanation = result["vitascore_explanation"]
        adjustments = {item["code"]: item for item in explanation["score_adjustments"]}

        self.assertIn("High protein content", explanation["positive_factors"])
        self.assertIn("whole_food_cap", adjustments)
        self.assertEqual(adjustments["whole_food_cap"]["impact"], -5)
        self.assertEqual(
            adjustments["whole_food_cap"]["reason"],
            "The final score was kept more conservative for a more cautious assessment of this product type",
        )
        self.assertIn("plain_nuts_seed_category", adjustments)
        self.assertEqual(adjustments["plain_nuts_seed_category"]["impact"], 4)

    async def test_greek_whole_food_cap_uses_user_facing_wording(self) -> None:
        result = await analyze_manual_product(
            {
                "name": "Plain Almonds",
                "brand": "Test",
                "unit": "g",
                "categories": ["nuts"],
                "ingredients_text": "almonds",
                "sugar_g": 4.0,
                "salt_g": 0.01,
                "sat_fat_g": 3.8,
                "protein_g": 21.0,
                "energy_kcal": 610,
                "serving_size": 30,
            },
            lang="el",
        )

        explanation = result["vitascore_explanation"]
        self.assertIn("score_adjustments", explanation)
        self.assertIsInstance(explanation["score_adjustments"], list)
        adjustments = {item["code"]: item for item in explanation["score_adjustments"]}

        self.assertIn("whole_food_cap", adjustments)
        self.assertEqual(adjustments["whole_food_cap"]["impact"], -5)
        self.assertIn(
            "Το τελικό σκορ κρατήθηκε πιο συντηρητικό για πιο προσεκτική αξιολόγηση αυτού του τύπου προϊόντος",
            adjustments["whole_food_cap"]["reason"],
        )
        self.assertNotIn("ανώτατο όριο ολόκληρης τροφής", adjustments["whole_food_cap"]["reason"])

    async def test_limited_estimate_keeps_confidence_notes_separate(self) -> None:
        result = await analyze_manual_product(
            {
                "name": "Mystery Snack",
                "brand": "Test",
                "unit": "g",
                "categories": ["snack"],
                "ingredients_text": "corn, salt",
                "protein_g": 2.0,
            },
            lang="en",
        )

        explanation = result["vitascore_explanation"]

        self.assertEqual(result["analysis_state"], "limited_estimate")
        self.assertIsNone(explanation["basic_nutrition_score"])
        self.assertIn("This is a limited estimate because core nutrition data is incomplete.", explanation["confidence_notes"])
        self.assertIn("Confidence is low.", explanation["confidence_notes"])
        self.assertIn("Some core nutrition fields are missing.", explanation["confidence_notes"])
        self.assertIn(
            "Incomplete nutrition data kept the basic nutrition assessment more cautious.",
            explanation["confidence_notes"],
        )

    async def test_multilingual_output_is_localized(self) -> None:
        payload = {
            "name": "Plain Almonds",
            "brand": "Test",
            "unit": "g",
            "categories": ["nuts"],
            "ingredients_text": "almonds",
            "sugar_g": 4.0,
            "salt_g": 0.01,
            "sat_fat_g": 3.8,
            "protein_g": 21.0,
            "energy_kcal": 610,
            "serving_size": 30,
        }
        expected = {
            "de": "Hoher Proteingehalt",
            "fr": "Teneur élevée en protéines",
            "el": "Υψηλή περιεκτικότητα σε πρωτεΐνη",
        }

        for lang, expected_factor in expected.items():
            result = await analyze_manual_product(payload, lang=lang)
            explanation = result["vitascore_explanation"]
            self.assertIn(expected_factor, explanation["positive_factors"])

    async def test_multilingual_adjustment_and_guard_strings_are_localized(self) -> None:
        adjustment_payload = {
            "name": "Plain Almonds",
            "brand": "Test",
            "unit": "g",
            "categories": ["nuts"],
            "ingredients_text": "almonds",
            "sugar_g": 4.0,
            "salt_g": 0.01,
            "sat_fat_g": 3.8,
            "protein_g": 21.0,
            "energy_kcal": 610,
            "serving_size": 30,
        }
        limited_payload = {
            "name": "Mystery Snack",
            "brand": "Test",
            "unit": "g",
            "categories": ["snack"],
            "ingredients_text": "corn, salt",
            "protein_g": 2.0,
        }
        expected_adjustment_reason = {
            "en": "The final score was kept more conservative for a more cautious assessment of this product type",
            "de": "Der Endwert wurde für eine vorsichtigere Bewertung dieses Produkttyps etwas konservativer gehalten",
            "fr": "Le score final a été maintenu plus prudent pour une évaluation plus prudente de ce type de produit",
            "el": "Το τελικό σκορ κρατήθηκε πιο συντηρητικό για πιο προσεκτική αξιολόγηση αυτού του τύπου προϊόντος",
        }
        expected_guard_note = {
            "en": "Incomplete nutrition data kept the basic nutrition assessment more cautious.",
            "de": "Unvollständige Nährwertangaben machten die grundlegende Ernährungsbewertung vorsichtiger.",
            "fr": "Des données nutritionnelles incomplètes ont rendu l'évaluation nutritionnelle de base plus prudente.",
            "el": "Η ελλιπής διατροφική πληροφορία κράτησε πιο προσεκτική τη βασική διατροφική αξιολόγηση.",
        }

        for lang, expected_reason in expected_adjustment_reason.items():
            adjustment_result = await analyze_manual_product(adjustment_payload, lang=lang)
            adjustments = {
                item["code"]: item for item in adjustment_result["vitascore_explanation"]["score_adjustments"]
            }
            self.assertEqual(adjustments["whole_food_cap"]["reason"], expected_reason)

            limited_result = await analyze_manual_product(limited_payload, lang=lang)
            self.assertIn(expected_guard_note[lang], limited_result["vitascore_explanation"]["confidence_notes"])


if __name__ == "__main__":
    unittest.main()
