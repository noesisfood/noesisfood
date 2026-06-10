import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from app.services import scanner_service as ss
from app.services.openfoodfacts_service import OFFResult


AMSTEL_RADLER_OFF_PAYLOAD = {
    "status": 1,
    "code": "5201261011148",
    "product": {
        "code": "5201261011148",
        "product_name": "Amstel Radler Lemon",
        "brands": "Amstel",
        "quantity": "330 ml",
        "categories": "Beverages, Alcoholic beverages, Beers, Radler",
        "categories_tags": ["en:beverages", "en:alcoholic-beverages", "en:beers", "en:radlers"],
        "labels": "ALC. 2% VOL.",
        "labels_tags": [],
        "ingredients_text": "beer, lemon juice, sugar",
        "serving_size": "330 ml",
        "nutrition_data_per": "100ml",
        "nutriments": {
            "energy-kcal_100g": 38,
            "sugars_100g": 4.7,
            "salt_100g": 0.01,
            "saturated-fat_100g": 0,
            "proteins_100g": 0.2,
        },
    },
}


class P0SafetyDataQualityTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        ss._SCAN_RESULT_CACHE.clear()

    async def test_amstel_radler_ean_is_confirmed_alcoholic_and_capped(self) -> None:
        with patch.object(
            ss,
            "fetch_off_product",
            AsyncMock(return_value=OFFResult(ok=True, status=200, payload=AMSTEL_RADLER_OFF_PAYLOAD)),
        ):
            result = await ss.scan_product("5201261011148", lang="en")

        self.assertEqual(result["alcohol_status"]["status"], "alcoholic")
        self.assertTrue(result["alcohol_status"]["is_alcoholic"])
        self.assertLessEqual(result["vitascore"], 59)
        self.assertNotEqual(result["analysis_confidence"], "high")
        self.assertEqual(
            result["vitascore_breakdown"]["alcohol_status_guard"]["reason"],
            "confirmed_alcoholic_informational_only",
        )

    async def test_openfoodfacts_alcohol_category_tags_are_detected(self) -> None:
        result = await ss.analyze_manual_product(
            {
                "name": "Test Lager",
                "brand": "Test",
                "unit": "ml",
                "categories_tags": ["en:beverages", "en:alcoholic-beverages"],
                "ingredients_text": "water, barley malt, hops",
                "energy_kcal": 42,
                "sugar_g": 0.5,
                "salt_g": 0.01,
                "sat_fat_g": 0,
                "protein_g": 0.4,
                "serving_size": 330,
            },
            lang="en",
        )

        self.assertEqual(result["alcohol_status"]["status"], "alcoholic")
        self.assertLessEqual(result["vitascore"], 59)

    async def test_verified_zero_zero_beer_is_not_treated_as_alcoholic(self) -> None:
        result = await ss.analyze_manual_product(
            {
                "name": "Test Beer 0.0 Alcohol-Free",
                "brand": "Test",
                "unit": "ml",
                "categories_tags": ["en:beverages", "en:beers"],
                "ingredients_text": "water, barley malt, hops",
                "energy_kcal": 18,
                "sugar_g": 1,
                "salt_g": 0.01,
                "sat_fat_g": 0,
                "protein_g": 0.2,
                "serving_size": 330,
            },
            lang="en",
        )

        self.assertEqual(result["alcohol_status"]["status"], "alcohol_free")
        self.assertFalse(result["alcohol_status"]["is_alcoholic"])

    async def test_unverified_radler_like_product_is_capped_and_not_high_confidence(self) -> None:
        result = await ss.analyze_manual_product(
            {
                "name": "Lemon Radler Style Drink",
                "brand": "Test",
                "unit": "ml",
                "categories": ["beverage"],
                "ingredients_text": "water, lemon, malt extract",
                "energy_kcal": 25,
                "sugar_g": 3,
                "salt_g": 0.01,
                "sat_fat_g": 0,
                "protein_g": 0.1,
                "serving_size": 330,
            },
            lang="en",
        )

        self.assertEqual(result["alcohol_status"]["status"], "unverified_beer_radler")
        self.assertLessEqual(result["vitascore"], 59)
        self.assertNotEqual(result["analysis_confidence"], "high")

    async def test_missing_salt_is_not_scored_as_favorable_zero(self) -> None:
        result = await ss.analyze_manual_product(
            {
                "name": "Incomplete Cereal",
                "brand": "Test",
                "unit": "g",
                "categories": ["cereal"],
                "ingredients_text": "oats, corn",
                "energy_kcal": 80,
                "sugar_g": 2,
                "sat_fat_g": 0,
                "protein_g": 2,
                "serving_size": 40,
            },
            lang="en",
        )

        self.assertIn("salt_g", result["data_quality"]["missing_core_fields"])
        self.assertLessEqual(result["vitascore_explanation"]["basic_nutrition_score"], 82)
        self.assertNotEqual(result["analysis_confidence"], "high")
        self.assertTrue(result["vitascore_breakdown"]["nutrition_completeness_guard"]["applied"])

    async def test_partial_missing_data_result_is_confidence_limited(self) -> None:
        result = await ss.analyze_manual_product(
            {
                "name": "Partial Snack",
                "brand": "Test",
                "unit": "g",
                "categories": ["snack"],
                "ingredients_text": "corn, salt",
                "energy_kcal": 80,
                "sugar_g": 2,
                "serving_size": 40,
            },
            lang="en",
        )

        self.assertEqual(result["analysis_state"], "partial_analysis")
        self.assertNotEqual(result["analysis_confidence"], "high")
        self.assertLessEqual(result["vitascore_explanation"]["basic_nutrition_score"], 78)
        self.assertLessEqual(result["vitascore"], 78)

    async def test_unreviewed_photo_enrichment_is_not_used_for_future_scan_scoring(self) -> None:
        with patch.object(
            ss,
            "_load_product_enrichments",
            return_value=[
                {
                    "barcode": "123",
                    "review_status": "unreviewed",
                    "updated_at": "2026-06-01T00:00:00Z",
                    "captured_payload": {
                        "nutrition_per_100": {
                            "energy_kcal": 1,
                            "sugar_g": 0,
                            "salt_g": 0,
                            "sat_fat_g": 0,
                            "protein_g": 0,
                            "unit": "g",
                        }
                    },
                }
            ],
        ):
            self.assertIsNone(ss._get_product_enrichment("123"))


class P0SafetyDataQualityUiTests(unittest.TestCase):
    def test_frontend_alcohol_and_partial_verdict_strings_cover_supported_languages(self) -> None:
        content = Path("app/frontend/index.html").read_text(encoding="utf-8")

        for key in (
            "compare_alcohol_informational",
            "quick_verdict_informational",
            "quick_verdict_partial",
        ):
            self.assertEqual(content.count(f"{key}:"), 4)

    def test_frontend_blocks_promotional_alcohol_verdict_and_comparison_winner(self) -> None:
        content = Path("app/frontend/index.html").read_text(encoding="utf-8")

        self.assertIn("function quickVerdictLabel(score, data = null)", content)
        self.assertIn('alcoholStatus === "alcoholic"', content)
        self.assertIn('alcoholStatus === "unverified_beer_radler"', content)
        self.assertIn("return t(\"quick_verdict_informational\")", content)
        self.assertIn("const alcoholInformationalOnly = !!(left.isAlcoholic || right.isAlcoholic);", content)
        self.assertIn('winner = "tie";', content)
        self.assertIn('!verdict.alcoholInformationalOnly && verdict.winner === "left"', content)
        self.assertIn('!verdict.alcoholInformationalOnly && verdict.winner === "right"', content)
        self.assertNotIn('quickVerdictLabel(score);', content)

    def test_frontend_cross_category_warning_applies_to_family_mismatch(self) -> None:
        content = Path("app/frontend/index.html").read_text(encoding="utf-8")

        self.assertIn("if (leftFamily === rightFamily) return false;", content)
        self.assertIn("return true;\n    }\n\n    function renderComparisonPanel", content)


if __name__ == "__main__":
    unittest.main()
