import unittest
from unittest.mock import patch

from app.services import scanner_service as ss
from app.services.openfoodfacts_service import OFFResult


BARCODE = "5201037713061"
EXPECTED_NAME = "\u0394\u0395\u039b\u03a4\u0391 advance \u03b5\u03c0\u03b9\u03b4\u03cc\u03c1\u03c0\u03b9\u03bf \u03b3\u03b9\u03b1\u03bf\u03c5\u03c1\u03c4\u03b9\u03bf\u03cd \u03bc\u03ae\u03bb\u03bf \u03bc\u03c0\u03b1\u03bd\u03ac\u03bd\u03b1"


def _ean13_check_digit(code: str) -> int:
    digits = [int(ch) for ch in code[:12]]
    return (10 - ((sum(digits[0::2]) + 3 * sum(digits[1::2])) % 10)) % 10


class DeltaAdvanceRegressionTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        ss._SCAN_RESULT_CACHE.clear()

    def test_delta_advance_barcode_is_valid_ean13(self) -> None:
        self.assertRegex(BARCODE, r"^\d{13}$")
        self.assertEqual(_ean13_check_digit(BARCODE), int(BARCODE[-1]))

    async def test_delta_advance_uses_curated_local_product_with_label_supported_data(self) -> None:
        with patch.object(
            ss,
            "fetch_off_product",
            side_effect=AssertionError("curated local product should be used before OpenFoodFacts"),
        ), patch.object(
            ss,
            "_lookup_external_safety_alerts",
            return_value={"checked": False, "source": None, "has_matches": False, "alerts": []},
        ):
            result = await ss.scan_product(BARCODE, lang="el")

        self.assertFalse(result.get("error"))
        self.assertEqual(result["source"], "local")
        self.assertEqual(result["matched_by"], "local_db")
        self.assertEqual(result["lookup_state"], "found_but_incomplete")
        self.assertEqual(result["analysis_state"], "partial_analysis")
        self.assertEqual(result["analysis_confidence"], "medium")
        self.assertEqual(result["scan_resolution_state"], "final_resolved_product")
        self.assertTrue(result["final_render_allowed"])

        product = result["product"]
        self.assertEqual(product["barcode"], BARCODE)
        self.assertEqual(product["name"], EXPECTED_NAME)
        self.assertEqual(product["brand"], "\u0394\u0395\u039b\u03a4\u0391 / Delta")
        self.assertEqual(product["quantity"], "280 g")
        self.assertEqual(product["categories"], ["Yogurt dessert", "Baby dairy product"])

        nutrition = result["nutrition_per_100"]
        self.assertEqual(nutrition["unit"], "g")
        self.assertEqual(nutrition["energy_kcal"], 86.0)
        self.assertEqual(nutrition["fat_g"], 2.4)
        self.assertEqual(nutrition["sat_fat_g"], 1.5)
        self.assertEqual(nutrition["carb_g"], 11.9)
        self.assertEqual(nutrition["sugar_g"], 9.8)
        self.assertEqual(nutrition["protein_g"], 3.5)
        self.assertEqual(nutrition["salt_g"], 0.1)
        self.assertEqual(nutrition["serving_size"], 140.0)
        self.assertEqual(result["meta"]["serving"], {"amount": 140.0, "unit": "g", "source": "from_product"})

        ingredient_names = [item["name"] for item in result["ingredients"]]
        self.assertIn("fresh whole and skimmed cow's milk", ingredient_names)
        self.assertIn("milk protein 7%", ingredient_names)
        self.assertIn("apple 3%", ingredient_names)
        self.assertIn("banana 3%", ingredient_names)
        self.assertIn("vitamin mix B6 niacin folic acid vitamin C", ingredient_names)
        self.assertIn("flavouring", ingredient_names)
        self.assertIn("yogurt culture", ingredient_names)

        self.assertEqual({item["id"] for item in result["allergen_detection"]["detected"]}, {"milk_lactose"})
        self.assertEqual(result["allergen_detection"]["possible_signals"], [])
        self.assertEqual(result["ingredients_intelligence"]["detected_e_numbers"], [])
        self.assertIn("additives", result["lookup_missing_fields"])

        self.assertEqual(result["dietary_signals"]["halal"]["status"], "unclear")
        self.assertEqual(result["dietary_signals"]["vegan"]["status"], "unclear")
        self.assertEqual(result["dietary_signals"]["vegetarian"]["status"], "unclear")

        curated_review = result["meta"]["curated_review"]
        self.assertEqual(curated_review["source"], "tester_label_photos")
        self.assertIn("new closed-tester product from Froso", curated_review["note"])
        self.assertIn("micronutrients retained as raw label metadata only", curated_review["note"])
        self.assertEqual(curated_review["confidence"], 0.86)

    def test_delta_advance_raw_label_metadata_is_present(self) -> None:
        products = ss._load_json(ss.PRODUCTS_FILE, {}).get("products", [])
        item = next(product for product in products if product.get("barcode") == BARCODE)

        self.assertEqual(item["package_quantity"], "2 x 140 g")
        self.assertEqual(item["servings_per_package"], 2)
        self.assertEqual(item["age_hint"], "from 6 months")
        self.assertEqual(item["serving_size"], {"value": 140.0, "unit": "g"})

        per100 = item["nutrients_per_100"]
        self.assertEqual(per100["energy_kj"], 361.0)
        self.assertEqual(per100["energy_kcal"], 86.0)
        self.assertEqual(per100["fat_g"], 2.4)
        self.assertEqual(per100["saturated_fat_g"], 1.5)
        self.assertEqual(per100["carbohydrates_g"], 11.9)
        self.assertEqual(per100["sugars_g"], 9.8)
        self.assertEqual(per100["protein_g"], 3.5)
        self.assertEqual(per100["salt_g"], 0.1)

        micronutrients = item["raw_label_metadata"]["micronutrients_per_100"]
        self.assertEqual(micronutrients["calcium_mg"], 100.0)
        self.assertEqual(micronutrients["iron_mg"], 1.2)
        self.assertEqual(micronutrients["vitamin_b12_ug"], 0.25)
        self.assertEqual(micronutrients["vitamin_b6_mg"], 0.105)
        self.assertEqual(micronutrients["vitamin_b2_mg"], 0.12)
        self.assertEqual(micronutrients["niacin_mg"], 1.35)
        self.assertEqual(micronutrients["folic_acid_ug"], 15.0)

    async def test_delta_advance_off_404_without_local_entry_remains_not_found(self) -> None:
        with patch.object(
            ss,
            "fetch_off_product",
            return_value=OFFResult(ok=False, status=404, error="Product not found in OpenFoodFacts"),
        ), patch.object(
            ss,
            "_lookup_external_safety_alerts",
            return_value={"checked": False, "source": None, "has_matches": False, "alerts": []},
        ), patch.object(
            ss,
            "_find_local_product",
            return_value=None,
        ):
            result = await ss.scan_product(BARCODE, lang="en")

        self.assertEqual(result["lookup_state"], "not_found")
        self.assertEqual(result["product"]["barcode"], BARCODE)
        self.assertEqual(result["product"]["name"], "Unknown product")
        self.assertFalse(result["final_render_allowed"])


if __name__ == "__main__":
    unittest.main()
