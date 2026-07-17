import unittest
from unittest.mock import patch

from app.services import scanner_service as ss
from app.services.openfoodfacts_service import OFFResult


BARCODE = "5201002004064"
SUPPORTED_LANGS = ("el", "en", "de", "fr")
NUTRITION_FIELDS = ("energy_kcal", "fat_g", "carb_g", "sugar_g", "salt_g", "sat_fat_g", "protein_g")
EXPECTED_NOT_FOUND_MISSING_FIELDS = {"ingredients", "nutriments", "serving_size", "additives", "categories"}
EXPECTED_CURATED_MISSING_FIELDS = {"ingredients", "additives", "categories"}


def _ean13_check_digit(code: str) -> int:
    digits = [int(ch) for ch in code[:12]]
    return (10 - ((sum(digits[0::2]) + 3 * sum(digits[1::2])) % 10)) % 10


class ClosedTestingMultipackRegressionTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        ss._SCAN_RESULT_CACHE.clear()

    def test_reported_jotis_multipack_barcode_is_valid_ean13(self) -> None:
        self.assertRegex(BARCODE, r"^\d{13}$")
        self.assertEqual(_ean13_check_digit(BARCODE), int(BARCODE[-1]))

    async def test_reported_jotis_multipack_off_404_returns_not_found_without_local_entry(self) -> None:
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
            for lang in SUPPORTED_LANGS:
                with self.subTest(lang=lang):
                    ss._SCAN_RESULT_CACHE.clear()
                    result = await ss.scan_product(BARCODE, lang=lang)

                    self.assertEqual(result["lookup_state"], "not_found")
                    self.assertEqual(result["analysis_state"], "limited_estimate")
                    self.assertEqual(result["analysis_confidence"], "low")
                    self.assertEqual(result["scan_resolution_state"], "fallback_estimate_only")
                    self.assertEqual(result["product"]["barcode"], BARCODE)
                    self.assertEqual(result["product"]["name"], "Unknown product")
                    self.assertFalse(result["final_render_allowed"])
                    self.assertTrue(EXPECTED_NOT_FOUND_MISSING_FIELDS.issubset(set(result["lookup_missing_fields"])))
                    self.assertTrue(all(result["nutrition_per_100"].get(field) is None for field in NUTRITION_FIELDS))

    async def test_reported_jotis_multipack_uses_curated_tester_nutrition_entry(self) -> None:
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
        self.assertEqual(result["analysis_confidence"], "low")
        self.assertEqual(result["scan_resolution_state"], "final_resolved_product")
        self.assertTrue(result["final_render_allowed"])
        self.assertEqual(result["final_render_reason"], "resolved_product_payload")

        product = result["product"]
        self.assertEqual(product["barcode"], BARCODE)
        self.assertEqual(product["name"], "\u0393\u0399\u03a9\u03a4\u0397\u03a3 Sweet & Balance \u03a6\u03c1\u03bf\u03c5\u03af \u03b6\u03b5\u03bb\u03ad \u03bc\u03b5 \u03b3\u03b5\u03cd\u03c3\u03b7 \u03ba\u03b5\u03c1\u03ac\u03c3\u03b9")
        self.assertEqual(product["brand"], "\u0393\u0399\u03a9\u03a4\u0397\u03a3 / Jotis")
        self.assertEqual(product["quantity"], "2 x 150 g")
        self.assertEqual(product["categories"], [])

        self.assertEqual(set(result["lookup_missing_fields"]), EXPECTED_CURATED_MISSING_FIELDS)
        nutrition = result["nutrition_per_100"]
        self.assertEqual(nutrition["unit"], "g")
        self.assertEqual(nutrition["energy_kcal"], 26.0)
        self.assertEqual(nutrition["fat_g"], 0.0)
        self.assertEqual(nutrition["sat_fat_g"], 0.0)
        self.assertEqual(nutrition["carb_g"], 5.6)
        self.assertEqual(nutrition["sugar_g"], 0.1)
        self.assertEqual(nutrition["protein_g"], 1.7)
        self.assertEqual(nutrition["salt_g"], 0.18)
        self.assertEqual(nutrition["serving_size"], 150.0)
        self.assertEqual(result["meta"]["serving"], {"amount": 150.0, "unit": "g", "source": "from_product"})

        self.assertEqual(result["ingredients"], [])
        self.assertEqual(result["allergen_detection"]["detected"], [])
        self.assertEqual(result["allergen_detection"]["possible_signals"], [])
        self.assertEqual(result["ingredients_intelligence"]["detected_e_numbers"], [])
        self.assertEqual(result["dietary_signals"]["vegan"]["status"], "unclear")
        self.assertEqual(result["dietary_signals"]["vegetarian"]["status"], "unclear")
        self.assertEqual(result["dietary_signals"]["halal"]["status"], "unclear")

        curated_review = result["meta"]["curated_review"]
        self.assertEqual(curated_review["source"], "tester_label_photos")
        self.assertIn("fourth closed-testing cycle tester feedback", curated_review["note"])
        self.assertIn("curator reviewed identity, quantity, serving metadata, and nutrition", curated_review["note"])
        self.assertIn("ingredients, allergens, additives, and dietary claims not transcribed", curated_review["note"])
        self.assertIn("no external database verification", curated_review["note"])
        self.assertEqual(curated_review["confidence"], 0.82)

    def test_reported_jotis_multipack_curated_raw_label_metadata_is_present(self) -> None:
        products = ss._load_json(ss.PRODUCTS_FILE, {}).get("products", [])
        item = next(product for product in products if product.get("barcode") == BARCODE)

        self.assertEqual(item["package_quantity"], "2 x 150 g")
        self.assertEqual(item["servings_per_package"], 2)
        self.assertEqual(item["serving_size"], {"value": 150.0, "unit": "g"})

        per100 = item["nutrients_per_100"]
        self.assertEqual(per100["energy_kj"], 108.0)
        self.assertEqual(per100["energy_kcal"], 26.0)
        self.assertEqual(per100["carbohydrates_g"], 5.6)
        self.assertEqual(per100["sugars_g"], 0.1)
        self.assertEqual(per100["polyols_g"], 5.2)
        self.assertEqual(per100["fiber_g"], 0.0)

        serving = item["nutrients_per_serving"]
        self.assertEqual(serving["serving_size"], 150.0)
        self.assertEqual(serving["energy_kj"], 162.0)
        self.assertEqual(serving["energy_kcal"], 40.0)
        self.assertEqual(serving["carbohydrates_g"], 8.4)
        self.assertEqual(serving["sugars_g"], 0.1)
        self.assertEqual(serving["polyols_g"], 7.7)
        self.assertEqual(serving["fiber_g"], 0.0)


if __name__ == "__main__":
    unittest.main()
