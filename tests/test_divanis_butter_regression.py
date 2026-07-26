import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from app.main import app
from app.services import scanner_service as ss


BARCODE = "5205757000067"
SUPPORTED_LANGS = ("el", "en", "de", "fr")


def _ean13_check_digit(code: str) -> int:
    digits = [int(ch) for ch in code[:12]]
    return (10 - ((sum(digits[0::2]) + 3 * sum(digits[1::2])) % 10)) % 10


class DivanisButterRegressionTests(unittest.TestCase):
    def setUp(self) -> None:
        ss._SCAN_RESULT_CACHE.clear()
        self.client = TestClient(app)

    def test_divanis_butter_barcode_is_valid_ean13(self) -> None:
        self.assertRegex(BARCODE, r"^\d{13}$")
        self.assertEqual(_ean13_check_digit(BARCODE), int(BARCODE[-1]))

    def test_divanis_butter_scan_resolves_from_curated_local_data_for_all_languages(self) -> None:
        with patch.object(
            ss,
            "fetch_off_product",
            side_effect=AssertionError("curated local product should be used before OpenFoodFacts"),
        ), patch.object(
            ss,
            "_lookup_external_safety_alerts",
            return_value={"checked": False, "source": None, "has_matches": False, "alerts": []},
        ):
            for lang in SUPPORTED_LANGS:
                with self.subTest(lang=lang):
                    ss._SCAN_RESULT_CACHE.clear()
                    response = self.client.get(f"/scan/{BARCODE}", params={"lang": lang})
                    self.assertEqual(response.status_code, 200)
                    result = response.json()

                    self.assertFalse(result.get("error"))
                    self.assertEqual(result["source"], "local")
                    self.assertEqual(result["matched_by"], "local_db")
                    self.assertEqual(result["lookup_state"], "found_but_incomplete")
                    self.assertEqual(result["analysis_state"], "partial_analysis")
                    self.assertEqual(result["scan_resolution_state"], "final_resolved_product")
                    self.assertTrue(result["final_render_allowed"])

                    product = result["product"]
                    self.assertEqual(product["barcode"], BARCODE)
                    self.assertEqual(product["brand"], "\u03a4\u03c5\u03c1\u03bf\u03ba\u03bf\u03bc\u03b5\u03af\u03bf \u0394\u03b9\u03b2\u03ac\u03bd\u03b7\u03c2 / Divanis")
                    self.assertEqual(product["name"], "\u0391\u03b3\u03bd\u03cc \u03b2\u03bf\u03cd\u03c4\u03c5\u03c1\u03bf \u03b3\u03ac\u03bb\u03b1\u03ba\u03c4\u03bf\u03c2")
                    self.assertEqual(product["quantity"], "500 g")

                    nutrition = result["nutrition_per_100"]
                    self.assertEqual(nutrition["unit"], "g")
                    self.assertEqual(nutrition["energy_kcal"], 895.0)
                    self.assertEqual(nutrition["fat_g"], 99.0)
                    self.assertEqual(nutrition["sat_fat_g"], 57.0)
                    self.assertEqual(nutrition["carb_g"], 0.0)
                    self.assertEqual(nutrition["sugar_g"], 0.0)
                    self.assertEqual(nutrition["protein_g"], 0.0)
                    self.assertEqual(nutrition["salt_g"], 0.1)
                    self.assertEqual(nutrition["serving_size"], 10.0)
                    self.assertEqual(result["meta"]["serving"], {"amount": 10.0, "unit": "g", "source": "from_product"})

                    allergen = result["allergen_detection"]
                    self.assertEqual(allergen["coverage"], "high")
                    self.assertEqual([item["id"] for item in allergen["detected"]], ["milk_lactose"])
                    self.assertEqual(allergen["detected"][0]["confidence"], "high")
                    self.assertEqual(allergen["detected"][0]["source"], "barcode_product_data")
                    self.assertEqual(allergen["possible_signals"], [])

                    self.assertIn("additives", result["lookup_missing_fields"])
                    self.assertEqual(result["ingredients_meta"]["source_language"], "el")
                    self.assertEqual(result["ingredients_intelligence"]["detected_e_numbers"], [])
                    self.assertEqual(result["dietary_signals"]["halal"]["status"], "unclear")
                    self.assertEqual(result["dietary_signals"]["vegetarian"]["status"], "unclear")

    def test_divanis_butter_curated_raw_label_metadata_is_present(self) -> None:
        products = ss._load_json(ss.PRODUCTS_FILE, {}).get("products", [])
        item = next(product for product in products if product.get("barcode") == BARCODE)

        self.assertEqual(item["key"], "divanis_pure_milk_butter_sheep_goat_milk_5205757000067")
        self.assertEqual(item["brand"], "\u03a4\u03c5\u03c1\u03bf\u03ba\u03bf\u03bc\u03b5\u03af\u03bf \u0394\u03b9\u03b2\u03ac\u03bd\u03b7\u03c2 / Divanis")
        self.assertEqual(item["name"], "\u0391\u03b3\u03bd\u03cc \u03b2\u03bf\u03cd\u03c4\u03c5\u03c1\u03bf \u03b3\u03ac\u03bb\u03b1\u03ba\u03c4\u03bf\u03c2")
        self.assertEqual(item["name_en"], "Pure milk butter")
        self.assertEqual(item["quantity"], "500 g")
        self.assertEqual(item["serving_size"], {"value": 10.0, "unit": "g"})

        per100 = item["nutrients_per_100"]
        self.assertEqual(per100["energy_kj"], 3672.0)
        self.assertEqual(per100["energy_kcal"], 895.0)
        self.assertEqual(per100["fat_g"], 99.0)
        self.assertEqual(per100["saturated_fat_g"], 57.0)
        self.assertEqual(per100["carbohydrates_g"], 0.0)
        self.assertEqual(per100["sugars_g"], 0.0)
        self.assertEqual(per100["protein_g"], 0.0)
        self.assertEqual(per100["sodium_g"], 0.0)
        self.assertEqual(per100["salt_g"], 0.1)

        serving = item["nutrients_per_serving"]
        self.assertEqual(serving["serving_size"], 10.0)
        self.assertEqual(serving["energy_kj"], 367.0)
        self.assertEqual(serving["energy_kcal"], 89.0)
        self.assertEqual(serving["fat_g"], 9.9)
        self.assertEqual(serving["saturated_fat_g"], 5.7)
        self.assertEqual(serving["salt_g"], 0.01)

        self.assertEqual(item["allergen_info"]["allergens_tags"], ["en:milk"])
        self.assertIn("milk butter", item["allergen_info"]["allergens_from_ingredients"])
        self.assertEqual(item["ingredients"]["confidence"], "partial")
        self.assertEqual(item["raw_label_metadata"]["certification_text"], "ISO 22000")
        self.assertEqual(item["raw_label_metadata"]["milk_origin"], "EU")

        curated_review = item["review"]
        self.assertEqual(curated_review["source"], "tester_label_photos")
        self.assertIn("ingredient confidence remains partial", curated_review["note"])
        self.assertIn("additives, E-numbers, and dietary claims not added", curated_review["note"])


if __name__ == "__main__":
    unittest.main()
