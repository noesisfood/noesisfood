import unittest
from unittest.mock import patch

from app.services import scanner_service as ss


BARCODE = "5201005073111"
SUPPORTED_LANGS = ("en", "el", "de", "fr")


def _ean13_check_digit(code: str) -> int:
    digits = [int(ch) for ch in code[:12]]
    return (10 - ((sum(digits[0::2]) + 3 * sum(digits[1::2])) % 10)) % 10


class AmitaFunRegressionTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        ss._SCAN_RESULT_CACHE.clear()

    def test_amita_fun_barcode_is_valid_ean13(self) -> None:
        self.assertRegex(BARCODE, r"^\d{13}$")
        self.assertEqual(_ean13_check_digit(BARCODE), int(BARCODE[-1]))

    async def test_amita_fun_resolves_from_curated_local_data_for_all_languages(self) -> None:
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
                    result = await ss.scan_product(BARCODE, lang=lang)

                    self.assertFalse(result.get("error"))
                    self.assertEqual(result["source"], "local")
                    self.assertEqual(result["matched_by"], "local_db")
                    self.assertEqual(result["scan_resolution_state"], "final_resolved_product")
                    self.assertTrue(result["final_render_allowed"])
                    self.assertEqual(result["vitascore"], 42)
                    self.assertEqual(result["ingredients_meta"]["source_language"], "el")
                    self.assertEqual(result["ingredients_meta"]["language"], "en")
                    self.assertEqual(result["meta"]["ingredients_meta"]["source_language"], "el")

                    product = result["product"]
                    self.assertEqual(product["barcode"], BARCODE)
                    self.assertEqual(product["name"], "Amita Fun 100% natural juice")
                    self.assertEqual(product["brand"], "Amita")
                    self.assertEqual(product["quantity"], "250 ml")
                    self.assertEqual(product["categories"], ["Fruit juice", "Mixed fruit juice beverage"])

                    nutrition = result["nutrition_per_100"]
                    self.assertEqual(nutrition["unit"], "ml")
                    self.assertEqual(nutrition["energy_kcal"], 45.0)
                    self.assertEqual(nutrition["fat_g"], 0.0)
                    self.assertEqual(nutrition["sat_fat_g"], 0.0)
                    self.assertEqual(nutrition["carb_g"], 10.9)
                    self.assertEqual(nutrition["sugar_g"], 10.2)
                    self.assertEqual(nutrition["protein_g"], 0.3)
                    self.assertEqual(nutrition["salt_g"], 0.01)
                    self.assertEqual(nutrition["serving_size"], 250.0)
                    self.assertEqual(result["meta"]["serving"], {"amount": 250.0, "unit": "ml", "source": "from_product"})

                    ingredient_names = [item["name"] for item in result["ingredients"]]
                    self.assertIn("apple concentrated juice 75%", ingredient_names)
                    self.assertIn("orange concentrated juice 11%", ingredient_names)
                    self.assertIn("apricot pulp 9%", ingredient_names)
                    self.assertIn("pear concentrated juice 5%", ingredient_names)
                    self.assertIn("vitamins B6 B12 C E", ingredient_names)

                    self.assertEqual(result["ingredients_intelligence"]["detected_e_numbers"], [])
                    self.assertEqual(result["ingredients_intelligence"]["e_number_details"], [])
                    self.assertEqual(result["allergen_detection"]["detected"], [])
                    self.assertEqual(result["allergen_detection"]["possible_signals"], [])
                    self.assertEqual(result["dietary_signals"]["vegan"]["status"], "unclear")
                    self.assertEqual(result["dietary_signals"]["vegetarian"]["status"], "unclear")
                    self.assertEqual(result["dietary_signals"]["halal"]["status"], "unclear")
                    self.assertIn("additives", result["lookup_missing_fields"])

    def test_amita_fun_curated_raw_label_metadata_is_present(self) -> None:
        products = ss._load_json(ss.PRODUCTS_FILE, {}).get("products", [])
        item = next(product for product in products if product.get("barcode") == BARCODE)

        self.assertEqual(item["quantity"], "250 ml")
        self.assertEqual(item["package_quantity"], "250 ml")
        self.assertEqual(item["servings_per_package"], 1)
        self.assertEqual(item["serving_size"], {"value": 250.0, "unit": "ml"})
        self.assertFalse(item.get("additives"))
        self.assertNotIn("allergen_info", item)

        per100 = item["nutrients_per_100"]
        self.assertEqual(per100["unit"], "ml")
        self.assertEqual(per100["energy_kj"], 191.0)
        self.assertEqual(per100["energy_kcal"], 45.0)
        self.assertEqual(per100["fat_g"], 0.0)
        self.assertEqual(per100["saturated_fat_g"], 0.0)
        self.assertEqual(per100["carbohydrates_g"], 10.9)
        self.assertEqual(per100["sugars_g"], 10.2)
        self.assertEqual(per100["fiber_g"], 0.4)
        self.assertEqual(per100["protein_g"], 0.3)
        self.assertEqual(per100["salt_g"], 0.01)

        serving = item["nutrients_per_serving"]
        self.assertEqual(serving["serving_size"], 250.0)
        self.assertEqual(serving["energy_kj"], 478.0)
        self.assertEqual(serving["energy_kcal"], 113.0)

        micronutrients = item["raw_label_metadata"]["micronutrients_per_100"]
        self.assertEqual(micronutrients["vitamin_b6_mg"], 0.28)
        self.assertEqual(micronutrients["vitamin_b12_ug"], 0.5)
        self.assertEqual(micronutrients["vitamin_c_mg"], 16.0)
        self.assertEqual(micronutrients["vitamin_e_mg"], 2.4)
        self.assertEqual(item["raw_label_metadata"]["micronutrients_nrv_percent"]["vitamin_c"], 20)

        curated_review = item["review"]
        self.assertEqual(curated_review["source"], "tester_label_photos")
        self.assertIn("new closed-tester product from Froso", curated_review["note"])
        self.assertIn("visible Greek label text", curated_review["note"])
        self.assertIn("additives, allergens, and dietary claims not added", curated_review["note"])
        self.assertEqual(curated_review["confidence"], 0.87)


if __name__ == "__main__":
    unittest.main()
