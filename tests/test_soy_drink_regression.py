import unittest
from pathlib import Path
from unittest.mock import patch

from app.services import scanner_service as ss
from app.services.openfoodfacts_service import OFFResult


BARCODE = "5205941062666"
SUPPORTED_LANGS = ("en", "el", "de", "fr")
NUTRITION_FIELDS = ("energy_kcal", "fat_g", "carb_g", "sugar_g", "salt_g", "sat_fat_g", "protein_g")
EXPECTED_MISSING_FIELDS = {"ingredients", "nutriments", "serving_size", "additives", "categories"}


def _ean13_check_digit(code: str) -> int:
    digits = [int(ch) for ch in code[:12]]
    return (10 - ((sum(digits[0::2]) + 3 * sum(digits[1::2])) % 10)) % 10


class SoyDrinkRegressionTests(unittest.IsolatedAsyncioTestCase):
    def test_reported_soy_drink_barcode_is_valid_ean13(self) -> None:
        self.assertRegex(BARCODE, r"^\d{13}$")
        self.assertEqual(_ean13_check_digit(BARCODE), int(BARCODE[-1]))

    async def test_reported_soy_drink_off_404_returns_unknown_product_fallback_for_all_languages(self) -> None:
        ss._SCAN_RESULT_CACHE.clear()
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
                    result = await ss.scan_product(BARCODE, lang=lang)

                    self.assertFalse(result.get("error"))
                    self.assertEqual(result["lookup_state"], "not_found")
                    self.assertEqual(result["analysis_state"], "limited_estimate")
                    self.assertEqual(result["scan_resolution_state"], "fallback_estimate_only")
                    self.assertEqual(result["product"]["barcode"], BARCODE)
                    self.assertEqual(result["final_render_allowed"], False)
                    self.assertEqual(result["meta"]["final_render_allowed"], False)
                    self.assertIn("product", result)
                    self.assertIn("nutrition_per_100", result)
                    self.assertIn("vitascore_explanation", result)
                    self.assertGreater(len(result.keys()), 10)
                    self.assertTrue(EXPECTED_MISSING_FIELDS.issubset(set(result["lookup_missing_fields"])))

                    nutrition = result["nutrition_per_100"]
                    self.assertTrue(all(nutrition.get(field) is None for field in NUTRITION_FIELDS))

                    notes = result["vitascore_explanation"]["confidence_notes"]
                    self.assertGreaterEqual(len(notes), 1)
                    if lang == "en":
                        self.assertTrue(any("limited estimate" in note for note in notes))
                    elif lang == "el":
                        self.assertTrue(any("\u03c0\u03b5\u03c1\u03b9\u03bf\u03c1\u03b9\u03c3\u03bc\u03ad\u03bd\u03b7" in note for note in notes))
                    elif lang == "de":
                        self.assertTrue(any("begrenzte" in note for note in notes))
                    elif lang == "fr":
                        self.assertTrue(any("estimation" in note for note in notes))

    async def test_reported_soy_drink_lookup_uses_curated_tester_photo_data(self) -> None:
        ss._SCAN_RESULT_CACHE.clear()
        with patch.object(
            ss,
            "fetch_off_product",
            side_effect=AssertionError("curated local product should be used before OpenFoodFacts"),
        ), patch.object(
            ss,
            "_lookup_external_safety_alerts",
            return_value={"checked": False, "source": None, "has_matches": False, "alerts": []},
        ):
            result = await ss.scan_product(BARCODE, lang="en")

        self.assertFalse(result.get("error"))
        self.assertEqual(result["source"], "local")
        self.assertEqual(result["matched_by"], "local_db")
        self.assertNotEqual(result["lookup_state"], "not_found")
        self.assertEqual(result["product"]["barcode"], BARCODE)
        self.assertEqual(result["product"]["name"], "Μικρές φάρμες του βουνού - Ρόφημα από σόγια")
        self.assertEqual(result["product"]["brand"], "Μικρές φάρμες του βουνού")
        self.assertEqual(result["product"]["quantity"], "1L")
        self.assertIn("ingredients", result["lookup_missing_fields"])
        curated_review = result["meta"]["curated_review"]
        self.assertEqual(curated_review["source"], "tester_label_photos")
        self.assertIn("curator reviewed", curated_review["note"])
        self.assertIn("no external database verification", curated_review["note"])
        self.assertEqual(curated_review["confidence"], 0.8)

        nutrition = result["nutrition_per_100"]
        self.assertEqual(nutrition["unit"], "ml")
        self.assertEqual(nutrition["energy_kcal"], 31.0)
        self.assertEqual(nutrition["fat_g"], 1.5)
        self.assertEqual(nutrition["sat_fat_g"], 0.3)
        self.assertEqual(nutrition["carb_g"], 0.9)
        self.assertEqual(nutrition["sugar_g"], 0.5)
        self.assertEqual(nutrition["protein_g"], 3.3)
        self.assertEqual(nutrition["salt_g"], 0.09)
        self.assertEqual(result["ingredients"], [])

    def test_reported_soy_drink_nutrition_ocr_rescue_accepts_bilingual_comma_decimal_table(self) -> None:
        payload = {
            "nutrition_image_data_url": "data:image/jpeg;base64,AAA",
            "nutrition_crop_applied": True,
            "existing_key": BARCODE,
            "existing_product": {
                "name": "Mikres farmes tou vounou - Soy drink",
                "brand": "Mikres farmes tou vounou",
                "categories": ["Soy drink"],
            },
            "existing_analysis": {
                "key": BARCODE,
                "nutrition_per_100": {"unit": "ml"},
                "meta": {"serving": {"unit": "ml"}},
            },
        }

        text = (
            "Ανά / Per 100 ml "
            "Ενέργεια / Energy 130 kJ / 31 kcal "
            "Λιπαρά / Fat 1,5 g "
            "Κορεσμένα / Saturates 0,3 g "
            "Υδατάνθρακες / Carbohydrate 0,9 g "
            "Σάκχαρα / Sugars 0,5 g "
            "Πρωτεΐνη / Protein 3,3 g "
            "Αλάτι / Salt 0,09 g"
        )

        rescued, debug = ss._evaluate_nutrition_photo_rescue(payload, text)

        self.assertIsInstance(rescued, dict)
        self.assertEqual(debug["parser_acceptance_reason"], "accepted_structured")
        self.assertEqual(debug["rescued_field_count"], 7)
        nutrition = rescued["nutrition_per_100"]
        self.assertEqual(nutrition["unit"], "ml")
        self.assertEqual(nutrition["energy_kcal"], 31.0)
        self.assertEqual(nutrition["fat_g"], 1.5)
        self.assertEqual(nutrition["sat_fat_g"], 0.3)
        self.assertEqual(nutrition["carb_g"], 0.9)
        self.assertEqual(nutrition["sugar_g"], 0.5)
        self.assertEqual(nutrition["protein_g"], 3.3)
        self.assertEqual(nutrition["salt_g"], 0.09)

    def test_frontend_recovery_i18n_keys_exist_for_all_supported_languages(self) -> None:
        content = Path("app/frontend/index.html").read_text(encoding="utf-8")
        for key in (
            "scan_state_unresolved_title",
            "scan_state_unresolved_body",
            "fallback_continue",
            "fallback_ingredient_photo",
            "fallback_nutrition_photo",
            "fallback_manual_entry",
            "err_not_found",
        ):
            with self.subTest(key=key):
                self.assertGreaterEqual(content.count(f"{key}:"), len(SUPPORTED_LANGS))


if __name__ == "__main__":
    unittest.main()
