import unittest
from unittest.mock import patch

from app.services import scanner_service as ss
from app.services.openfoodfacts_service import OFFResult


BARCODE = "5205941062666"


def _ean13_check_digit(code: str) -> int:
    digits = [int(ch) for ch in code[:12]]
    return (10 - ((sum(digits[0::2]) + 3 * sum(digits[1::2])) % 10)) % 10


class SoyDrinkRegressionTests(unittest.IsolatedAsyncioTestCase):
    def test_reported_soy_drink_barcode_is_valid_ean13(self) -> None:
        self.assertRegex(BARCODE, r"^\d{13}$")
        self.assertEqual(_ean13_check_digit(BARCODE), int(BARCODE[-1]))

    async def test_reported_soy_drink_off_404_returns_unresolved_not_found(self) -> None:
        with patch.object(
            ss,
            "fetch_off_product",
            return_value=OFFResult(ok=False, status=404, error="Product not found in OpenFoodFacts"),
        ), patch.object(
            ss,
            "_lookup_external_safety_alerts",
            return_value={"checked": False, "source": None, "has_matches": False, "alerts": []},
        ):
            result = await ss.scan_product(BARCODE, lang="en")

        self.assertFalse(result.get("error"))
        self.assertEqual(result["lookup_state"], "not_found")
        self.assertEqual(result["analysis_state"], "limited_estimate")
        self.assertEqual(result["scan_resolution_state"], "fallback_estimate_only")
        self.assertEqual(result["product"]["barcode"], BARCODE)
        self.assertEqual(result["meta"]["final_render_allowed"], False)

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


if __name__ == "__main__":
    unittest.main()
