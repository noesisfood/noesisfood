import unittest
from unittest.mock import AsyncMock, patch

from app.services import scanner_service as ss


class AnalysisConfidenceTests(unittest.IsolatedAsyncioTestCase):
    async def test_barcode_scan_returns_barcode_backed_confidence_reason(self) -> None:
        result = await ss.scan_product("5449000000996", lang="en")

        self.assertEqual(result["analysis_confidence"], "medium")
        self.assertIn("Barcode-linked product data was available.", result["confidence_reasons"])
        self.assertIn("Core nutrition fields were available.", result["confidence_reasons"])

    async def test_manual_analysis_returns_medium_confidence_with_manual_reason(self) -> None:
        result = await ss.analyze_manual_product(
            {
                "name": "Manual Cereal",
                "brand": "Test",
                "unit": "g",
                "ingredients_text": "oats, cocoa, sugar",
                "energy_kcal": 390,
                "sugar_g": 14,
                "salt_g": 0.4,
                "sat_fat_g": 1.2,
                "protein_g": 9,
                "serving_size": 40,
            },
            lang="en",
        )

        self.assertEqual(result["analysis_confidence"], "medium")
        self.assertIn("Nutrition data was entered manually.", result["confidence_reasons"])

    async def test_photo_enrichment_adds_ocr_confidence_reasons(self) -> None:
        extracted = {
            "product_name": None,
            "brand": None,
            "ingredients_text": None,
            "categories": [],
            "nutrition_per_100": {
                "unit": "g",
                "energy_kcal": 265.0,
                "sugar_g": 1.0,
                "salt_g": 2.5,
                "sat_fat_g": 14.0,
                "protein_g": 17.0,
            },
            "confidence": "medium",
            "extracted_fields": ["nutrition_per_100", "energy_kcal", "sugar_g", "salt_g", "sat_fat_g", "protein_g"],
            "notes": "Nutrition table extracted from photo.",
            "label_kind": "nutrition",
        }
        payload = {
            "nutrition_image_data_url": "data:image/jpeg;base64,AAA",
            "nutrition_crop_applied": True,
            "existing_key": "4000000000001",
            "existing_analysis": {
                "key": "4000000000001",
                "source": "openfoodfacts",
                "product": {"barcode": "4000000000001"},
                "nutrition_per_100": {"unit": "g"},
                "meta": {"serving": {"unit": "g"}},
            },
            "existing_product": {
                "name": "Protein Cheese",
                "brand": "Test",
                "categories": ["cheese"],
                "image_url": "",
                "quantity": "",
            },
        }

        with (
            patch.object(ss, "_extract_photo_payload_with_ai", AsyncMock(return_value=extracted)),
            patch.object(ss, "_persist_product_enrichment", return_value={}),
        ):
            result = await ss.analyze_photo_product(payload, lang="en")

        self.assertEqual(result["analysis_confidence"], "medium")
        self.assertIn("Nutrition data was partially extracted from OCR.", result["confidence_reasons"])
        self.assertTrue(any("rescued from OCR" in item for item in result["confidence_reasons"]))

    async def test_limited_estimate_returns_low_confidence_with_missing_core_reason(self) -> None:
        result = await ss.analyze_manual_product(
            {
                "name": "Mystery Snack",
                "brand": "Test",
                "unit": "g",
                "ingredients_text": "corn, salt",
                "protein_g": 2.0,
            },
            lang="en",
        )

        self.assertEqual(result["analysis_confidence"], "low")
        self.assertIn("The result is based on a limited estimate.", result["confidence_reasons"])
        self.assertTrue(any("core nutrition fields" in item for item in result["confidence_reasons"]))

    async def test_confidence_reasons_are_localized(self) -> None:
        payload = {
            "name": "Manual Cereal",
            "brand": "Test",
            "unit": "g",
            "ingredients_text": "oats, cocoa, sugar",
            "energy_kcal": 390,
            "sugar_g": 14,
            "salt_g": 0.4,
            "sat_fat_g": 1.2,
            "protein_g": 9,
            "serving_size": 40,
        }
        expected = {
            "de": "Die Nährwerte wurden manuell eingegeben.",
            "fr": "Les valeurs nutritionnelles ont été saisies manuellement.",
            "el": "Τα διατροφικά στοιχεία δόθηκαν χειροκίνητα.",
        }

        for lang, expected_reason in expected.items():
            result = await ss.analyze_manual_product(payload, lang=lang)
            self.assertIn(expected_reason, result["confidence_reasons"])


if __name__ == "__main__":
    unittest.main()
