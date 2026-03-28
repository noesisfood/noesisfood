import unittest
from unittest.mock import AsyncMock, patch

from app.services import scanner_service as ss


class PhotoFallbackCompositionTests(unittest.IsolatedAsyncioTestCase):
    def test_normalize_photo_payload_applies_composition_table_water_fallback(self) -> None:
        parsed = {
            "product_name": "NatÃ¼rliches Mineralwasser mit KohlensÃ¤ure (medium)",
            "brand": "K-Classic",
            "ingredients_text": None,
            "categories": [],
            "nutrition_per_100": {"unit": None, "energy_kcal": None, "sugar_g": None, "salt_g": None, "sat_fat_g": None, "protein_g": None},
            "confidence": "medium",
            "extracted_fields": ["product_name", "brand"],
            "notes": "Mineral composition table visible.",
            "label_kind": "composition_table",
            "composition_table_text": "Calcium Magnesium Hydrogencarbonat",
        }
        payload = {
            "existing_product": {
                "name": "NatÃ¼rliches Mineralwasser mit KohlensÃ¤ure (medium)",
                "brand": "K-Classic",
                "categories": ["NatÃ¼rliches Mineralwasser", "NatÃ¼rliches Mineralwasser mit wenig KohlensÃ¤ure versetzt"],
            },
            "existing_analysis": {
                "nutrition_per_100": {"unit": "ml"},
                "meta": {"serving": {"unit": "ml"}},
            },
        }

        normalized = ss._normalize_photo_extracted_payload(parsed, payload)

        self.assertEqual(normalized["product_name"], "NatÃ¼rliches Mineralwasser mit KohlensÃ¤ure (medium)")
        self.assertEqual(normalized["ingredients_text"], "NatÃ¼rliches Mineralwasser mit KohlensÃ¤ure (medium)")
        self.assertEqual(normalized["label_kind"], "composition_table")
        self.assertEqual(normalized["nutrition_per_100"]["unit"], "ml")
        self.assertEqual(normalized["nutrition_per_100"]["energy_kcal"], 0.0)
        self.assertEqual(normalized["nutrition_per_100"]["sugar_g"], 0.0)
        self.assertEqual(normalized["nutrition_per_100"]["salt_g"], 0.0)
        self.assertIn("composition_table", normalized["extracted_fields"])
        self.assertIn("nutrition_per_100", normalized["extracted_fields"])

    def test_build_photo_context_water_fallback_recovers_from_existing_product_context(self) -> None:
        payload = {
            "ingredient_image_data_url": "data:image/jpeg;base64,AAA",
            "existing_product": {
                "name": "NatÃ¼rliches Mineralwasser mit KohlensÃ¤ure (medium)",
                "brand": "K-Classic",
                "categories": ["NatÃ¼rliches Mineralwasser", "NatÃ¼rliches Mineralwasser mit wenig KohlensÃ¤ure versetzt"],
            },
            "existing_analysis": {
                "nutrition_per_100": {"unit": "ml"},
                "meta": {"serving": {"unit": "ml"}},
            },
        }

        normalized = ss._build_photo_context_water_fallback(payload)

        self.assertIsInstance(normalized, dict)
        self.assertEqual(normalized["product_name"], "NatÃ¼rliches Mineralwasser mit KohlensÃ¤ure (medium)")
        self.assertEqual(normalized["ingredients_text"], "NatÃ¼rliches Mineralwasser mit KohlensÃ¤ure (medium)")
        self.assertEqual(normalized["label_kind"], "composition_table")
        self.assertEqual(normalized["nutrition_per_100"]["unit"], "ml")
        self.assertEqual(normalized["nutrition_per_100"]["energy_kcal"], 0.0)
        self.assertEqual(normalized["nutrition_per_100"]["sugar_g"], 0.0)
        self.assertIn("composition_table_context", normalized["extracted_fields"])

    def test_build_photo_context_water_fallback_skips_non_water_products(self) -> None:
        payload = {
            "ingredient_image_data_url": "data:image/jpeg;base64,AAA",
            "existing_product": {
                "name": "Apfelschorle",
                "brand": "K-Classic",
                "categories": ["Soft drink"],
            },
        }

        normalized = ss._build_photo_context_water_fallback(payload)

        self.assertIsNone(normalized)

    def test_build_nutrition_photo_rescue_payload_salvages_malformed_table_text(self) -> None:
        payload = {
            "nutrition_image_data_url": "data:image/jpeg;base64,AAA",
            "nutrition_crop_applied": True,
            "existing_key": "4000000000001",
            "existing_product": {
                "name": "Feta",
                "brand": "Noesis",
                "categories": ["Feta", "Cheese"],
            },
            "existing_analysis": {
                "key": "4000000000001",
                "nutrition_per_100": {"unit": "g"},
                "meta": {"serving": {"unit": "g"}},
            },
        }

        rescued = ss._build_nutrition_photo_rescue_payload(
            payload,
            "Per 100 g Energy 265 kcal Fat 21 g Saturates 14 g Carbohydrate 1.0 g Sugars 1.0 g Protein 17 g Salt 2.5 g",
        )

        self.assertIsInstance(rescued, dict)
        self.assertEqual(rescued["label_kind"], "nutrition")
        self.assertEqual(rescued["product_name"], "Feta")
        self.assertEqual(rescued["nutrition_per_100"]["unit"], "g")
        self.assertEqual(rescued["nutrition_per_100"]["energy_kcal"], 265.0)
        self.assertEqual(rescued["nutrition_per_100"]["sugar_g"], 1.0)
        self.assertEqual(rescued["nutrition_per_100"]["salt_g"], 2.5)
        self.assertEqual(rescued["nutrition_per_100"]["sat_fat_g"], 14.0)
        self.assertEqual(rescued["nutrition_per_100"]["protein_g"], 17.0)
        self.assertIn("nutrition_photo_context", rescued["extracted_fields"])

    def test_build_nutrition_photo_rescue_payload_normalizes_noisy_ocr_text(self) -> None:
        payload = {
            "nutrition_image_data_url": "data:image/jpeg;base64,AAA",
            "nutrition_crop_applied": True,
            "existing_key": "4000000000001",
            "existing_product": {
                "name": "Feta",
                "brand": "Noesis",
                "categories": ["Feta", "Cheese"],
            },
            "existing_analysis": {
                "key": "4000000000001",
                "nutrition_per_100": {"unit": "g"},
                "meta": {"serving": {"unit": "g"}},
            },
        }

        rescued = ss._build_nutrition_photo_rescue_payload(
            payload,
            "Per100g | Energy265kcai | Satur ates14g | Sugers1,0g | Protei n17g | Sa1t2,5g",
        )

        self.assertIsInstance(rescued, dict)
        self.assertEqual(rescued["nutrition_per_100"]["energy_kcal"], 265.0)
        self.assertEqual(rescued["nutrition_per_100"]["sugar_g"], 1.0)
        self.assertEqual(rescued["nutrition_per_100"]["salt_g"], 2.5)
        self.assertEqual(rescued["nutrition_per_100"]["sat_fat_g"], 14.0)
        self.assertEqual(rescued["nutrition_per_100"]["protein_g"], 17.0)

    def test_evaluate_nutrition_photo_rescue_accepts_multilingual_partial_table(self) -> None:
        payload = {
            "nutrition_image_data_url": "data:image/jpeg;base64,AAA",
            "nutrition_crop_applied": True,
            "existing_key": "4000000000001",
            "existing_product": {
                "name": "Feta",
                "brand": "Noesis",
                "categories": ["Feta", "Cheese"],
            },
            "existing_analysis": {
                "key": "4000000000001",
                "nutrition_per_100": {"unit": "g"},
                "meta": {"serving": {"unit": "g"}},
            },
        }

        rescued, debug = ss._evaluate_nutrition_photo_rescue(
            payload,
            "Per 100 g Energie 265 kcal Fett 21 g Kohlenhydrate 1,0 g Eiweiss 17 g Salz 2,5 g",
        )

        self.assertIsInstance(rescued, dict)
        self.assertEqual(debug["parser_acceptance_reason"], "accepted")
        self.assertEqual(debug["rescued_field_count"], 5)
        self.assertEqual(rescued["nutrition_per_100"]["energy_kcal"], 265.0)
        self.assertEqual(rescued["nutrition_per_100"]["fat_g"], 21.0)
        self.assertEqual(rescued["nutrition_per_100"]["carb_g"], 1.0)
        self.assertEqual(rescued["nutrition_per_100"]["protein_g"], 17.0)
        self.assertEqual(rescued["nutrition_per_100"]["salt_g"], 2.5)

    def test_evaluate_nutrition_photo_rescue_accepts_two_field_partial_table(self) -> None:
        payload = {
            "nutrition_image_data_url": "data:image/jpeg;base64,AAA",
            "nutrition_crop_applied": True,
            "existing_key": "4000000000001",
            "existing_product": {
                "name": "Feta",
                "brand": "Noesis",
                "categories": ["Feta", "Cheese"],
            },
            "existing_analysis": {
                "key": "4000000000001",
                "nutrition_per_100": {"unit": "g"},
                "meta": {"serving": {"unit": "g"}},
            },
        }

        rescued, debug = ss._evaluate_nutrition_photo_rescue(
            payload,
            "Per 100 g Energy 265 kcal Protein 17 g",
        )

        self.assertIsInstance(rescued, dict)
        self.assertEqual(debug["parser_acceptance_reason"], "accepted")
        self.assertEqual(debug["rescued_field_count"], 2)
        self.assertIn("265 kcal", debug["ocr_text_preview"])
        self.assertEqual(rescued["nutrition_per_100"]["energy_kcal"], 265.0)
        self.assertEqual(rescued["nutrition_per_100"]["protein_g"], 17.0)

    def test_extract_text_from_rapidocr_result_accepts_tuple_txts(self) -> None:
        class FakeRapidOCROutput:
            txts = ("Per 100 g", "Energy 265 kcal", "Salt 2.5 g")

        text = ss._extract_text_from_rapidocr_result(FakeRapidOCROutput())

        self.assertEqual(text, "Per 100 g\nEnergy 265 kcal\nSalt 2.5 g")

    async def test_extract_nutrition_photo_text_locally_uses_retry_only_after_failed_first_pass(self) -> None:
        payload = {
            "nutrition_image_data_url": "data:image/jpeg;base64,AAA",
            "nutrition_crop_applied": True,
            "existing_key": "4000000000001",
            "existing_analysis": {
                "key": "4000000000001",
                "nutrition_per_100": {"unit": "g"},
                "meta": {"serving": {"unit": "g"}},
            },
            "existing_product": {
                "name": "Feta",
                "brand": "Noesis",
                "categories": ["Feta", "Cheese"],
            },
        }

        with (
            patch.dict("os.environ", {"ENABLE_LOCAL_NUTRITION_OCR_FALLBACK": "1"}),
            patch.object(ss, "_decode_image_data_url", return_value=b"image-bytes"),
            patch.object(
                ss,
                "_preprocess_nutrition_image",
                return_value=("base-variant", {
                    "original_image_size": [800, 1200],
                    "processed_image_size": [1400, 2100],
                    "thresholding_applied": True,
                    "preprocess_variant": "base",
                }),
            ),
            patch.object(
                ss,
                "_preprocess_nutrition_retry_image",
                return_value=("retry-variant", {
                    "original_image_size": [800, 1200],
                    "processed_image_size": [1400, 2100],
                    "thresholding_applied": True,
                    "preprocess_variant": "retry_strong_threshold",
                }),
            ),
            patch.object(ss, "_get_local_nutrition_ocr_engine", return_value=object()),
            patch.object(
                ss,
                "_evaluate_nutrition_photo_rescue",
                side_effect=[
                    (None, {"parser_acceptance_reason": "insufficient_rescued_fields"}),
                    ({"nutrition_per_100": {"energy_kcal": 265.0, "salt_g": 2.5}}, {"parser_acceptance_reason": "accepted"}),
                ],
            ) as rescue_mock,
            patch.object(
                ss,
                "_extract_text_from_rapidocr_result",
                side_effect=["Energy 265 kcal", "Energy 265 kcal Salt 2.5 g"],
            ),
        ):
            calls = []

            def fake_engine(variant, **kwargs):
                calls.append((variant, kwargs))
                return object()

            with patch.object(ss, "_get_local_nutrition_ocr_engine", return_value=fake_engine):
                text = await ss._extract_nutrition_photo_text_locally(payload)

        self.assertEqual(text, "Energy 265 kcal Salt 2.5 g")
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[0][0], "base-variant")
        self.assertEqual(calls[1][0], "retry-variant")
        self.assertEqual(calls[0][1]["use_cls"], False)
        self.assertEqual(calls[1][1]["use_cls"], False)
        self.assertEqual(rescue_mock.call_count, 2)

    async def test_extract_nutrition_photo_text_locally_reports_engine_unavailable(self) -> None:
        payload = {"nutrition_image_data_url": "data:image/jpeg;base64,AAA"}

        with (
            patch.object(ss, "_decode_image_data_url", return_value=b"image-bytes"),
            patch.object(ss, "_get_local_nutrition_ocr_engine", return_value=None),
        ):
            text, debug = await ss._extract_nutrition_photo_text_locally_with_debug(payload)

        self.assertEqual(text, "")
        self.assertEqual(debug["local_ocr_enabled"], True)
        self.assertEqual(debug["local_ocr_engine_available"], False)
        self.assertEqual(debug["local_ocr_status"], "engine_unavailable")

    def test_preprocess_nutrition_image_reports_debug_metadata(self) -> None:
        if ss.Image is None:
            self.skipTest("Pillow not available")
        img = ss.Image.new("RGB", (1000, 600), "white")
        from io import BytesIO

        buf = BytesIO()
        img.save(buf, format="JPEG", quality=90)
        result = ss._preprocess_nutrition_image(buf.getvalue())

        self.assertIsNotNone(result)
        processed, debug = result
        self.assertEqual(debug["original_image_size"], [1000, 600])
        self.assertEqual(debug["processed_image_size"], [1750, 1050])
        self.assertEqual(debug["thresholding_applied"], True)
        self.assertEqual(processed.size, (1750, 1050))

    async def test_analyze_photo_product_resolves_mineral_water_composition_case(self) -> None:
        extracted = {
            "product_name": "NatÃ¼rliches Mineralwasser mit KohlensÃ¤ure (medium)",
            "brand": "K-Classic",
            "ingredients_text": "NatÃ¼rliches Mineralwasser mit KohlensÃ¤ure (medium)",
            "categories": ["NatÃ¼rliches Mineralwasser", "NatÃ¼rliches Mineralwasser mit wenig KohlensÃ¤ure versetzt"],
            "nutrition_per_100": {
                "unit": "ml",
                "energy_kcal": 0.0,
                "sugar_g": 0.0,
                "salt_g": 0.0,
                "sat_fat_g": 0.0,
                "protein_g": 0.0,
            },
            "confidence": "medium",
            "extracted_fields": ["product_name", "brand", "ingredients_text", "nutrition_per_100", "composition_table"],
            "notes": "Composition-table water fallback applied with conservative zero nutrition for plain mineral water.",
            "label_kind": "composition_table",
        }
        payload = {
            "ingredient_image_data_url": "data:image/jpeg;base64,AAA",
            "existing_key": "4337185151651",
            "existing_analysis": {
                "key": "4337185151651",
                "source": "openfoodfacts",
                "product": {"barcode": "4337185151651"},
                "meta": {"serving": {"unit": "ml"}},
            },
            "existing_product": {
                "name": "NatÃ¼rliches Mineralwasser mit KohlensÃ¤ure (medium)",
                "brand": "K-Classic",
                "categories": ["NatÃ¼rliches Mineralwasser", "NatÃ¼rliches Mineralwasser mit wenig KohlensÃ¤ure versetzt"],
                "image_url": "",
                "quantity": "",
            },
        }

        with (
            patch.object(ss, "_extract_photo_payload_with_ai", AsyncMock(return_value=extracted)),
            patch.object(ss, "_persist_product_enrichment", return_value={}),
        ):
            result = await ss.analyze_photo_product(payload, lang="en")

        self.assertFalse(result.get("error"))
        self.assertEqual(result["product"]["name"], "NatÃ¼rliches Mineralwasser mit KohlensÃ¤ure (medium)")
        self.assertEqual(result["analysis_state"], "partial_analysis")
        self.assertEqual(result["lookup_state"], "found_but_incomplete")
        self.assertEqual(result["source"], "openfoodfacts")
        self.assertEqual(result["matched_by"], "photo_enrichment")

    async def test_analyze_photo_product_uses_context_fallback_when_ai_extraction_fails(self) -> None:
        extracted_error = {
            "error": {"code": "PHOTO_EXTRACTION_FAILED", "message": "Could not extract enough data from the photo."},
            "status": 422,
            "lookup_state": "found_but_incomplete",
            "analysis_state": "insufficient_data",
            "analysis_confidence": "low",
        }
        payload = {
            "ingredient_image_data_url": "data:image/jpeg;base64,AAA",
            "existing_key": "4337185151651",
            "existing_analysis": {
                "key": "4337185151651",
                "source": "openfoodfacts",
                "product": {"barcode": "4337185151651"},
                "nutrition_per_100": {"unit": "ml"},
                "meta": {"serving": {"unit": "ml"}},
            },
            "existing_product": {
                "name": "NatÃ¼rliches Mineralwasser mit KohlensÃ¤ure (medium)",
                "brand": "K-Classic",
                "categories": ["NatÃ¼rliches Mineralwasser", "NatÃ¼rliches Mineralwasser mit wenig KohlensÃ¤ure versetzt"],
                "image_url": "",
                "quantity": "",
            },
        }

        with (
            patch.object(ss, "_extract_photo_payload_with_ai", AsyncMock(return_value=extracted_error)),
            patch.object(ss, "_persist_product_enrichment", return_value={}),
        ):
            result = await ss.analyze_photo_product(payload, lang="en")

        self.assertFalse(result.get("error"))
        self.assertEqual(result["product"]["name"], "NatÃ¼rliches Mineralwasser mit KohlensÃ¤ure (medium)")
        self.assertEqual(result["analysis_state"], "partial_analysis")
        self.assertEqual(result["lookup_state"], "found_but_incomplete")
        self.assertEqual(result["source"], "openfoodfacts")
        self.assertEqual(result["matched_by"], "photo_enrichment")
        self.assertEqual(result["photo_extraction"]["confidence"], "low")
        self.assertIn("composition_table_context", result["photo_extraction"]["extracted_fields"])

    async def test_analyze_photo_product_accepts_nutrition_only_photo_payload(self) -> None:
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
                "meta": {"serving": {"unit": "g"}},
            },
            "existing_product": {
                "name": "Feta",
                "brand": "Noesis",
                "categories": ["Feta", "Cheese"],
                "image_url": "",
                "quantity": "",
            },
        }

        with (
            patch.object(ss, "_extract_photo_payload_with_ai", AsyncMock(return_value=extracted)),
            patch.object(ss, "_persist_product_enrichment", return_value={}),
        ):
            result = await ss.analyze_photo_product(payload, lang="en")

        self.assertFalse(result.get("error"))
        self.assertEqual(result["product"]["name"], "Feta")
        self.assertEqual(result["source"], "openfoodfacts")
        self.assertEqual(result["matched_by"], "photo_enrichment")
        self.assertEqual(result["nutrition_per_100"]["energy_kcal"], 265.0)
        self.assertEqual(result["nutrition_per_100"]["sugar_g"], 1.0)
        self.assertEqual(result["nutrition_per_100"]["salt_g"], 2.5)
        self.assertEqual(result["nutrition_per_100"]["sat_fat_g"], 14.0)
        self.assertEqual(result["nutrition_per_100"]["protein_g"], 17.0)
        self.assertEqual(result["photo_extraction"]["used_nutrition_photo"], True)

    async def test_analyze_photo_product_uses_nutrition_rescue_when_extractor_returns_error(self) -> None:
        extracted_error = {
            "error": {"code": "PHOTO_EXTRACTION_FAILED", "message": "Could not extract enough data from the photo."},
            "status": 422,
            "lookup_state": "found_but_incomplete",
            "analysis_state": "insufficient_data",
            "analysis_confidence": "low",
        }
        payload = {
            "nutrition_image_data_url": "data:image/jpeg;base64,AAA",
            "existing_key": "4000000000001",
            "existing_analysis": {
                "key": "4000000000001",
                "source": "openfoodfacts",
                "product": {"barcode": "4000000000001"},
                "nutrition_per_100": {"unit": "g"},
                "meta": {"serving": {"unit": "g"}},
            },
            "existing_product": {
                "name": "Feta",
                "brand": "Noesis",
                "categories": ["Feta", "Cheese"],
                "image_url": "",
                "quantity": "",
            },
        }

        with (
            patch.object(ss, "_extract_photo_payload_with_ai", AsyncMock(return_value=extracted_error)),
            patch.object(
                ss,
                "_evaluate_nutrition_photo_rescue",
                return_value=(
                    {
                        "product_name": "Feta",
                        "brand": "Noesis",
                        "ingredients_text": None,
                        "categories": ["Feta", "Cheese"],
                        "nutrition_per_100": {
                            "unit": "g",
                            "energy_kcal": 265.0,
                            "sugar_g": 1.0,
                            "salt_g": 2.5,
                            "sat_fat_g": 14.0,
                            "protein_g": 17.0,
                        },
                        "confidence": "low",
                        "extracted_fields": ["nutrition_per_100", "nutrition_photo_context", "energy_kcal", "sugar_g", "salt_g", "sat_fat_g", "protein_g"],
                        "notes": "Nutrition-photo fallback accepted with nutrition-only enrichment for an existing partial product.",
                        "label_kind": "nutrition",
                    },
                    {"rescued_field_count": 5, "rescued_field_names": ["energy_kcal", "sugar_g", "salt_g", "sat_fat_g", "protein_g"], "parser_acceptance_reason": "accepted"},
                ),
            ),
            patch.object(ss, "_persist_product_enrichment", return_value={}),
        ):
            result = await ss.analyze_photo_product(payload, lang="en")

        self.assertFalse(result.get("error"))
        self.assertEqual(result["product"]["name"], "Feta")
        self.assertEqual(result["source"], "openfoodfacts")
        self.assertEqual(result["matched_by"], "photo_enrichment")
        self.assertEqual(result["nutrition_per_100"]["energy_kcal"], 265.0)
        self.assertEqual(result["nutrition_per_100"]["salt_g"], 2.5)
        self.assertIn("nutrition_photo_context", result["photo_extraction"]["extracted_fields"])

    async def test_analyze_photo_product_uses_ocr_backed_nutrition_rescue_when_extractor_errors(self) -> None:
        extracted_error = {
            "error": {"code": "PHOTO_EXTRACTION_FAILED", "message": "Could not extract enough data from the photo."},
            "status": 422,
            "lookup_state": "found_but_incomplete",
            "analysis_state": "insufficient_data",
            "analysis_confidence": "low",
        }
        payload = {
            "nutrition_image_data_url": "data:image/jpeg;base64,AAA",
            "existing_key": "4000000000001",
            "existing_analysis": {
                "key": "4000000000001",
                "source": "openfoodfacts",
                "product": {"barcode": "4000000000001"},
                "nutrition_per_100": {"unit": "g"},
                "meta": {"serving": {"unit": "g"}},
            },
            "existing_product": {
                "name": "Feta",
                "brand": "Noesis",
                "categories": ["Feta", "Cheese"],
                "image_url": "",
                "quantity": "",
            },
        }

        with (
            patch.object(ss, "_extract_photo_payload_with_ai", AsyncMock(return_value=extracted_error)),
            patch.object(
                ss,
                "_extract_nutrition_photo_text_locally_with_debug",
                AsyncMock(return_value=("Per 100 g Energy 265 kcal Saturates 14 g Sugars 1.0 g Protein 17 g Salt 2.5 g", {
                    "local_ocr_enabled": True,
                    "local_ocr_engine_available": True,
                    "local_ocr_engine_name": "RapidOCR",
                    "local_ocr_retry_used": False,
                    "local_ocr_status": "success_base",
                    "first_pass_text_length": 78,
                    "second_pass_text_length": 0,
                })),
            ),
            patch.object(ss, "_persist_product_enrichment", return_value={}),
        ):
            result = await ss.analyze_photo_product(payload, lang="en")

        self.assertFalse(result.get("error"))
        self.assertEqual(result["product"]["name"], "Feta")
        self.assertEqual(result["source"], "openfoodfacts")
        self.assertEqual(result["matched_by"], "photo_enrichment")
        self.assertEqual(result["nutrition_per_100"]["energy_kcal"], 265.0)
        self.assertEqual(result["nutrition_per_100"]["sat_fat_g"], 14.0)
        self.assertEqual(result["nutrition_per_100"]["salt_g"], 2.5)
        self.assertIn("nutrition_photo_context", result["photo_extraction"]["extracted_fields"])
        self.assertEqual(result["photo_extraction"]["debug"]["ocr_helper_invoked"], True)
        self.assertEqual(result["photo_extraction"]["debug"]["ocr_text_non_empty"], True)
        self.assertGreaterEqual(int(result["photo_extraction"]["debug"]["rescued_field_count"]), 2)
        self.assertEqual(result["photo_extraction"]["debug"]["local_ocr_enabled"], True)

    async def test_analyze_photo_product_uses_local_ocr_after_upstream_unavailable(self) -> None:
        extracted_error = {
            "error": "Photo extraction is not available.",
            "error_code": "PHOTO_EXTRACTION_UNAVAILABLE",
            "status_code": 422,
            "lookup_state": "found_but_incomplete",
            "lookup_missing_fields": [],
            "analysis_state": "insufficient_data",
            "analysis_confidence": "low",
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
                "name": "Feta",
                "brand": "Noesis",
                "categories": ["Feta", "Cheese"],
                "image_url": "",
                "quantity": "",
            },
        }

        with (
            patch.object(ss, "_extract_photo_payload_with_ai", AsyncMock(return_value=extracted_error)),
            patch.object(
                ss,
                "_extract_nutrition_photo_text_locally_with_debug",
                AsyncMock(return_value=("Per 100 g Energy 265 kcal Saturates 14 g Sugars 1.0 g Protein 17 g Salt 2.5 g", {
                    "local_ocr_enabled": True,
                    "local_ocr_engine_available": True,
                    "local_ocr_engine_name": "RapidOCR",
                    "local_ocr_retry_used": False,
                    "local_ocr_status": "success_base",
                    "first_pass_text_length": 78,
                    "second_pass_text_length": 0,
                })),
            ),
            patch.object(ss, "_persist_product_enrichment", return_value={}),
        ):
            result = await ss.analyze_photo_product(payload, lang="en")

        self.assertFalse(result.get("error"))
        self.assertEqual(result["product"]["name"], "Feta")
        self.assertEqual(result["source"], "openfoodfacts")
        self.assertEqual(result["matched_by"], "photo_enrichment")
        self.assertEqual(result["nutrition_per_100"]["energy_kcal"], 265.0)
        self.assertEqual(result["nutrition_per_100"]["salt_g"], 2.5)
        self.assertEqual(result["photo_extraction"]["debug"]["ocr_helper_invoked"], True)
        self.assertEqual(result["photo_extraction"]["debug"]["ocr_text_non_empty"], True)
        self.assertEqual(result["photo_extraction"]["debug"]["local_ocr_enabled"], True)
        self.assertEqual(result["photo_extraction"]["debug"]["nutrition_crop_applied"], True)

    async def test_analyze_photo_product_returns_debug_on_failed_nutrition_rescue(self) -> None:
        extracted_error = {
            "error": {"code": "PHOTO_EXTRACTION_FAILED", "message": "Could not extract enough data from the photo."},
            "error_code": "PHOTO_EXTRACTION_FAILED",
            "status": 422,
            "lookup_state": "found_but_incomplete",
            "analysis_state": "insufficient_data",
            "analysis_confidence": "low",
        }
        payload = {
            "nutrition_image_data_url": "data:image/jpeg;base64,AAA",
            "existing_key": "4000000000001",
            "existing_analysis": {
                "key": "4000000000001",
                "source": "openfoodfacts",
                "product": {"barcode": "4000000000001"},
                "nutrition_per_100": {"unit": "g"},
                "meta": {"serving": {"unit": "g"}},
            },
            "existing_product": {
                "name": "Feta",
                "brand": "Noesis",
                "categories": ["Feta", "Cheese"],
                "image_url": "",
                "quantity": "",
            },
        }

        with (
            patch.object(ss, "_extract_photo_payload_with_ai", AsyncMock(return_value=extracted_error)),
            patch.object(
                ss,
                "_extract_nutrition_photo_text_locally_with_debug",
                AsyncMock(return_value=("", {
                    "local_ocr_enabled": True,
                    "local_ocr_engine_available": True,
                    "local_ocr_engine_name": "RapidOCR",
                    "local_ocr_retry_used": True,
                    "local_ocr_status": "no_text",
                    "first_pass_text_length": 0,
                    "second_pass_text_length": 0,
                })),
            ),
        ):
            result = await ss.analyze_photo_product(payload, lang="en")

        self.assertTrue(result.get("error"))
        self.assertEqual(result["error_code"], "PHOTO_EXTRACTION_FAILED")
        debug = result.get("photo_extraction_debug") or {}
        self.assertEqual(debug.get("nutrition_upload_present"), True)
        self.assertEqual(debug.get("ocr_helper_invoked"), True)
        self.assertEqual(debug.get("ocr_text_non_empty"), False)
        self.assertEqual(int(debug.get("ocr_text_length") or 0), 0)
        self.assertEqual(int(debug.get("rescued_field_count") or 0), 0)
        self.assertEqual(debug.get("final_error_branch"), "photo_extraction_failed")
        self.assertEqual(debug.get("local_ocr_enabled"), True)

    async def test_analyze_photo_product_reports_engine_unavailable_for_nutrition_rescue(self) -> None:
        extracted_error = {
            "error": "Photo extraction is not available.",
            "error_code": "PHOTO_EXTRACTION_UNAVAILABLE",
            "status_code": 422,
            "lookup_state": "found_but_incomplete",
            "lookup_missing_fields": [],
            "analysis_state": "insufficient_data",
            "analysis_confidence": "low",
        }
        payload = {
            "nutrition_image_data_url": "data:image/jpeg;base64,AAA",
            "existing_key": "4000000000001",
            "existing_analysis": {
                "key": "4000000000001",
                "source": "openfoodfacts",
                "product": {"barcode": "4000000000001"},
                "nutrition_per_100": {"unit": "g"},
                "meta": {"serving": {"unit": "g"}},
            },
            "existing_product": {
                "name": "Feta",
                "brand": "Noesis",
                "categories": ["Feta", "Cheese"],
            },
        }

        with (
            patch.object(ss, "_extract_photo_payload_with_ai", AsyncMock(return_value=extracted_error)),
            patch.object(
                ss,
                "_extract_nutrition_photo_text_locally_with_debug",
                AsyncMock(return_value=("", {
                    "local_ocr_enabled": True,
                    "local_ocr_engine_available": False,
                    "local_ocr_engine_name": "",
                    "local_ocr_retry_used": False,
                    "local_ocr_status": "engine_unavailable",
                    "first_pass_text_length": 0,
                    "second_pass_text_length": 0,
                })),
            ),
        ):
            result = await ss.analyze_photo_product(payload, lang="en")

        self.assertTrue(result.get("error"))
        self.assertEqual(result["error_code"], "PHOTO_EXTRACTION_UNAVAILABLE")
        debug = result.get("photo_extraction_debug") or {}
        self.assertEqual(debug.get("ocr_helper_invoked"), True)
        self.assertEqual(debug.get("ocr_text_non_empty"), False)
        self.assertEqual(debug.get("local_ocr_enabled"), True)
        self.assertEqual(debug.get("local_ocr_engine_available"), False)
        self.assertEqual(debug.get("local_ocr_status"), "engine_unavailable")

    async def test_analyze_photo_product_remaps_non_empty_ocr_rejection_to_extraction_failed(self) -> None:
        extracted_error = {
            "error": "Photo extraction is not available.",
            "error_code": "PHOTO_EXTRACTION_UNAVAILABLE",
            "status_code": 422,
            "lookup_state": "found_but_incomplete",
            "lookup_missing_fields": [],
            "analysis_state": "insufficient_data",
            "analysis_confidence": "low",
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
                "name": "Feta",
                "brand": "Noesis",
                "categories": ["Feta", "Cheese"],
            },
        }

        with (
            patch.object(ss, "_extract_photo_payload_with_ai", AsyncMock(return_value=extracted_error)),
            patch.object(
                ss,
                "_extract_nutrition_photo_text_locally_with_debug",
                AsyncMock(return_value=("Fett 21 g", {
                    "local_ocr_enabled": True,
                    "local_ocr_engine_available": True,
                    "local_ocr_engine_name": "RapidOCR",
                    "local_ocr_retry_used": False,
                    "local_ocr_status": "rescue_rejected",
                    "first_pass_text_length": 9,
                    "second_pass_text_length": 0,
                })),
            ),
        ):
            result = await ss.analyze_photo_product(payload, lang="en")

        self.assertTrue(result.get("error"))
        self.assertEqual(result["error_code"], "PHOTO_EXTRACTION_FAILED")
        debug = result.get("photo_extraction_debug") or {}
        self.assertEqual(debug.get("ocr_text_non_empty"), True)
        self.assertEqual(debug.get("parser_acceptance_reason"), "insufficient_rescued_fields")
        self.assertIn("fat_g", debug.get("rescued_field_names") or [])
        self.assertEqual(debug.get("ocr_text_preview"), "Fett 21 g")
        self.assertEqual(debug.get("final_error_branch"), "photo_extraction_failed")


if __name__ == "__main__":
    unittest.main()
