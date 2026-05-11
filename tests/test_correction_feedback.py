import json
import unittest
from pathlib import Path

from app.api.routes import scan as scan_route
from app.services.correction_feedback_service import submit_correction_feedback


def _payload(**overrides):
    base = {
        "product": {
            "name": "Crunchy Cereal",
            "brand": "Test",
            "barcode": "4000000000001",
            "key": "4000000000001",
        },
        "source_type": "barcode",
        "lang": "en",
        "original_nutrition_per_100": {
            "unit": "g",
            "energy_kcal": 390,
            "fat_g": 7,
            "carb_g": 68,
            "sugar_g": 14,
            "salt_g": 0.4,
            "sat_fat_g": 1.2,
            "protein_g": 9,
            "serving_size": 40,
        },
        "corrected_nutrition_per_100": {
            "unit": "g",
            "energy_kcal": 390,
            "fat_g": 7,
            "carb_g": 68,
            "sugar_g": 6,
            "salt_g": 0.1,
            "sat_fat_g": 1.2,
            "protein_g": 9,
            "serving_size": 40,
        },
        "analysis_confidence": "medium",
        "confidence_reasons": ["Core nutrition fields were available."],
        "corrected_in_session": True,
    }
    base.update(overrides)
    return base


class CorrectionFeedbackTests(unittest.TestCase):
    def _store_path(self, name: str) -> Path:
        root = Path("data") / "test_feedback"
        root.mkdir(parents=True, exist_ok=True)
        return root / name

    def test_backend_accepts_valid_changed_feedback_and_writes_jsonl(self) -> None:
        store_path = self._store_path("accepts_valid.jsonl")
        store_path.unlink(missing_ok=True)
        result = submit_correction_feedback(_payload(), lang="en", store_path=store_path)

        self.assertTrue(result["ok"])
        self.assertTrue(store_path.exists())
        lines = store_path.read_text(encoding="utf-8").strip().splitlines()
        self.assertEqual(len(lines), 1)
        record = json.loads(lines[0])
        self.assertEqual(record["product"]["barcode"], "4000000000001")
        self.assertEqual(record["corrected_nutrition_per_100"]["sugar_g"], 6.0)
        store_path.unlink(missing_ok=True)

    def test_backend_rejects_no_change_feedback(self) -> None:
        payload = _payload(corrected_nutrition_per_100=_payload()["original_nutrition_per_100"])
        store_path = self._store_path("rejects_no_change.jsonl")
        store_path.unlink(missing_ok=True)
        result = submit_correction_feedback(payload, lang="en", store_path=store_path)

        self.assertFalse(result["ok"])
        self.assertEqual(result["error_code"], "NO_CHANGED_FIELDS")

    def test_backend_rejects_unrealistic_values(self) -> None:
        payload = _payload(corrected_nutrition_per_100={**_payload()["corrected_nutrition_per_100"], "sugar_g": 150})
        store_path = self._store_path("rejects_unrealistic.jsonl")
        store_path.unlink(missing_ok=True)
        result = submit_correction_feedback(payload, lang="en", store_path=store_path)

        self.assertFalse(result["ok"])
        self.assertEqual(result["error_code"], "UNREALISTIC_VALUES")

    def test_backend_rejects_duplicate_submission_within_window(self) -> None:
        store_path = self._store_path("rejects_duplicate.jsonl")
        store_path.unlink(missing_ok=True)
        first = submit_correction_feedback(_payload(), lang="en", store_path=store_path)
        second = submit_correction_feedback(_payload(), lang="en", store_path=store_path)

        self.assertTrue(first["ok"])
        self.assertFalse(second["ok"])
        self.assertEqual(second["error_code"], "DUPLICATE_FEEDBACK")
        store_path.unlink(missing_ok=True)

    def test_route_error_status_uses_feedback_status_code(self) -> None:
        self.assertEqual(scan_route._error_status({"status_code": 409}), 409)

    def test_frontend_feedback_cta_and_strings_exist(self) -> None:
        content = Path("app/frontend/index.html").read_text(encoding="utf-8")

        self.assertIn('id="submitCorrectionFeedbackBtn"', content)
        self.assertIn('${d?.corrected_in_session ? `<button class="ghost" id="submitCorrectionFeedbackBtn">${t("submit_correction_feedback")}</button>` : ``}', content)
        self.assertIn("function buildCorrectionFeedbackPayload(currentData)", content)
        self.assertIn('submit_correction_feedback: "Submit correction feedback"', content)
        self.assertIn('submit_correction_feedback: "Υποβολή ανατροφοδότησης διόρθωσης"', content)
        self.assertIn('submit_correction_feedback: "Korrekturfeedback senden"', content)
        self.assertIn('submit_correction_feedback: "Envoyer le retour de correction"', content)
        self.assertIn('feedback_thanks: "Thank you for helping improve nutrition accuracy"', content)
        self.assertIn('beta_title: "Public Beta"', content)
        self.assertIn('beta_title: "Δημόσιο Beta"', content)
        self.assertIn('beta_title: "Öffentliche Beta"', content)
        self.assertIn('beta_title: "Bêta publique"', content)
        self.assertIn('err_photo_extract: "We could not read enough nutrition data. Try a sharper photo or crop closer to the nutrition table."', content)


if __name__ == "__main__":
    unittest.main()
