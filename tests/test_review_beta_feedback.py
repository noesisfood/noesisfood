import importlib.util
import json
import unittest
from pathlib import Path


def _load_script_module():
    path = Path("scripts/review_beta_feedback.py")
    spec = importlib.util.spec_from_file_location("review_beta_feedback", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class ReviewBetaFeedbackTests(unittest.TestCase):
    def _path(self, name: str) -> Path:
        root = Path("data") / "test_feedback"
        root.mkdir(parents=True, exist_ok=True)
        return root / name

    def test_summary_groups_products_fields_and_invalid_rows(self) -> None:
        module = _load_script_module()
        path = self._path("beta_review.jsonl")
        path.unlink(missing_ok=True)
        record_a = {
            "product": {"name": "Crunchy Cereal", "brand": "Test", "barcode": "4000000000001", "key": "4000000000001"},
            "timestamp": "2026-05-12T08:00:00Z",
            "original_nutrition_per_100": {"unit": "g", "sugar_g": 14, "salt_g": 0.4},
            "corrected_nutrition_per_100": {"unit": "g", "sugar_g": 6, "salt_g": 0.1},
            "corrected_in_session": True,
            "changed_fields": ["sugar_g", "salt_g"],
        }
        record_b = {
            "product": {"name": "Crunchy Cereal", "brand": "Test", "barcode": "4000000000001", "key": "4000000000001"},
            "timestamp": "2026-05-12T09:00:00Z",
            "original_nutrition_per_100": {"unit": "g", "fat_g": 12},
            "corrected_nutrition_per_100": {"unit": "g", "fat_g": 8},
            "corrected_in_session": True,
            "changed_fields": ["fat_g"],
        }
        record_c = {
            "product": {"name": "Mystery Product"},
            "timestamp": "2026-05-12T10:00:00Z",
            "original_nutrition_per_100": {"unit": "g"},
            "corrected_nutrition_per_100": {"unit": "ml"},
            "corrected_in_session": False,
        }
        with path.open("w", encoding="utf-8") as handle:
            handle.write(json.dumps(record_a) + "\n")
            handle.write("{broken json\n")
            handle.write(json.dumps(record_b) + "\n")
            handle.write(json.dumps(record_c) + "\n")

        records, invalid = module.load_feedback_records(path)
        summary = module.summarize_feedback(records, invalid)
        rendered = module.format_summary(summary, limit=5)

        self.assertEqual(summary["total_feedback_submissions"], 3)
        self.assertEqual(summary["submissions_by_product"]["4000000000001 (Crunchy Cereal)"], 2)
        self.assertEqual(summary["most_frequently_corrected_fields"][0]["field"], "sugar_g")
        self.assertEqual(summary["repeated_product_reports"][0]["product"], "4000000000001 (Crunchy Cereal)")
        self.assertTrue(any(row["reason"] == "invalid_json" for row in summary["invalid_or_noisy_records"]))
        self.assertTrue(any("not_corrected_session" in row["reason"] for row in summary["invalid_or_noisy_records"]))
        self.assertIn("Total feedback submissions: 3", rendered)
        self.assertIn("sugar_g", rendered)

        path.unlink(missing_ok=True)


if __name__ == "__main__":
    unittest.main()
