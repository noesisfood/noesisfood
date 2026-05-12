import json
import os
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

from app.main import app
from app.services.internal_beta_review_service import get_feedback_summary
from app.services.monitoring_service import get_beta_monitoring_summary, log_event, reset_event_counters


class InternalBetaReviewTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = TestClient(app)
        reset_event_counters()

    def _store_path(self, name: str) -> Path:
        root = Path("data") / "test_feedback"
        root.mkdir(parents=True, exist_ok=True)
        return root / name

    def _record(self, idx: int, barcode: str = "4000000000001", name: str = "Crunchy Cereal", changed_fields=None):
        return {
            "product": {
                "name": name,
                "brand": "Test",
                "barcode": barcode,
                "key": barcode,
            },
            "source_type": "barcode",
            "lang": "en",
            "timestamp": f"2026-05-12T08:{idx:02d}:00Z",
            "original_nutrition_per_100": {"unit": "g", "sugar_g": 14, "salt_g": 0.4},
            "corrected_nutrition_per_100": {"unit": "g", "sugar_g": 6, "salt_g": 0.1},
            "analysis_confidence": "medium",
            "confidence_reasons": ["Core nutrition fields were available."],
            "corrected_in_session": True,
            "changed_fields": changed_fields or ["sugar_g", "salt_g"],
        }

    def test_feedback_summary_counts_recent_limit_and_repeated_products(self) -> None:
        store_path = self._store_path("internal_review_summary.jsonl")
        store_path.unlink(missing_ok=True)
        with store_path.open("w", encoding="utf-8") as handle:
            for idx in range(12):
                record = self._record(idx, barcode="4000000000001" if idx < 4 else f"50000000000{idx:02d}")
                if idx == 11:
                    record["photo_blob"] = "data:image/jpeg;base64,AAAA"
                    record["raw_ocr_text"] = "sugar salt fat"
                    record["user_id"] = "user-123"
                    record["ip_address"] = "127.0.0.1"
                handle.write(json.dumps(record) + "\n")
            handle.write("{broken json\n")
            noisy = self._record(13, barcode="", name="Mystery", changed_fields=[])
            noisy["corrected_in_session"] = False
            handle.write(json.dumps(noisy) + "\n")

        summary = get_feedback_summary(store_path=store_path, recent_limit=10)

        self.assertEqual(summary["total_feedback_submissions"], 13)
        self.assertGreaterEqual(summary["invalid_or_noisy_record_count"], 2)
        self.assertEqual(len(summary["recent_feedback"]), 10)
        self.assertEqual(summary["submissions_by_product"][0]["product_key"], "4000000000001")
        self.assertEqual(summary["submissions_by_product"][0]["count"], 4)
        self.assertEqual(summary["most_frequently_corrected_fields"][0]["field"], "sugar_g")
        self.assertEqual(summary["repeated_product_reports"][0]["product_key"], "4000000000001")
        rendered = json.dumps(summary)
        self.assertNotIn("raw_ocr_text", rendered)
        self.assertNotIn("photo_blob", rendered)
        self.assertNotIn("user_id", rendered)
        self.assertNotIn("ip_address", rendered)
        self.assertNotIn("original_nutrition_per_100", rendered)
        self.assertNotIn("corrected_nutrition_per_100", rendered)
        store_path.unlink(missing_ok=True)

    def test_monitoring_summary_maps_expected_counters(self) -> None:
        logger_name = "noesisfood.test.internal_beta_review"
        import logging
        logger = logging.getLogger(logger_name)
        log_event(logger, "scan_started")
        log_event(logger, "scan_started")
        log_event(logger, "scan_completed")
        log_event(logger, "scan_failed")
        log_event(logger, "feedback_submission_completed")
        log_event(logger, "feedback_submission_failed")
        log_event(logger, "correction_submitted")

        summary = get_beta_monitoring_summary()

        self.assertEqual(summary["monitoring_window"], "current_process_lifetime")
        self.assertEqual(summary["scan_started"], 2)
        self.assertEqual(summary["scan_completed"], 1)
        self.assertEqual(summary["scan_failed"], 1)
        self.assertEqual(summary["feedback_submitted"], 1)
        self.assertEqual(summary["feedback_failed"], 1)
        self.assertEqual(summary["correction_submitted"], 1)

    def test_internal_endpoint_rejects_when_token_missing(self) -> None:
        with patch.dict(os.environ, {}, clear=False):
            if "BETA_REVIEW_TOKEN" in os.environ:
                del os.environ["BETA_REVIEW_TOKEN"]
            response = self.client.get("/internal/beta/feedback-summary")
        self.assertEqual(response.status_code, 404)

    def test_internal_endpoint_rejects_wrong_token(self) -> None:
        with patch.dict(os.environ, {"BETA_REVIEW_TOKEN": "secret-token"}, clear=False):
            response = self.client.get("/internal/beta/feedback-summary", headers={"X-Beta-Review-Token": "wrong-token"})
        self.assertEqual(response.status_code, 403)

    def test_internal_endpoint_returns_summary_with_correct_token(self) -> None:
        store_path = self._store_path("internal_review_route.jsonl")
        store_path.unlink(missing_ok=True)
        with store_path.open("w", encoding="utf-8") as handle:
            handle.write(json.dumps(self._record(1)) + "\n")
        with patch("app.services.internal_beta_review_service.FEEDBACK_STORE_PATH", store_path):
            with patch.dict(os.environ, {"BETA_REVIEW_TOKEN": "secret-token"}, clear=False):
                response = self.client.get(
                    "/internal/beta/feedback-summary",
                    headers={"X-Beta-Review-Token": "secret-token"},
                )
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertIn("feedback", body)
        self.assertIn("monitoring", body)
        self.assertEqual(body["feedback"]["total_feedback_submissions"], 1)
        self.assertEqual(body["monitoring"]["monitoring_window"], "current_process_lifetime")
        store_path.unlink(missing_ok=True)


if __name__ == "__main__":
    unittest.main()
