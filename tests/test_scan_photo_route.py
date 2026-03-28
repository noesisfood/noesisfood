import unittest
from unittest.mock import AsyncMock, patch

from app.api.routes import scan as scan_route


class ScanPhotoRouteTests(unittest.IsolatedAsyncioTestCase):
    def test_error_status_maps_photo_failures_away_from_422(self) -> None:
        self.assertEqual(scan_route._error_status({"error_code": "PHOTO_EXTRACTION_UNAVAILABLE"}), 503)
        self.assertEqual(scan_route._error_status({"error_code": "PHOTO_EXTRACTION_FAILED"}), 409)
        self.assertEqual(scan_route._error_status({"error_code": "PHOTO_PARSING_FAILED"}), 409)

    def test_photo_payload_summary_includes_crop_and_bytes_presence(self) -> None:
        payload = {
            "nutrition_image_data_url": "data:image/jpeg;base64,QUFB",
            "nutrition_crop_applied": True,
            "ingredient_image_data_url": "",
            "existing_key": "4000000000001",
        }

        summary = scan_route._photo_payload_summary(payload)

        self.assertEqual(summary["nutrition_upload_present"], True)
        self.assertEqual(summary["nutrition_crop_applied"], True)
        self.assertEqual(summary["nutrition_image_bytes_present"], True)
        self.assertEqual(summary["existing_key"], "4000000000001")

    async def test_scan_photo_endpoint_returns_503_for_extraction_unavailable(self) -> None:
        payload = {"nutrition_image_data_url": "data:image/jpeg;base64,QUFB", "nutrition_crop_applied": True}
        returned = {
            "error": "Photo extraction is not available.",
            "error_code": "PHOTO_EXTRACTION_UNAVAILABLE",
            "status_code": 422,
            "lookup_state": "found_but_incomplete",
            "analysis_state": "insufficient_data",
            "photo_extraction_debug": {"local_ocr_status": "engine_unavailable"},
        }

        with patch.object(scan_route, "analyze_photo_product", AsyncMock(return_value=returned)):
            response = await scan_route.scan_photo_endpoint(payload=payload, lang="el")

        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.body.decode("utf-8").count("PHOTO_EXTRACTION_UNAVAILABLE"), 1)

    async def test_scan_photo_endpoint_returns_409_for_extraction_failed(self) -> None:
        payload = {"nutrition_image_data_url": "data:image/jpeg;base64,QUFB", "nutrition_crop_applied": False}
        returned = {
            "error": {"code": "PHOTO_EXTRACTION_FAILED", "message": "Could not extract enough data from the photo."},
            "error_code": "PHOTO_EXTRACTION_FAILED",
            "status_code": 422,
            "lookup_state": "found_but_incomplete",
            "analysis_state": "insufficient_data",
            "photo_extraction_debug": {"local_ocr_status": "rescue_rejected"},
        }

        with patch.object(scan_route, "analyze_photo_product", AsyncMock(return_value=returned)):
            response = await scan_route.scan_photo_endpoint(payload=payload, lang="de")

        self.assertEqual(response.status_code, 409)
        self.assertEqual(response.body.decode("utf-8").count("PHOTO_EXTRACTION_FAILED"), 2)


if __name__ == "__main__":
    unittest.main()
