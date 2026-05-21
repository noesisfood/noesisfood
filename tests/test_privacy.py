import unittest

from fastapi.testclient import TestClient

from app.main import app


class PrivacyPageTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = TestClient(app)

    def test_privacy_route_returns_expected_page(self) -> None:
        response = self.client.get("/privacy")
        self.assertEqual(response.status_code, 200)
        self.assertIn("text/html", response.headers.get("content-type", ""))
        content = response.text
        self.assertIn("NoesisFood", content)
        self.assertIn("Privacy Policy", content)
        self.assertIn("p.flemetakis@web.de", content)
        self.assertIn("not replace medical advice", content)
        self.assertIn("Photos and scan data", content)
        self.assertIn("Feedback and corrections", content)
        self.assertIn("Children", content)
        self.assertIn("Changes to this policy", content)
        self.assertIn("/data-deletion", content)


if __name__ == "__main__":
    unittest.main()
