import unittest

from fastapi.testclient import TestClient

from app.main import app


class DataDeletionPageTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = TestClient(app)

    def test_data_deletion_route_returns_expected_page(self) -> None:
        response = self.client.get("/data-deletion")
        self.assertEqual(response.status_code, 200)
        self.assertIn("text/html", response.headers.get("content-type", ""))
        content = response.text
        self.assertIn("NoesisFood", content)
        self.assertIn("Data Deletion", content)
        self.assertIn("p.flemetakis@web.de", content)
        self.assertIn("does not use user accounts", content)
        self.assertIn("request deletion or correction", content)
        self.assertIn("technical logs", content)
        self.assertIn("/privacy", content)


if __name__ == "__main__":
    unittest.main()
