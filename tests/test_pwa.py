import unittest
from pathlib import Path

from fastapi.testclient import TestClient

from app.main import app


class PwaTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = TestClient(app)

    def test_manifest_route_returns_expected_fields(self) -> None:
        response = self.client.get("/manifest.webmanifest")
        self.assertEqual(response.status_code, 200)
        self.assertIn("application/manifest+json", response.headers.get("content-type", ""))
        body = response.json()
        self.assertEqual(body["name"], "NoesisFood")
        self.assertEqual(body["short_name"], "NoesisFood")
        self.assertIn("Nutrition intelligence for supermarket products", body["description"])
        self.assertEqual(body["start_url"], "/")
        self.assertEqual(body["scope"], "/")
        self.assertEqual(body["display"], "standalone")
        self.assertEqual(body["theme_color"], "#0f766e")
        self.assertEqual(len(body["icons"]), 3)

    def test_service_worker_route_returns_expected_script(self) -> None:
        response = self.client.get("/service-worker.js")
        self.assertEqual(response.status_code, 200)
        self.assertIn("application/javascript", response.headers.get("content-type", ""))
        content = response.text
        self.assertIn('const CACHE_NAME = "noesisfood-shell-v1";', content)
        self.assertIn('"/scan/manual"', content)
        self.assertIn('"/scan/photo"', content)
        self.assertIn('"/feedback/correction"', content)
        self.assertIn('"/internal/beta"', content)
        self.assertIn('request.method !== "GET"', content)

    def test_frontend_links_manifest_and_registers_service_worker(self) -> None:
        content = Path("app/frontend/index.html").read_text(encoding="utf-8")
        self.assertIn('<link rel="manifest" href="/manifest.webmanifest" />', content)
        self.assertIn('<meta name="theme-color" content="#0f766e" />', content)
        self.assertIn('if ("serviceWorker" in navigator)', content)
        self.assertIn('navigator.serviceWorker.register("/service-worker.js").catch(() => {});', content)

    def test_icon_files_exist(self) -> None:
        for name in ("icon-192.png", "icon-512.png", "icon-512-maskable.png"):
            self.assertTrue((Path("app/frontend/icons") / name).exists(), name)


if __name__ == "__main__":
    unittest.main()
