import unittest
from pathlib import Path

from PIL import Image
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
        self.assertEqual(body["theme_color"], "#0f2742")
        self.assertEqual(len(body["icons"]), 3)

    def test_service_worker_route_returns_expected_script(self) -> None:
        response = self.client.get("/service-worker.js")
        self.assertEqual(response.status_code, 200)
        self.assertIn("application/javascript", response.headers.get("content-type", ""))
        content = response.text
        self.assertIn('const CACHE_NAME = "noesisfood-shell-v2";', content)
        self.assertIn("if (key !== CACHE_NAME)", content)
        self.assertIn('"/scan/manual"', content)
        self.assertIn('"/scan/photo"', content)
        self.assertIn('"/feedback/correction"', content)
        self.assertIn('"/internal/beta"', content)
        self.assertIn('request.method !== "GET"', content)
        self.assertNotIn('const CACHE_NAME = "noesisfood-shell-v1";', content)

    def test_frontend_links_manifest_and_registers_service_worker(self) -> None:
        content = Path("app/frontend/index.html").read_text(encoding="utf-8")
        self.assertIn('<link rel="manifest" href="/manifest.webmanifest" />', content)
        self.assertIn('<meta name="theme-color" content="#0f2742" />', content)
        self.assertIn('if ("serviceWorker" in navigator)', content)
        self.assertIn('navigator.serviceWorker.register("/service-worker.js").catch(() => {});', content)
        self.assertIn('window.__NF_BUILD__ = "2026-05-20-pwa-shell-v2";', content)

    def test_assetlinks_route_returns_expected_json(self) -> None:
        response = self.client.get("/.well-known/assetlinks.json")
        self.assertEqual(response.status_code, 200)
        self.assertIn("application/json", response.headers.get("content-type", ""))
        body = response.json()
        self.assertEqual(len(body), 1)
        self.assertIn("delegate_permission/common.handle_all_urls", body[0]["relation"])
        self.assertEqual(body[0]["target"]["namespace"], "android_app")
        self.assertEqual(body[0]["target"]["package_name"], "com.noesisfood.app")
        self.assertIn(
            "AF:A2:CC:DA:B9:DD:41:24:17:6D:70:58:00:8F:41:52:52:91:71:11:7A:25:D1:61:2E:C6:A4:EA:34:A2:A7:B9",
            body[0]["target"]["sha256_cert_fingerprints"],
        )

    def test_icon_files_exist(self) -> None:
        for name in ("icon-192.png", "icon-512.png", "icon-512-maskable.png"):
            self.assertTrue((Path("app/frontend/icons") / name).exists(), name)

    def test_icon_files_are_valid_pngs_with_expected_sizes(self) -> None:
        expected = {
            "icon-192.png": (192, 192),
            "icon-512.png": (512, 512),
            "icon-512-maskable.png": (512, 512),
        }
        for name, size in expected.items():
            path = Path("app/frontend/icons") / name
            with Image.open(path) as image:
                self.assertEqual(image.format, "PNG")
                self.assertEqual(image.size, size)

    def test_icon_routes_return_png(self) -> None:
        for route in ("/icons/icon-192.png", "/icons/icon-512.png", "/icons/icon-512-maskable.png"):
            response = self.client.get(route)
            self.assertEqual(response.status_code, 200, route)
            self.assertIn("image/png", response.headers.get("content-type", ""), route)


if __name__ == "__main__":
    unittest.main()
