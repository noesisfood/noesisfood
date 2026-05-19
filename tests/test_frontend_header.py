import unittest
from pathlib import Path

from fastapi.testclient import TestClient

from app.main import app


class FrontendHeaderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = TestClient(app)

    def test_header_uses_responsive_branding_markup(self) -> None:
        content = Path("app/frontend/index.html").read_text(encoding="utf-8")

        self.assertIn('class="brandIconImg"', content)
        self.assertIn('src="/static/brand/noesisfood-logo-source.png"', content)
        self.assertIn('class="brandWordmark">NoesisFood</div>', content)
        self.assertIn(".topRight{display:flex; flex-direction:column; align-items:flex-end; gap:8px; flex:0 0 auto;}", content)
        self.assertIn("@media (min-width: 560px){", content)
        self.assertIn(".topRight{\n        flex-direction:row;", content)
        self.assertIn('id="langSel"', content)
        self.assertIn('id="stepTxt"', content)
        self.assertNotIn('<div class="logo"></div>', content)

    def test_brand_asset_route_returns_png(self) -> None:
        response = self.client.get("/static/brand/noesisfood-logo-source.png")
        self.assertEqual(response.status_code, 200)
        self.assertIn("image/png", response.headers.get("content-type", ""))


if __name__ == "__main__":
    unittest.main()
