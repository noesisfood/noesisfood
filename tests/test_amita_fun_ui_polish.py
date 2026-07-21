import unittest
from pathlib import Path


FRONTEND = Path("app/frontend/index.html")


class AmitaFunUiPolishTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.content = FRONTEND.read_text(encoding="utf-8")

    def test_tiny_nonzero_nutrition_values_keep_two_decimals(self) -> None:
        self.assertIn(
            "if (n !== 0 && Math.abs(n) < 0.1) return (Math.round(n * 100) / 100).toString();",
            self.content,
        )
        self.assertIn("return (Math.round(n * 10) / 10).toString();", self.content)

    def test_ingredient_language_badge_uses_meta_before_inference(self) -> None:
        self.assertGreaterEqual(
            self.content.count("data?.ingredients_meta || data?.meta?.ingredients_meta || {}"),
            2,
        )
        self.assertIn("if (!sourceLang) return t(\"source_language_missing\");", self.content)

    def test_allergen_empty_sources_have_localized_fallback(self) -> None:
        expected = {
            "No explicit allergen source was available in the label data checked.",
            "Δεν υπήρχε ρητή πηγή αλλεργιογόνων στα διαθέσιμα δεδομένα ετικέτας.",
            "Keine ausdrückliche Allergenquelle war in den geprüften Etikettendaten verfügbar.",
            "Aucune source explicite d’allergènes n’était disponible dans les données d’étiquette vérifiées.",
        }
        for text in expected:
            with self.subTest(text=text):
                self.assertIn(text, self.content)
        self.assertIn('t("allergen_no_explicit_source")', self.content)


if __name__ == "__main__":
    unittest.main()
