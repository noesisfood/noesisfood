import unittest
from pathlib import Path


class DietarySignalsUiTests(unittest.TestCase):
    def test_frontend_contains_dietary_signals_card_and_localized_strings(self) -> None:
        content = Path("app/frontend/index.html").read_text(encoding="utf-8")

        self.assertIn("function renderDietarySignalsCard(data)", content)
        self.assertIn('${renderDietarySignalsCard(d)}', content)
        self.assertIn('dietary_signals_title: "Dietary signals from available label data"', content)
        self.assertIn('dietary_signal_halal_certified: "Halal certification detected"', content)
        self.assertIn('dietary_signal_halal_possible_not_suitable: "Possible non-halal ingredient signals"', content)
        self.assertIn('dietary_signal_vegan_labeled: "Vegan label detected"', content)
        self.assertIn('dietary_signal_vegetarian_labeled: "Vegetarian label detected"', content)
        self.assertIn('dietary_signals_title: "Διατροφικά σήματα από διαθέσιμα δεδομένα ετικέτας"', content)
        self.assertIn('dietary_signals_title: "Ernährungssignale aus verfügbaren Etikettendaten"', content)
        self.assertIn('dietary_signals_title: "Signaux alimentaires à partir des données d\'étiquette disponibles"', content)

    def test_frontend_avoids_forbidden_dietary_claims(self) -> None:
        content = Path("app/frontend/index.html").read_text(encoding="utf-8").lower()

        self.assertNotIn("probably_vegan", content)
        self.assertNotIn("possible_halal", content)
        self.assertNotIn("religiously compliant", content)
        self.assertNotIn("guaranteed suitable", content)
        self.assertNotIn("dietary guarantee", content)
        self.assertNotIn("halal checker", content)

    def test_frontend_keeps_allergen_usage_context_and_feedback_hooks_present(self) -> None:
        content = Path("app/frontend/index.html").read_text(encoding="utf-8")

        self.assertIn('${renderAllergenCard(d)}', content)
        self.assertIn('${renderUsageContextBlock(d)}', content)
        self.assertIn('id="submitCorrectionFeedbackBtn"', content)
        self.assertIn('submit_correction_feedback: "Submit correction feedback"', content)

    def test_catalog_contains_only_v1_supported_signals(self) -> None:
        content = Path("app/data/dietary_signals_catalog.json").read_text(encoding="utf-8")
        self.assertIn('"halal"', content)
        self.assertIn('"vegan"', content)
        self.assertIn('"vegetarian"', content)
        self.assertNotIn('"kosher"', content)
        self.assertNotIn('"gluten_free"', content)
        self.assertNotIn('"lactose_free"', content)


if __name__ == "__main__":
    unittest.main()
