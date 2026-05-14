import unittest
from pathlib import Path


class DietarySignalsUiTests(unittest.TestCase):
    def test_frontend_contains_compact_dietary_card_and_localized_strings(self) -> None:
        content = Path("app/frontend/index.html").read_text(encoding="utf-8")

        self.assertIn("function renderDietarySignalsCard(data)", content)
        self.assertIn("function dietarySignalSummaryLabel(signalKey, signalData)", content)
        self.assertIn("function dietaryCoverageValueLabel(value)", content)
        self.assertIn('${renderDietarySignalsCard(d)}', content)
        self.assertIn('dietary_signals_title: "Dietary indications from available label data"', content)
        self.assertIn('dietary_signal_details: "View details"', content)
        self.assertIn('dietary_signal_summary_certified: "Certification detected"', content)
        self.assertIn('dietary_signal_summary_labeled: "Label detected"', content)
        self.assertIn('dietary_signal_summary_unclear: "Unclear"', content)
        self.assertIn('dietary_signal_summary_caution: "Points to check"', content)
        self.assertIn('dietary_signal_warning_shared: "Always check the official product packaging or certification."', content)
        self.assertIn('dietary_signals_title: "Διατροφικές ενδείξεις από διαθέσιμα δεδομένα ετικέτας"', content)
        self.assertIn('dietary_signal_summary_unclear: "Ασαφές"', content)
        self.assertIn('dietary_signal_summary_caution: "Σημεία προσοχής"', content)
        self.assertIn('dietary_signal_details: "Προβολή λεπτομερειών"', content)
        self.assertIn('dietary_signal_warning_shared: "Ελέγχετε πάντα την επίσημη συσκευασία ή πιστοποίηση του προϊόντος."', content)
        self.assertIn('dietary_signals_title: "Ernährungsbezogene Hinweise aus verfügbaren Etikettendaten"', content)
        self.assertIn('dietary_signal_summary_caution: "Wichtige Hinweise"', content)
        self.assertIn('dietary_signal_warning_shared: "Prüfen Sie immer die offizielle Verpackung oder Zertifizierung des Produkts."', content)
        self.assertIn('dietary_signals_title: "Indications alimentaires à partir des données d\'étiquette disponibles"', content)
        self.assertIn('dietary_signal_summary_caution: "Points d’attention"', content)

    def test_frontend_uses_compact_summary_with_shared_details_and_warning(self) -> None:
        content = Path("app/frontend/index.html").read_text(encoding="utf-8")
        dietary_slice = content[content.index("function renderDietarySignalsCard(data)") : content.index("function renderDetailsScoreSummary(data)")]

        self.assertIn('const summaryRows = order.map(signalKey => {', dietary_slice)
        self.assertIn('const detailBlocks = order.map(signalKey => {', dietary_slice)
        self.assertIn('<details style="margin-top:12px;">', dietary_slice)
        self.assertIn('summary class="mini" style="cursor:pointer;">${t("dietary_signal_details")}', dietary_slice)
        self.assertIn('${_escapeHtml(dietarySignalSummaryLabel(signalKey, signal))}', dietary_slice)
        self.assertEqual(dietary_slice.count('${_escapeHtml(t("dietary_signal_warning_shared"))}'), 1)
        self.assertNotIn('<div class="err" style="margin-top:10px;">${_escapeHtml(warning)}</div>', dietary_slice)

    def test_frontend_renders_allergens_before_dietary_signals(self) -> None:
        content = Path("app/frontend/index.html").read_text(encoding="utf-8")
        self.assertLess(content.index('${renderAllergenCard(d)}'), content.index('${renderDietarySignalsCard(d)}'))

    def test_frontend_orders_dietary_signals_vegan_then_vegetarian_then_halal(self) -> None:
        content = Path("app/frontend/index.html").read_text(encoding="utf-8")
        self.assertIn('const order = ["vegan", "vegetarian", "halal"];', content)

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
