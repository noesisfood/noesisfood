import unittest
from pathlib import Path


class UsageContextUiTests(unittest.TestCase):
    def test_frontend_contains_usage_context_rendering_and_strings(self) -> None:
        content = Path("app/frontend/index.html").read_text(encoding="utf-8")

        self.assertIn("function usageContextApplies(data)", content)
        self.assertIn("function renderUsageContextBlock(data)", content)
        self.assertIn('usage_context_label_seasoning = "Seasoning use"', content)
        self.assertIn('usage_context_label_seasoning = "\\u03a7\\u03c1\\u03ae\\u03c3\\u03b7 \\u03c9\\u03c2 \\u03ba\\u03b1\\u03c1\\u03cd\\u03ba\\u03b5\\u03c5\\u03bc\\u03b1"', content)
        self.assertIn('usage_context_label_seasoning = "Verwendung als Gew\\u00fcrz"', content)
        self.assertIn('usage_context_label_seasoning = "Utilisation comme assaisonnement"', content)
        self.assertIn('${renderUsageContextBlock(d)}', content)
        self.assertIn('const quickVerdict = usageContextApplies(d) ? usageContextPrimaryLabel(d) : quickVerdictLabel(score);', content)

    def test_frontend_keeps_allergen_and_feedback_hooks_present(self) -> None:
        content = Path("app/frontend/index.html").read_text(encoding="utf-8")

        self.assertIn('${renderAllergenCard(d)}', content)
        self.assertIn('id="submitCorrectionFeedbackBtn"', content)
        self.assertIn('correct_nutrition_values: "Correct nutrition values"', content)
        self.assertIn('submit_correction_feedback: "Submit correction feedback"', content)

    def test_frontend_does_not_show_generic_quick_verdict_as_primary_when_usage_context_applies(self) -> None:
        content = Path("app/frontend/index.html").read_text(encoding="utf-8")
        self.assertIn('usageContextApplies(d) ? usageContextPrimaryLabel(d) : quickVerdictLabel(score)', content)


if __name__ == "__main__":
    unittest.main()
