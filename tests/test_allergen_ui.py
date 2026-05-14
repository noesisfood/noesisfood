import unittest
from pathlib import Path


class AllergenUiTests(unittest.TestCase):
    def test_frontend_contains_allergen_card_and_localized_strings(self) -> None:
        content = Path("app/frontend/index.html").read_text(encoding="utf-8")

        self.assertIn("function renderAllergenCard(data)", content)
        self.assertIn('${renderAllergenCard(d)}', content)
        self.assertIn('allergen_detected_title: "Detected allergen signals"', content)
        self.assertIn('allergen_detected_title: "Εντοπισμένες ενδείξεις αλλεργιογόνων"', content)
        self.assertIn('allergen_detected_title: "Erkannte Allergensignale"', content)
        self.assertIn('allergen_detected_title: "Signaux allergènes détectés"', content)
        self.assertIn('allergen_possible_title: "Possible allergen signals"', content)
        self.assertIn('allergen_warning_fallback: "Always check the official product label if you have allergies."', content)
        self.assertIn('allergen_warning_fallback: "Αν έχετε αλλεργία, ελέγχετε πάντα την επίσημη συσκευασία."', content)
        self.assertIn('allergen_warning_fallback: "Wenn Sie Allergien haben, prüfen Sie immer das offizielle Produktetikett."', content)
        self.assertIn('allergen_warning_fallback: "Si vous avez des allergies, vérifiez toujours l\'étiquette officielle du produit."', content)

    def test_frontend_avoids_overclaiming_words_for_allergen_feature(self) -> None:
        content = Path("app/frontend/index.html").read_text(encoding="utf-8").lower()

        self.assertNotIn("allergen-free", content)
        self.assertNotIn("safe for allergy sufferers", content)
        self.assertNotIn("complete coverage", content)
        self.assertNotIn("no cross-contamination risk", content)
        self.assertNotIn("no may contain risk", content)

    def test_catalog_contains_eu14_groups(self) -> None:
        content = Path("app/data/allergen_catalog.json").read_text(encoding="utf-8")
        for allergen_id in (
            "gluten",
            "crustaceans",
            "eggs",
            "fish",
            "peanuts",
            "soybeans",
            "milk_lactose",
            "nuts",
            "celery",
            "mustard",
            "sesame",
            "sulphites",
            "lupin",
            "molluscs",
        ):
            self.assertIn(f'"id": "{allergen_id}"', content)


if __name__ == "__main__":
    unittest.main()
