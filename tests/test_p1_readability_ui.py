import unittest
from pathlib import Path


FRONTEND = Path("app/frontend/index.html")


class P1ReadabilityUiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.content = FRONTEND.read_text(encoding="utf-8")

    def test_improve_result_guidance_is_localized_in_all_supported_languages(self) -> None:
        expected = [
            'enrich_guidance: "For a more complete result, photograph the nutrition label or fill in/correct the values."',
            'enrich_guidance: "Για πιο πλήρες αποτέλεσμα, φωτογράφισε τη διατροφική ετικέτα ή συμπλήρωσε/διόρθωσε τις τιμές."',
            'enrich_guidance: "Für ein vollständigeres Ergebnis fotografiere die Nährwerttabelle oder ergänze/korrigiere die Werte."',
            'enrich_guidance: "Pour un résultat plus complet, photographiez l’étiquette nutritionnelle ou complétez/corrigez les valeurs."',
        ]
        for text in expected:
            with self.subTest(text=text):
                self.assertIn(text, self.content)

    def test_clearer_next_step_labels_are_localized(self) -> None:
        expected = [
            'enrich_next_step_nutrition: "Photograph the nutrition label"',
            'enrich_next_step_ingredients: "Photograph the ingredients list"',
            'enrich_next_step_review: "Review the updated result"',
            'enrich_after_next_step: "After that, the app re-runs the analysis and shows the updated result."',
            'enrich_next_step_nutrition: "Φωτογράφισε τη διατροφική ετικέτα"',
            'enrich_next_step_ingredients: "Φωτογράφισε τη λίστα συστατικών"',
            'enrich_next_step_review: "Δες το ενημερωμένο αποτέλεσμα"',
            'enrich_after_next_step: "Μετά, η εφαρμογή επαναλαμβάνει την ανάλυση και δείχνει το ενημερωμένο αποτέλεσμα."',
            'enrich_next_step_nutrition: "Nährwerttabelle fotografieren"',
            'enrich_next_step_ingredients: "Zutatenliste fotografieren"',
            'enrich_next_step_review: "Aktualisiertes Ergebnis prüfen"',
            'enrich_after_next_step: "Danach wiederholt die App die Analyse und zeigt das aktualisierte Ergebnis."',
            'enrich_next_step_nutrition: "Photographier l’étiquette nutritionnelle"',
            'enrich_next_step_ingredients: "Photographier la liste des ingrédients"',
            'enrich_next_step_review: "Vérifier le résultat mis à jour"',
            'enrich_after_next_step: "Ensuite, l’application relance l’analyse et affiche le résultat mis à jour."',
        ]
        for text in expected:
            with self.subTest(text=text):
                self.assertIn(text, self.content)

    def test_improve_result_keeps_existing_actions_and_uses_prominent_guidance(self) -> None:
        self.assertIn('<div class="trustNotice improveGuidance">${t("enrich_guidance")}</div>', self.content)
        self.assertIn('<div class="nextStepText"><b>${t("enrich_next_step")}:</b> ${nextStepLabel}</div>', self.content)
        self.assertIn('<div class="stepHint">${t("enrich_after_next_step")}</div>', self.content)
        self.assertNotIn('<div class="stepHint">${t("enrich_step_refresh")} · ${t("enrich_step_result")}</div>', self.content)
        self.assertIn('id="nutritionPhotoBtn"', self.content)
        self.assertIn('id="ingredientPhotoBtn"', self.content)
        self.assertIn('id="manualEntryBtn"', self.content)
        self.assertIn('id="nutritionGalleryBtn"', self.content)
        self.assertIn('id="ingredientGalleryBtn"', self.content)

    def test_trust_hierarchy_is_presentation_only_for_confidence_and_alcohol(self) -> None:
        self.assertIn("function renderConfidenceSummary(data)", self.content)
        self.assertIn('class="trustNotice ${noticeClass}"', self.content)
        self.assertIn("function quickVerdictNoticeClass(data = null)", self.content)
        self.assertIn('return "trustNotice info";', self.content)
        self.assertIn('return "ok";', self.content)
        self.assertIn('${quickVerdictNoticeClass(d)}', self.content)
        self.assertIn('alcoholStatus === "alcoholic"', self.content)
        self.assertIn('alcoholStatus === "unverified_beer_radler"', self.content)


if __name__ == "__main__":
    unittest.main()
