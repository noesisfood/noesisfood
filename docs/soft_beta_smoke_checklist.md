# Soft Beta Smoke Checklist

Use this checklist before and after each Render deploy during the 10-user soft beta.

## Pre-deploy

- Confirm `main` is clean and the intended commit hash is pushed.
- Run the targeted regression suite:
  - `python -m unittest tests.test_monitoring_service tests.test_correction_feedback tests.test_session_correction_recalc tests.test_correction_prefill tests.test_analysis_confidence tests.test_vita_score_explanation tests.test_photo_fallback tests.test_scan_photo_route`

## Core flows

- Barcode scan:
  - scan a known product barcode
  - verify Vita Score, confidence, and explanation render
- Manual analysis:
  - submit a manual payload with core nutrition fields
  - verify result renders and confidence is present
- Photo nutrition analysis:
  - upload a nutrition photo
  - verify either a result or a recovery-oriented error message appears
- Corrected-session recalculation:
  - open `Correct nutrition values`
  - change at least one nutrition field
  - verify Final Vita Score and Basic Nutrition Score refresh
- Feedback submission:
  - submit correction feedback from a corrected-session result
  - verify success response

## UI and language checks

- Beta disclaimer is visible on home and result screens.
- Confidence/explanation block is visible on the result screen.
- EL / EN / DE / FR critical strings exist:
  - beta disclaimer copy
  - confidence badge labels
  - correction CTA
  - feedback CTA
  - feedback success/failure copy
  - recovery-oriented photo error copy

## Post-deploy Render verification

- Confirm local `HEAD` and `origin/main` match.
- Poll the live Render homepage until the new UI strings appear.
- Run one live scan path:
  - barcode or manual minimum
- Run one live corrected-session recalculation.
- Run one live correction feedback submission.
- Check logs for:
  - `scan_started`
  - `scan_completed` or `scan_failed`
  - `analysis_confidence_assigned`
  - `ocr_success` / `ocr_partial_rescue` / `ocr_failed` when photo analysis is used
  - `correction_submitted`
  - `correction_feedback_submitted`

## Beta go / no-go

- Go if all core flows work and no new blocking error codes appear.
- No-go if any of the following fail:
  - barcode/manual/photo entry path
  - corrected-session recalculation
  - feedback submission
  - critical localized strings missing
