# Beta Daily Review

Use this once per day during the 10-user soft beta.

## 0. Production review endpoint

For live Render review, use the protected internal endpoint instead of reading server files directly.

Endpoint:

`GET /internal/beta/feedback-summary`

Required header:

`X-Beta-Review-Token: <token>`

Generate a token in PowerShell:

```powershell
python -c "import secrets; print(secrets.token_urlsafe(48))"
```

Render setup:

1. Open the Render Dashboard
2. Open the NoesisFood service
3. Open `Environment`
4. Add Environment Variable
5. Key: `BETA_REVIEW_TOKEN`
6. Value: generated token
7. Save and deploy or redeploy

Production usage:

```powershell
curl.exe -H "X-Beta-Review-Token: <token>" https://noesisfood.onrender.com/internal/beta/feedback-summary
```

Monitoring note:

- `monitoring_window` is reported as `current_process_lifetime`
- these counters reset when the Render instance restarts or the service redeploys

Important:

- never put the token in frontend code
- never commit the token
- never share screenshots containing the token
- rotate the token if it is exposed or shared too widely

## 1. Review correction feedback

Feedback queue path:

`data/correction_feedback/correction_feedback.jsonl`

Run the helper:

```powershell
python scripts/review_beta_feedback.py
```

Useful variants:

```powershell
python scripts/review_beta_feedback.py --limit 10
python scripts/review_beta_feedback.py --json
python scripts/review_beta_feedback.py --path data/correction_feedback/correction_feedback.jsonl
```

The summary shows:

- total feedback submissions
- submissions by barcode/key/product
- most frequently corrected fields
- before/after values
- repeated product reports
- invalid or noisy records that can be detected locally

What to look for:

- the same barcode or key appearing repeatedly
- the same field being corrected often, especially `sugar_g`, `salt_g`, `fat_g`, `sat_fat_g`
- repeated before/after patterns for one product
- malformed lines or suspicious records

## 2. Review monitoring logs

Search for these event names:

- `scan_started`
- `scan_completed`
- `scan_failed`
- `ocr_success`
- `ocr_partial_rescue`
- `ocr_failed`
- `analysis_confidence_assigned`
- `correction_submitted`
- `correction_feedback_submitted`
- `feedback_submission_failed`
- `feedback_validation_failed`

Example PowerShell searches:

```powershell
rg "scan_started|scan_completed|scan_failed" .
rg "ocr_success|ocr_partial_rescue|ocr_failed" .
rg "feedback_submission_failed|feedback_validation_failed|correction_feedback_submitted" .
```

If you have exported application logs locally, point `rg` at that file instead of the repo root.

Quick counts:

```powershell
rg -o "scan_started" path\\to\\app.log | Measure-Object
rg -o "scan_completed" path\\to\\app.log | Measure-Object
rg -o "scan_failed" path\\to\\app.log | Measure-Object
rg -o "ocr_failed" path\\to\\app.log | Measure-Object
rg -o "feedback_submission_failed" path\\to\\app.log | Measure-Object
```

What to check:

- `scan_started` vs `scan_completed`: large gaps usually mean user-visible failures
- `scan_failed`: inspect `error_code` and `status_code`
- `ocr_failed`: photo path needs attention
- `ocr_partial_rescue`: OCR is working, but only partially
- `feedback_submission_failed` and `feedback_validation_failed`: users are trying to submit corrections and hitting friction

## 3. Daily beta rhythm

Minimum daily review:

1. Run `python scripts/review_beta_feedback.py`
2. Check repeated products and repeated corrected fields
3. Review failed scan events
4. Review OCR failures and partial rescues
5. Review feedback submission failures
6. Note the top 1-3 issues to fix next

## 4. What not to do yet

- Do not write correction feedback into the main product store
- Do not write back to OpenFoodFacts
- Do not change scoring math from one or two user reports alone
- Do not treat low confidence as product healthiness
