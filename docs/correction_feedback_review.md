# Correction Feedback Review

This document describes how to review the append-only correction feedback queue during soft beta.

## Storage location

- Feedback is stored separately from product analysis data.
- JSONL path:
  - `data/correction_feedback/correction_feedback.jsonl`

Each line is one independent feedback record.

## What is stored

- product name / brand / barcode / key
- source type
- language
- timestamp
- original nutrition snapshot
- corrected nutrition snapshot
- analysis confidence
- confidence reasons
- corrected-session marker

## What is not stored

- no user identifiers
- no IP addresses in the feedback payload
- no photo blobs
- no raw OCR text

## How to inspect the queue

Examples:

```powershell
Get-Content data\correction_feedback\correction_feedback.jsonl | Select-Object -Last 20
```

```powershell
Get-Content data\correction_feedback\correction_feedback.jsonl | Select-String "4000000000001"
```

## How to identify repeated barcode/key reports

Look for repeated values of:

- `product.barcode`
- `product.key`
- `changed_fields`
- identical corrected nutrition snapshots

Repeated reports for the same barcode/key are higher-priority review candidates than isolated one-off corrections.

## Triage guidance

Prioritize:

1. repeated corrections for the same barcode/key
2. repeated corrections to the same field, such as `sugar_g` or `salt_g`
3. corrections attached to medium/high-confidence results
4. corrections coming from photo/barcode flows, where reusable data improvement is more likely

De-prioritize:

- one-off noisy submissions
- records missing a barcode/key but only containing a generic name
- submissions with implausible context even if they passed validation

## Review cadence during soft beta

- Review the queue at least 2-3 times per week.
- If beta activity rises, review daily.

## What not to do yet

- do not automatically write corrections into the main product data
- do not write back to OpenFoodFacts
- do not auto-train any scoring or OCR behavior from this queue

This queue is for controlled review only.
