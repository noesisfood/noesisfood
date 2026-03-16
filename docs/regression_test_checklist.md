# Regression Test Checklist

## Purpose
This pack is a lightweight manual regression guide for the multilingual ingredient intelligence flow in NoesisFood.

Use it after changes to:
- `app/frontend/index.html`
- `app/frontend/ingredient_glossary.json`
- `app/frontend/ingredient_dict_el.json`
- backend ingredient intelligence or E-number logic

Reference data lives in [app/frontend/regression_test_products.json](/C:/Users/pflem/noesisfood/app/frontend/regression_test_products.json).

## High-Priority Smoke Tests
Run the first five products in `regression_test_products.json` first after any deploy. These are the mandatory smoke tests:

1. `Coca-Cola Original Taste`
2. `Red Bull Energy Drink`
3. `Fanta Orange`
4. `Sprite`
5. `Schweppes Indian Tonic`

These cover:
- DE source ingredient text
- PL source ingredient text
- additive flags
- caffeine flag
- E-number cards
- grouped ingredient translation
- language switching across `EL`, `EN`, `DE`, `FR`

The JSON pack marks these entries with `"smoke_test": true`.

## Manual Test Flow
For each product:

1. Open the product in the app by barcode.
2. Verify the page loads without frontend errors.
3. Confirm the ingredient glossary toggle is available.
4. Enable ingredient translation.
5. Check the detected or inferred source language against the expected value in the JSON pack.
6. Verify `Μεταφράστηκαν X/Y` or the equivalent localized counter appears and is reasonable.
7. Check grouped ingredient rows for the listed `key_expected_matches`.
8. Check expected flags in the Ingredients Intelligence section.
9. Check expected E-numbers in the E-number panel.
10. Switch UI language through `EL`, `EN`, `DE`, `FR` and confirm labels and translated ingredient values change with the selected language.

## What To Verify In Each UI Language
For `EL`:
- Section headers are Greek.
- Grouped ingredient items are shown in Greek where glossary coverage exists.
- E-number title, role, and explanation body are Greek.

For `EN`:
- Section headers are English.
- Grouped ingredient items are shown in English where glossary coverage exists.
- E-number title, role, and explanation body are English.

For `DE`:
- Section headers are German.
- Grouped ingredient items are shown in German where glossary coverage exists.
- E-number title, role, and explanation body are German.

For `FR`:
- Section headers are French.
- Grouped ingredient items are shown in French where glossary coverage exists.
- E-number title, role, and explanation body are French.

## Pass / Fail Criteria
Pass:
- The app renders without breaking the scan flow.
- The selected UI language changes visible labels immediately.
- Source language is correct or plausibly inferred.
- Listed `key_expected_matches` resolve to the expected canonical concepts.
- Expected flags appear when relevant.
- Expected E-numbers appear when relevant.
- No obvious fallback regression appears for grouped ingredient rendering.

Fail:
- The language selector changes but visible content stays in the old language.
- Ingredient translation toggle stops affecting grouped ingredient values.
- Expected high-confidence aliases such as `kofeina`, `Säuerungsmittel`, `arôme naturel`, `woda`, `barwniki` stop resolving.
- Expected E-number cards disappear or fall back to mixed-language text.
- Source attribution or ingredient note text becomes inconsistent across languages.

## Recommended Smoke Assertions
Use these quick checks to catch the highest-value regressions:

- `Coca-Cola Original Taste`
  - `Säuerungsmittel` resolves to `Acidifier` in EN.
  - `Farbstoff` resolves to `Χρωστική` in EL.
  - `E150` and `E338` appear.
  - caffeine-related flag appears.

- `Red Bull Energy Drink`
  - `kofeina` resolves to `Caffeine` in EN.
  - `woda` resolves to `Νερό` in EL.
  - `aromaty` or other plural flavour aliases resolve correctly.
  - source language is `PL`.

- `Volvic Touch Citron`
  - `arôme naturel` resolves to `Natural flavouring` in EN.
  - `concentré de jus de citron` resolves to `Lemon juice concentrate` in EN.
  - source language is `FR`.

- `Fanta Orange`
  - `natürliches Orangenaroma` resolves to the orange flavouring canonical entry.
  - `E330` appears with localized explanation content.

## Expansion Rules
When adding a new regression product:

1. Prefer stable, widely available OFF products with known barcodes.
2. Include at least one concrete expected translation match.
3. Add expected flags only if they are stable enough to avoid noisy failures.
4. Add expected E-numbers only if they appear consistently in OFF data.
5. Set priority:
   - `P0`: release-blocking smoke test
   - `P1`: important multilingual coverage
   - `P2`: useful ingredient-family coverage
   - `P3`: broader confidence coverage

## Suggested Regression Cadence
- After any deploy: run the five smoke tests first.
- After glossary changes: run all `P0` and `P1`.
- After localization changes: run all `P0` and switch all four UI languages on at least two products.
- Before deploys that touch ingredient intelligence: run the full pack if time allows.
