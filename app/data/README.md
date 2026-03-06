# NoesisFood Curation v0

This folder contains a curated product layer on top of OpenFoodFacts.

## Source of truth
- Curators edit a shared Google Sheet using the required columns.
- The sheet is exported as CSV.
- `scripts/export_products.py` validates and writes `app/data/products.json` (default output).

## Export
```bash
python scripts/export_products.py path/to/sheet.csv