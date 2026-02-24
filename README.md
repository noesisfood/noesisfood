# NoesisFood

NoesisFood is a consumer transparency and nutrition awareness engine.

It analyzes food products using:
- Hybrid VitaScore v3 (per 100g/ml + per serving sugar impact)
- WHO sugar guidelines
- Protein bonus (solid foods only)
- Beverage-aware strict scoring
- OpenFoodFacts integration
- RASFF alert layer

## Tech Stack

- FastAPI
- Python
- OpenFoodFacts API
- ngrok (for public testing)

## Run locally

```bash
pip install -r requirements.txt
python -m uvicorn app.main:app --reload --port 8000
