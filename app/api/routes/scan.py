# app/api/routes/scan.py

from fastapi import APIRouter
from app.services.scanner_service import scan_product

router = APIRouter()

@router.get("/scan/{key}")
async def scan_endpoint(key: str):
    # scanner_service.scan_product is async -> must await
    return await scan_product(key)