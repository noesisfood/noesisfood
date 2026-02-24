from fastapi import APIRouter
from app.services.scanner_service import scan_product

router = APIRouter()

@router.get("/scan/{product_id}")
async def scan_endpoint(product_id: str):
    return await scan_product(product_id)