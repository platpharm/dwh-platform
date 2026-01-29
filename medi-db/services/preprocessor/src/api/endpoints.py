import logging
from fastapi import APIRouter, HTTPException

from shared.clients.es_client import es_client
from shared.models.schemas import PreprocessResponse, HealthResponse

from src.processors.order_processor import order_processor
from src.processors.product_processor import product_processor
from src.processors.pharmacy_processor import pharmacy_processor
from src.processors.keyword_extractor import keyword_extractor

logger = logging.getLogger(__name__)

router = APIRouter()

@router.get("/health", response_model=HealthResponse)
async def health_check():
    es_healthy = es_client.health_check()

    status = "healthy" if es_healthy else "unhealthy"

    return HealthResponse(
        status=status,
        service="preprocessor",
        dependencies={
            "elasticsearch": es_healthy
        }
    )

@router.post("/preprocess/all", response_model=PreprocessResponse)
async def preprocess_all():
    logger.info("Starting full preprocessing pipeline")

    results = {
        "orders": None,
        "products": None,
        "accounts": None,
        "keywords": None
    }

    total_count = 0

    try:
        results["orders"] = order_processor.process()
        if results["orders"]["success"]:
            total_count += results["orders"].get("processed_count", 0)

        results["products"] = product_processor.process()
        if results["products"]["success"]:
            total_count += results["products"].get("processed_count", 0)

        results["accounts"] = pharmacy_processor.process()
        if results["accounts"]["success"]:
            total_count += results["accounts"].get("processed_count", 0)

        results["keywords"] = keyword_extractor.process()
        if results["keywords"]["success"]:
            total_count += results["keywords"].get("mapping_count", 0)

        all_success = all(
            r["success"] for r in results.values() if r is not None
        )

        logger.info(f"Full preprocessing completed: {total_count} total records processed")

        return PreprocessResponse(
            success=all_success,
            processed_count=total_count,
            message=f"Full preprocessing completed. Results: {results}"
        )

    except Exception as e:
        logger.error(f"Full preprocessing failed: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Preprocessing failed: {str(e)}"
        )

@router.post("/preprocess/orders", response_model=PreprocessResponse)
async def preprocess_orders():
    logger.info("Starting order data preprocessing")

    try:
        result = order_processor.process()

        return PreprocessResponse(
            success=result["success"],
            processed_count=result.get("processed_count", 0),
            message=result.get("message", "")
        )

    except Exception as e:
        logger.error(f"Order preprocessing failed: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Order preprocessing failed: {str(e)}"
        )

@router.post("/preprocess/products", response_model=PreprocessResponse)
async def preprocess_products():
    logger.info("Starting product data preprocessing")

    try:
        result = product_processor.process()

        return PreprocessResponse(
            success=result["success"],
            processed_count=result.get("processed_count", 0),
            message=result.get("message", "")
        )

    except Exception as e:
        logger.error(f"Product preprocessing failed: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Product preprocessing failed: {str(e)}"
        )

@router.post("/preprocess/accounts", response_model=PreprocessResponse)
async def preprocess_accounts():
    logger.info("Starting pharmacy (account) data preprocessing")

    try:
        result = pharmacy_processor.process()

        return PreprocessResponse(
            success=result["success"],
            processed_count=result.get("processed_count", 0),
            message=result.get("message", "")
        )

    except Exception as e:
        logger.error(f"Pharmacy preprocessing failed: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Pharmacy preprocessing failed: {str(e)}"
        )

@router.post("/keywords/extract")
async def extract_keywords():
    try:
        result = keyword_extractor.extract_all_keywords()
        return result

    except Exception as e:
        logger.error(f"Keyword extraction failed: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Keyword extraction failed: {str(e)}"
        )

@router.get("/products/summary")
async def get_products_summary():
    try:
        summary = order_processor.get_product_sales_summary()
        return {
            "success": True,
            "count": len(summary),
            "data": summary
        }

    except Exception as e:
        logger.error(f"Failed to get product summary: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get product summary: {str(e)}"
        )

@router.get("/pharmacies/summary")
async def get_pharmacies_summary():
    try:
        summary = order_processor.get_pharmacy_purchase_summary()
        return {
            "success": True,
            "count": len(summary),
            "data": summary
        }

    except Exception as e:
        logger.error(f"Failed to get pharmacy summary: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get pharmacy summary: {str(e)}"
        )
