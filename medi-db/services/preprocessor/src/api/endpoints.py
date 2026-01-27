"""API 엔드포인트 정의"""
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
    """
    헬스체크 엔드포인트

    서비스 상태 및 의존성(Elasticsearch) 연결 상태를 확인합니다.
    """
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
    """
    전체 전처리 실행

    주문, 상품, 약국 데이터를 순차적으로 전처리하고
    키워드 추출 및 트렌드 매핑을 수행합니다.
    """
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
    """
    주문 데이터 전처리

    orders_detail 및 orders 테이블의 데이터를 전처리합니다.
    - NULL 값 처리
    - 이상치 제거
    - 파생 변수 생성 (총 금액, 날짜 분리 등)
    """
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
    """
    상품 데이터 전처리

    product 테이블의 데이터를 전처리합니다.
    - 텍스트 정규화
    - 카테고리 매핑
    - 상품명/성분명 정규화
    - 키워드 추출 및 트렌드 매핑
    """
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
    """
    약국 데이터 전처리

    account 테이블의 데이터를 전처리합니다.
    - 텍스트 정규화
    - 전화번호 정규화
    - 주소에서 지역 정보 추출
    - 좌표 유효성 검증
    """
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
    """
    키워드 추출 (트렌드 매핑 없이)

    모든 상품에서 키워드를 추출하고 통계를 반환합니다.
    - name: 상품명에서 추출한 키워드
    - efficacy: 효능에서 추출한 키워드
    - ingredient: 성분에서 추출한 키워드
    - category: 카테고리 코드에서 매핑된 키워드
    """
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
    """
    상품 판매 요약 조회

    상품별 판매 통계를 반환합니다.
    """
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
    """
    약국 구매 요약 조회

    약국별 구매 통계를 반환합니다.
    """
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
