
import asyncio
from typing import Any, Dict, List, Optional
from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel

from shared.clients.es_client import es_client
from shared.models.schemas import CrawlRequest, CrawlResponse, HealthResponse
from src.crawlers.google_trends import google_crawler
from src.crawlers.naver_trends import naver_crawler

router = APIRouter()

class GoogleCrawlRequest(CrawlRequest):
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    include_related: bool = True

class NaverCrawlRequest(CrawlRequest):
    start_date: Optional[str] = None
    end_date: Optional[str] = None

class ProductTrendsCrawlRequest(BaseModel):
    top_n: int = 100
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    include_related: bool = True

class ProductTrendsCrawlResponse(BaseModel):
    success: bool
    google_trend_count: int = 0
    google_mapping_count: int = 0
    naver_trend_count: int = 0
    naver_mapping_count: int = 0
    products_processed: int = 0
    message: str

@router.get("/health", response_model=HealthResponse)
async def health_check():
    es_healthy = es_client.health_check()

    return HealthResponse(
        status="healthy" if es_healthy else "degraded",
        service="crawler",
        dependencies={
            "elasticsearch": es_healthy,
        },
    )

@router.post("/crawl/google", response_model=CrawlResponse)
async def crawl_google(request: GoogleCrawlRequest):
    if not request.keywords:
        raise HTTPException(status_code=400, detail="keywords는 필수입니다")

    try:
        count = await asyncio.to_thread(
            google_crawler.crawl_and_save,
            keywords=request.keywords,
            start_date=request.start_date,
            end_date=request.end_date,
            include_related=request.include_related,
        )

        return CrawlResponse(
            success=True,
            count=count,
            message=f"구글 트렌드에서 {count}건의 트렌드 데이터를 저장했습니다",
        )

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"구글 트렌드 크롤링 중 오류 발생: {str(e)}",
        )

@router.post("/crawl/naver", response_model=CrawlResponse)
async def crawl_naver(request: NaverCrawlRequest):
    if not request.keywords:
        raise HTTPException(status_code=400, detail="keywords는 필수입니다")

    try:
        count = await asyncio.to_thread(
            naver_crawler.crawl_and_save,
            keywords=request.keywords,
            start_date=request.start_date,
            end_date=request.end_date,
        )

        return CrawlResponse(
            success=True,
            count=count,
            message=f"네이버 트렌드에서 {count}건의 트렌드 데이터를 저장했습니다",
        )

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"네이버 트렌드 크롤링 중 오류 발생: {str(e)}",
        )

@router.post("/crawl/product-trends", response_model=ProductTrendsCrawlResponse)
async def crawl_product_trends(request: ProductTrendsCrawlRequest):
    try:
        products = await asyncio.to_thread(
            google_crawler.get_top_products_from_es,
            top_n=request.top_n,
        )

        if not products:
            return ProductTrendsCrawlResponse(
                success=False,
                products_processed=0,
                message="No products found in ES",
            )

        google_result = await asyncio.to_thread(
            google_crawler.crawl_product_trends_and_save,
            top_n=request.top_n,
            start_date=request.start_date,
            end_date=request.end_date,
            include_related=request.include_related,
            products=products,
        )

        naver_result = await asyncio.to_thread(
            naver_crawler.crawl_product_trends_and_save,
            top_n=request.top_n,
            start_date=request.start_date,
            end_date=request.end_date,
            products=products,
        )

        return ProductTrendsCrawlResponse(
            success=google_result.get("success", False) or naver_result.get("success", False),
            google_trend_count=google_result.get("trend_count", 0),
            google_mapping_count=google_result.get("mapping_count", 0),
            naver_trend_count=naver_result.get("trend_count", 0),
            naver_mapping_count=naver_result.get("mapping_count", 0),
            products_processed=len(products),
            message=(f"Google: {google_result.get('message', '')}, "
                     f"Naver: {naver_result.get('message', '')}"),
        )

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"상품 트렌드 크롤링 중 오류 발생: {str(e)}",
        )
