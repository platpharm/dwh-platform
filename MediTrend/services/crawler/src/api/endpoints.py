"""Crawler Service API 엔드포인트"""

from typing import Any, Dict, List, Optional
from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel

from shared.clients.es_client import es_client
from shared.models.schemas import CrawlRequest, CrawlResponse, HealthResponse
from src.crawlers.google_trends import google_crawler
from src.crawlers.paper_crawler import create_paper_crawler

router = APIRouter()


class GoogleCrawlRequest(CrawlRequest):
    """구글 트렌드 크롤링 요청"""
    start_date: Optional[str] = None  # YYYY-MM-DD
    end_date: Optional[str] = None    # YYYY-MM-DD
    include_related: bool = True      # 연관 검색어 포함 여부


class PaperCrawlRequest(CrawlRequest):
    """논문 크롤링 요청"""
    sources: Optional[List[str]] = None  # ["pubmed", "arxiv"]
    max_results: int = 50                # 소스당 최대 결과 수


class ProductTrendsCrawlRequest(BaseModel):
    """상품명 기반 트렌드 크롤링 요청 (CDC ES 사용)"""
    top_n: int = 100                     # 검색할 상위 상품 수
    start_date: Optional[str] = None     # YYYY-MM-DD
    end_date: Optional[str] = None       # YYYY-MM-DD
    include_related: bool = True         # 연관 검색어 포함 여부


class ProductTrendsCrawlResponse(BaseModel):
    """상품명 기반 트렌드 크롤링 응답"""
    success: bool
    trend_count: int
    mapping_count: int
    products_processed: int
    unique_keywords: int
    category_fallbacks: int
    message: str


@router.get("/health", response_model=HealthResponse)
async def health_check():
    """헬스체크 엔드포인트"""
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
    """
    구글 트렌드 크롤링

    - 키워드 목록을 받아 구글 트렌드 데이터를 수집
    - 수집된 데이터는 Elasticsearch에 저장
    - pytrends 라이브러리 사용 (API 키 불필요)
    """
    if not request.keywords:
        raise HTTPException(status_code=400, detail="keywords는 필수입니다")

    try:
        count = google_crawler.crawl_and_save(
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


@router.post("/crawl/papers", response_model=CrawlResponse)
async def crawl_papers(request: PaperCrawlRequest):
    """
    논문 크롤링 (PubMed, arXiv)

    - 키워드 목록을 받아 관련 논문 제목/초록 데이터를 수집
    - 수집된 데이터는 Elasticsearch에 저장
    - sources: ["pubmed", "arxiv"] 중 선택 가능 (기본: 둘 다)
    """
    if not request.keywords:
        raise HTTPException(status_code=400, detail="keywords는 필수입니다")

    try:
        async with create_paper_crawler() as crawler:
            count = await crawler.crawl_and_save(
                keywords=request.keywords,
                sources=request.sources,
                max_results=request.max_results,
            )

        return CrawlResponse(
            success=True,
            count=count,
            message=f"논문 검색에서 {count}건의 데이터를 저장했습니다",
        )

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"논문 크롤링 중 오류 발생: {str(e)}",
        )


@router.post("/crawl/product-trends", response_model=ProductTrendsCrawlResponse)
async def crawl_product_trends(request: ProductTrendsCrawlRequest):
    """
    상품명 기반 Google Trends 크롤링 (CDC ES 사용)

    - CDC ES (platpharm.public.product)에서 상품명 조회
    - 인기 상품 Top N개 선별 (ranking_result 우선, 없으면 전체 상품)
    - 상품명을 키워드로 Google Trends 데이터를 수집
    - 검색량이 적은 상품은 카테고리명으로 대체 검색
    - 수집된 데이터와 상품-키워드 매핑을 Elasticsearch에 저장
    """
    try:
        result = google_crawler.crawl_product_trends_and_save(
            top_n=request.top_n,
            start_date=request.start_date,
            end_date=request.end_date,
            include_related=request.include_related,
        )

        return ProductTrendsCrawlResponse(
            success=result.get("success", False),
            trend_count=result.get("trend_count", 0),
            mapping_count=result.get("mapping_count", 0),
            products_processed=result.get("products_processed", 0),
            unique_keywords=result.get("unique_keywords", 0),
            category_fallbacks=result.get("category_fallbacks", 0),
            message=result.get("message", ""),
        )

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"상품 트렌드 크롤링 중 오류 발생: {str(e)}",
        )


