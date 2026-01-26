"""Crawler Service API 엔드포인트"""

from typing import List, Optional
from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel

from shared.clients.es_client import es_client
from shared.models.schemas import CrawlRequest, CrawlResponse, HealthResponse
from src.crawlers.naver_datalab import naver_crawler
from src.crawlers.google_trends import google_crawler
from src.crawlers.paper_crawler import create_paper_crawler

router = APIRouter()


# 추가 요청 스키마
class NaverCrawlRequest(CrawlRequest):
    """네이버 크롤링 요청"""
    start_date: Optional[str] = None  # YYYY-MM-DD
    end_date: Optional[str] = None    # YYYY-MM-DD


class GoogleCrawlRequest(CrawlRequest):
    """구글 트렌드 크롤링 요청"""
    start_date: Optional[str] = None  # YYYY-MM-DD
    end_date: Optional[str] = None    # YYYY-MM-DD
    include_related: bool = True      # 연관 검색어 포함 여부


class PaperCrawlRequest(CrawlRequest):
    """논문 크롤링 요청"""
    sources: Optional[List[str]] = None  # ["pubmed", "arxiv"]
    max_results: int = 50                # 소스당 최대 결과 수


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


@router.post("/crawl/naver", response_model=CrawlResponse)
async def crawl_naver(request: NaverCrawlRequest):
    """
    네이버 데이터랩 크롤링

    - 키워드 목록을 받아 네이버 데이터랩 트렌드 데이터를 수집
    - 수집된 데이터는 Elasticsearch에 저장
    - 환경변수 NAVER_CLIENT_ID, NAVER_CLIENT_SECRET 필요
    """
    if not request.keywords:
        raise HTTPException(status_code=400, detail="keywords는 필수입니다")

    try:
        count = await naver_crawler.crawl_and_save(
            keywords=request.keywords,
            start_date=request.start_date,
            end_date=request.end_date,
        )

        return CrawlResponse(
            success=True,
            count=count,
            message=f"네이버 데이터랩에서 {count}건의 트렌드 데이터를 저장했습니다",
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"네이버 크롤링 중 오류 발생: {str(e)}",
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
