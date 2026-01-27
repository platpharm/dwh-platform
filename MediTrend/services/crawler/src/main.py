"""Crawler Service - FastAPI 엔트리포인트"""

from fastapi import FastAPI
from contextlib import asynccontextmanager

from shared.clients.es_client import es_client
from src.api import router


@asynccontextmanager
async def lifespan(app: FastAPI):
    """애플리케이션 라이프사이클 관리"""
    print("Crawler Service starting...")
    if es_client.health_check():
        print("Elasticsearch connection: OK")
    else:
        print("Elasticsearch connection: FAILED")
    yield
    print("Crawler Service shutting down...")


app = FastAPI(
    title="MediDB Crawler Service",
    description="트렌드 데이터 크롤링 서비스 (구글 트렌드, 논문)",
    version="0.1.0",
    lifespan=lifespan,
)

app.include_router(router, prefix="")


@app.get("/")
async def root():
    """루트 엔드포인트"""
    return {
        "service": "MediDB Crawler Service",
        "version": "0.2.0",
        "endpoints": [
            "/health",
            "/crawl/google",
            "/crawl/papers",
            "/crawl/product-trends",
            "/crawl/product-trends-pg",
        ],
    }
