"""Forecasting Service FastAPI 엔트리포인트"""
from fastapi import FastAPI
from contextlib import asynccontextmanager

from shared.clients.es_client import es_client
from shared.models.schemas import HealthResponse
from src.api.endpoints import router


@asynccontextmanager
async def lifespan(app: FastAPI):
    """애플리케이션 생명주기 관리"""
    yield


app = FastAPI(
    title="MediDB Forecasting Service",
    description="수요예측 및 인기 의약품 랭킹 서비스",
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(router)


@app.get("/health", response_model=HealthResponse, tags=["Health"])
async def health_check():
    """헬스체크 엔드포인트"""
    es_healthy = es_client.health_check()

    return HealthResponse(
        status="healthy" if es_healthy else "degraded",
        service="forecasting",
        dependencies={
            "elasticsearch": es_healthy,
        }
    )
