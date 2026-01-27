"""MediTrend Targeting Service - FastAPI 엔트리포인트"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.api.endpoints import router
from shared.clients.es_client import es_client

app = FastAPI(
    title="MediTrend Targeting Service",
    description="의약품-약국 타겟팅 매칭 서비스",
    version="0.1.0",
)

# CORS 설정
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 라우터 등록
app.include_router(router)


@app.on_event("startup")
async def startup_event():
    """서비스 시작 시 초기화"""
    # ES 연결 확인
    if es_client.health_check():
        print("Elasticsearch connection established")
    else:
        print("Warning: Elasticsearch connection failed")


@app.on_event("shutdown")
async def shutdown_event():
    """서비스 종료 시 정리"""
    print("Targeting service shutting down")
