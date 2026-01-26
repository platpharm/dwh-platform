"""FastAPI 애플리케이션 엔트리포인트"""
import logging
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.api.endpoints import router

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# FastAPI 앱 생성
app = FastAPI(
    title="TipsDips Preprocessor Service",
    description="의약품 데이터 전처리 및 키워드 추출 서비스",
    version="1.0.0",
)

# CORS 미들웨어 추가
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
    """서비스 시작 시 실행"""
    logger.info("TipsDips Preprocessor Service started")


@app.on_event("shutdown")
async def shutdown_event():
    """서비스 종료 시 실행"""
    logger.info("TipsDips Preprocessor Service stopped")
