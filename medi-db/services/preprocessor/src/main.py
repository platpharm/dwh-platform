import logging
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.api.endpoints import router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="MediDB Preprocessor Service",
    description="의약품 데이터 전처리 및 키워드 추출 서비스",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)

@app.on_event("startup")
async def startup_event():
    logger.info("MediDB Preprocessor Service started")

@app.on_event("shutdown")
async def shutdown_event():
    logger.info("MediDB Preprocessor Service stopped")
