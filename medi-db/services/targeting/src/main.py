
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.api.endpoints import router
from shared.clients.es_client import es_client

@asynccontextmanager
async def lifespan(app: FastAPI):
    if es_client.health_check():
        print("Elasticsearch connection established")
    else:
        print("Warning: Elasticsearch connection failed")
    yield
    print("Targeting service shutting down")

app = FastAPI(
    title="MediDB Targeting Service",
    description="의약품-약국 타겟팅 매칭 서비스",
    version="0.1.0",
    lifespan=lifespan,
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(router)
