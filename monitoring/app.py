"""
DWH Platform Monitoring Service

모든 서비스의 상태를 모니터링하고 대시보드를 제공합니다.
"""

import asyncio
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

import httpx
from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import psycopg2

load_dotenv()

app = FastAPI(
    title="DWH Platform Monitor",
    description="MediTrend & Pharm-Clustering 통합 모니터링",
    version="1.0.0",
)


class ServiceStatus(BaseModel):
    name: str
    status: str  # healthy, unhealthy, unknown
    latency_ms: Optional[float] = None
    last_check: str
    details: Optional[Dict[str, Any]] = None


class SystemHealth(BaseModel):
    overall: str
    services: List[ServiceStatus]
    elasticsearch: ServiceStatus
    postgresql: ServiceStatus
    timestamp: str


# MediTrend 서비스 목록
MEDITREND_SERVICES = {
    "crawler": "http://meditrend-crawler:8000",
    "preprocessor": "http://meditrend-preprocessor:8000",
    "clustering": "http://meditrend-clustering:8000",
    "forecasting": "http://meditrend-forecasting:8000",
    "targeting": "http://meditrend-targeting:8000",
    "dashboard": "http://meditrend-dashboard:8501",
    "airflow": "http://airflow-webserver:8080",
}

# 환경 변수
ES_HOST = os.getenv("ES_HOST", "host.docker.internal")
ES_PORT = int(os.getenv("ES_PORT", "54321"))
ES_USERNAME = os.getenv("ES_USERNAME", "")
ES_PASSWORD = os.getenv("ES_PASSWORD", "")

PG_HOST = os.getenv("PG_HOST", "host.docker.internal")
PG_PORT = int(os.getenv("PG_PORT", "12345"))
PG_DATABASE = os.getenv("PG_DATABASE", "platpharm")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "")


async def check_service(name: str, url: str) -> ServiceStatus:
    """HTTP 서비스 상태 확인"""
    start = datetime.now()
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            # 서비스별 health endpoint
            health_url = f"{url}/health" if "8501" not in url else url
            response = await client.get(health_url)
            latency = (datetime.now() - start).total_seconds() * 1000

            return ServiceStatus(
                name=name,
                status="healthy" if response.status_code == 200 else "unhealthy",
                latency_ms=round(latency, 2),
                last_check=datetime.now().isoformat(),
                details={"status_code": response.status_code},
            )
    except Exception as e:
        return ServiceStatus(
            name=name,
            status="unhealthy",
            last_check=datetime.now().isoformat(),
            details={"error": str(e)},
        )


def check_elasticsearch() -> ServiceStatus:
    """Elasticsearch 연결 상태 확인"""
    start = datetime.now()
    try:
        auth = (ES_USERNAME, ES_PASSWORD) if ES_USERNAME and ES_PASSWORD else None
        es = Elasticsearch(
            hosts=[{"host": ES_HOST, "port": ES_PORT, "scheme": "http"}],
            basic_auth=auth,
            request_timeout=5,
        )

        if es.ping():
            latency = (datetime.now() - start).total_seconds() * 1000
            info = es.info()
            return ServiceStatus(
                name="elasticsearch",
                status="healthy",
                latency_ms=round(latency, 2),
                last_check=datetime.now().isoformat(),
                details={
                    "cluster_name": info.get("cluster_name"),
                    "version": info.get("version", {}).get("number"),
                },
            )
        else:
            return ServiceStatus(
                name="elasticsearch",
                status="unhealthy",
                last_check=datetime.now().isoformat(),
                details={"error": "Ping failed"},
            )
    except Exception as e:
        return ServiceStatus(
            name="elasticsearch",
            status="unhealthy",
            last_check=datetime.now().isoformat(),
            details={"error": str(e)},
        )


def check_postgresql() -> ServiceStatus:
    """PostgreSQL 연결 상태 확인"""
    start = datetime.now()
    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            database=PG_DATABASE,
            user=PG_USER,
            password=PG_PASSWORD,
            connect_timeout=5,
        )
        latency = (datetime.now() - start).total_seconds() * 1000

        cursor = conn.cursor()
        cursor.execute("SELECT version()")
        version = cursor.fetchone()[0]
        cursor.close()
        conn.close()

        return ServiceStatus(
            name="postgresql",
            status="healthy",
            latency_ms=round(latency, 2),
            last_check=datetime.now().isoformat(),
            details={"version": version[:50]},
        )
    except Exception as e:
        return ServiceStatus(
            name="postgresql",
            status="unhealthy",
            last_check=datetime.now().isoformat(),
            details={"error": str(e)},
        )


@app.get("/health")
async def health():
    """서비스 헬스체크"""
    return {"status": "healthy", "service": "monitoring"}


@app.get("/", response_model=SystemHealth)
async def get_system_health():
    """전체 시스템 상태 조회"""
    # 모든 서비스 상태 확인 (병렬)
    service_checks = [
        check_service(name, url) for name, url in MEDITREND_SERVICES.items()
    ]
    services = await asyncio.gather(*service_checks)

    # DB 상태 확인
    es_status = check_elasticsearch()
    pg_status = check_postgresql()

    # 전체 상태 판단
    all_statuses = [s.status for s in services] + [es_status.status, pg_status.status]
    if all(s == "healthy" for s in all_statuses):
        overall = "healthy"
    elif any(s == "healthy" for s in all_statuses):
        overall = "degraded"
    else:
        overall = "unhealthy"

    return SystemHealth(
        overall=overall,
        services=list(services),
        elasticsearch=es_status,
        postgresql=pg_status,
        timestamp=datetime.now().isoformat(),
    )


@app.get("/services/{service_name}")
async def get_service_status(service_name: str):
    """개별 서비스 상태 조회"""
    if service_name not in MEDITREND_SERVICES:
        raise HTTPException(status_code=404, detail=f"Service '{service_name}' not found")

    return await check_service(service_name, MEDITREND_SERVICES[service_name])


@app.get("/es/indices")
async def get_es_indices():
    """Elasticsearch 인덱스 목록"""
    try:
        auth = (ES_USERNAME, ES_PASSWORD) if ES_USERNAME and ES_PASSWORD else None
        es = Elasticsearch(
            hosts=[{"host": ES_HOST, "port": ES_PORT, "scheme": "http"}],
            basic_auth=auth,
        )

        indices = es.cat.indices(format="json")
        return {
            "count": len(indices),
            "indices": [
                {
                    "name": idx["index"],
                    "docs": idx.get("docs.count", "0"),
                    "size": idx.get("store.size", "0"),
                    "health": idx.get("health", "unknown"),
                }
                for idx in indices
                if not idx["index"].startswith(".")
            ],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8888)
