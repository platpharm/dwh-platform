
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query

from shared.clients.es_client import es_client
from shared.models.schemas import (
    TargetRequest,
    TargetResponse,
    HealthResponse,
)
from src.matcher.product_pharmacy_matcher import product_pharmacy_matcher

router = APIRouter()

@router.post("/target/run", response_model=TargetResponse)
async def run_targeting(request: TargetRequest) -> TargetResponse:
    try:
        result = product_pharmacy_matcher.run_targeting(
            top_n_products=request.top_n_products,
            top_n_pharmacies=request.top_n_pharmacies
        )

        return TargetResponse(
            success=True,
            match_count=result["total_matches"],
            message=f"타겟팅 완료: {result['saved_count']}건 저장, {result['error_count']}건 오류"
        )

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"타겟팅 실행 중 오류 발생: {str(e)}"
        )

@router.get("/target/results")
async def get_targeting_results(
    limit: int = Query(default=100, ge=1, le=1000, description="최대 결과 수")
) -> Dict[str, Any]:
    try:
        results = product_pharmacy_matcher.get_targeting_results(limit=limit)

        return {
            "success": True,
            "count": len(results),
            "results": results
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"결과 조회 중 오류 발생: {str(e)}"
        )

@router.get("/target/product/{product_id}")
async def get_target_pharmacies(
    product_id: int,
    limit: int = Query(default=50, ge=1, le=500, description="최대 결과 수")
) -> Dict[str, Any]:
    try:
        results = product_pharmacy_matcher.get_targeting_results(
            product_id=product_id,
            limit=limit
        )

        if not results:
            return {
                "success": True,
                "product_id": product_id,
                "count": 0,
                "pharmacies": [],
                "message": "해당 상품에 대한 타겟팅 결과가 없습니다"
            }

        pharmacies = [
            {
                "pharmacy_id": r["pharmacy_id"],
                "pharmacy_name": r.get("pharmacy_name"),
                "pharmacy_address": r.get("pharmacy_address"),
                "match_score": r["match_score"],
                "pharmacy_cluster_id": r.get("pharmacy_cluster_id")
            }
            for r in results
        ]

        return {
            "success": True,
            "product_id": product_id,
            "product_name": results[0].get("product_name") if results else None,
            "product_cluster_id": results[0].get("product_cluster_id") if results else None,
            "ranking_score": results[0].get("ranking_score") if results else None,
            "count": len(pharmacies),
            "pharmacies": pharmacies
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"타겟 약국 조회 중 오류 발생: {str(e)}"
        )

@router.get("/target/pharmacy/{pharmacy_id}")
async def get_recommended_products(
    pharmacy_id: int,
    limit: int = Query(default=50, ge=1, le=500, description="최대 결과 수")
) -> Dict[str, Any]:
    try:
        results = product_pharmacy_matcher.get_targeting_results(
            pharmacy_id=pharmacy_id,
            limit=limit
        )

        if not results:
            return {
                "success": True,
                "pharmacy_id": pharmacy_id,
                "count": 0,
                "products": [],
                "message": "해당 약국에 대한 추천 상품이 없습니다"
            }

        products = [
            {
                "product_id": r["product_id"],
                "product_name": r.get("product_name"),
                "match_score": r["match_score"],
                "product_cluster_id": r.get("product_cluster_id"),
                "ranking_score": r.get("ranking_score")
            }
            for r in results
        ]

        return {
            "success": True,
            "pharmacy_id": pharmacy_id,
            "pharmacy_name": results[0].get("pharmacy_name") if results else None,
            "pharmacy_cluster_id": results[0].get("pharmacy_cluster_id") if results else None,
            "count": len(products),
            "products": products
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"추천 상품 조회 중 오류 발생: {str(e)}"
        )

@router.get("/health", response_model=HealthResponse)
async def health_check() -> HealthResponse:
    es_healthy = es_client.health_check()

    dependencies = {
        "elasticsearch": es_healthy
    }

    status = "healthy" if all(dependencies.values()) else "degraded"

    return HealthResponse(
        status=status,
        service="targeting",
        dependencies=dependencies
    )
