"""Forecasting API 엔드포인트"""
import logging
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from shared.models.schemas import (
    ForecastRequest,
    ForecastResponse,
    ForecastingResult,
    RankingResult,
)
from shared.config import ESIndex
from shared.clients.es_client import es_client
from src.models.demand_forecast import DemandForecaster
from src.models.ranking_calculator import RankingCalculator
from src.models.trend_extractor import TrendExtractor
from src.models.vector_generator import VectorGenerator

logger = logging.getLogger(__name__)

router = APIRouter()


class RankingRequest(BaseModel):
    """랭킹 계산 요청"""
    days: int = 30
    category: Optional[str] = None
    top_n: Optional[int] = None
    sales_weight: float = 0.6
    product_trend_weight: float = 0.3
    category_trend_weight: float = 0.1


class RankingResponse(BaseModel):
    """랭킹 계산 응답"""
    success: bool
    ranking_count: int
    message: str


class ForecastResultResponse(BaseModel):
    """예측 결과 조회 응답"""
    count: int
    results: List[ForecastingResult]


class RankingResultResponse(BaseModel):
    """랭킹 결과 조회 응답"""
    count: int
    results: List[RankingResult]


class VectorRequest(BaseModel):
    """벡터 생성 요청"""
    entity_type: str = "product"  # "product" or "pharmacy"
    days: int = 90
    trend_weight: float = 0.3


class VectorResponse(BaseModel):
    """벡터 생성 응답"""
    success: bool
    vector_count: int
    message: str


@router.post("/forecast/run", response_model=ForecastResponse, tags=["Forecast"])
async def run_forecast(request: ForecastRequest):
    """수요예측 실행

    시계열 분석을 통해 상품별 미래 수요를 예측합니다.
    """
    try:
        forecaster = DemandForecaster()

        results = forecaster.run_forecast(
            product_ids=request.product_ids,
            days_ahead=request.days_ahead,
            model_type="auto"
        )

        if not results:
            return ForecastResponse(
                success=True,
                forecast_count=0,
                message="No products to forecast. Check if historical data exists."
            )

        success_count, failed_count = forecaster.save_results(results)

        return ForecastResponse(
            success=True,
            forecast_count=success_count,
            message=f"Forecast completed. {success_count} products forecasted."
        )

    except Exception as e:
        logger.error(f"Forecast failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/forecast/results", response_model=ForecastResultResponse, tags=["Forecast"])
async def get_forecast_results(
    product_id: Optional[int] = Query(None, description="상품 ID 필터"),
    limit: int = Query(100, description="조회 개수", ge=1, le=1000)
):
    """예측 결과 조회

    저장된 수요예측 결과를 조회합니다.
    """
    try:
        query = {"match_all": {}}

        if product_id is not None:
            query = {"term": {"product_id": product_id}}

        results = es_client.search(
            index=ESIndex.FORECASTING_RESULT,
            query=query,
            size=limit
        )

        forecast_results = []
        for r in results:
            try:
                forecast_results.append(ForecastingResult(**r))
            except Exception as e:
                logger.warning(f"Failed to parse forecast result: {e}")
                continue

        return ForecastResultResponse(
            count=len(forecast_results),
            results=forecast_results
        )

    except Exception as e:
        logger.error(f"Failed to get forecast results: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/forecast/ranking", response_model=RankingResponse, tags=["Ranking"])
async def run_ranking(request: RankingRequest):
    """인기 의약품 랭킹 계산

    판매량(60%) + 상품명 트렌드(30%) + 카테고리 트렌드(10%) 기반 랭킹
    """
    try:
        calculator = RankingCalculator()

        results = calculator.run_ranking(
            days=request.days,
            category=request.category,
            top_n=request.top_n,
            sales_weight=request.sales_weight,
            product_trend_weight=request.product_trend_weight,
            category_trend_weight=request.category_trend_weight,
        )

        if not results:
            return RankingResponse(
                success=True,
                ranking_count=0,
                message="No products to rank. Check if sales data exists."
            )

        success_count, failed_count = calculator.save_results(results)

        return RankingResponse(
            success=True,
            ranking_count=success_count,
            message=f"Ranking completed. {success_count} products ranked."
        )

    except Exception as e:
        logger.error(f"Ranking failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/ranking/results", response_model=RankingResultResponse, tags=["Ranking"])
async def get_ranking_results(
    category: Optional[str] = Query(None, description="카테고리 필터"),
    top_n: int = Query(100, description="상위 N개", ge=1, le=1000)
):
    """랭킹 결과 조회

    저장된 인기 의약품 랭킹 결과를 조회합니다.
    """
    try:
        if category:
            query = {"term": {"category2": category}}
        else:
            query = {"match_all": {}}

        body = {
            "query": query,
            "size": top_n,
            "sort": [{"rank": {"order": "asc"}}]
        }
        response = es_client.client.search(index=ESIndex.RANKING_RESULT, body=body)
        results = [hit["_source"] for hit in response["hits"]["hits"]]

        ranking_results = []
        for r in results:
            try:
                ranking_results.append(RankingResult(**r))
            except Exception as e:
                logger.warning(f"Failed to parse ranking result: {e}")
                continue

        return RankingResultResponse(
            count=len(ranking_results),
            results=ranking_results
        )

    except Exception as e:
        logger.error(f"Failed to get ranking results: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/trend/emerging", tags=["Trend"])
async def get_emerging_trends(
    days: int = Query(30, description="분석 기간"),
    min_momentum: float = Query(10.0, description="최소 모멘텀")
):
    """급상승 트렌드 조회

    최근 급상승 중인 트렌드 키워드를 조회합니다.
    """
    try:
        extractor = TrendExtractor()
        trends = extractor.extract_emerging_trends(
            days=days,
            min_momentum=min_momentum
        )

        return {
            "count": len(trends),
            "trends": trends
        }

    except Exception as e:
        logger.error(f"Failed to get emerging trends: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/trend/product/{product_id}", tags=["Trend"])
async def get_product_trend(product_id: int):
    """상품 트렌드 점수 조회

    특정 상품의 트렌드 점수를 조회합니다.
    """
    try:
        extractor = TrendExtractor()
        score = extractor.get_product_trend_score(product_id)

        return {
            "product_id": product_id,
            "trend_score": score
        }

    except Exception as e:
        logger.error(f"Failed to get product trend: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/vector/generate", response_model=VectorResponse, tags=["Vector"])
async def generate_vectors(request: VectorRequest):
    """트렌드+주문 벡터 생성

    상품 또는 약국의 특성 벡터를 생성합니다.
    """
    try:
        generator = VectorGenerator()

        if request.entity_type == "product":
            ids_df, vectors = generator.generate_combined_vectors(
                days=request.days,
                trend_weight=request.trend_weight
            )
        else:
            ids_df, vectors = generator.generate_pharmacy_vectors(
                days=request.days
            )

        if vectors.size == 0:
            return VectorResponse(
                success=True,
                vector_count=0,
                message="No data available for vector generation."
            )

        success_count, failed_count = generator.save_vectors(
            entity_type=request.entity_type,
            ids_df=ids_df,
            vectors=vectors
        )

        return VectorResponse(
            success=True,
            vector_count=success_count,
            message=f"Vector generation completed. {success_count} vectors created."
        )

    except Exception as e:
        logger.error(f"Vector generation failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/vector/similar/{product_id}", tags=["Vector"])
async def find_similar_products(
    product_id: int,
    top_n: int = Query(10, description="상위 N개", ge=1, le=100)
):
    """유사 상품 조회

    특정 상품과 유사한 상품을 조회합니다.
    """
    try:
        generator = VectorGenerator()
        similar = generator.find_similar_products(product_id, top_n=top_n)

        return {
            "product_id": product_id,
            "similar_products": similar
        }

    except Exception as e:
        logger.error(f"Failed to find similar products: {e}")
        raise HTTPException(status_code=500, detail=str(e))
