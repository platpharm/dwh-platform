"""수요예측 차트 페이지"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from typing import List, Dict, Any
from datetime import datetime
from elasticsearch.exceptions import NotFoundError

from shared.clients.es_client import es_client
from shared.config import ESIndex


def fetch_forecast_data(
    product_id: str = None,
    model_type: str = None,
    limit: int = 100
) -> List[Dict[str, Any]]:
    """ES에서 수요예측 결과 조회"""
    must_clauses = []

    if product_id:
        must_clauses.append({"term": {"product_id": product_id}})
    if model_type:
        must_clauses.append({"term": {"model_type": model_type}})

    if must_clauses:
        query = {"bool": {"must": must_clauses}}
    else:
        query = {"match_all": {}}

    try:
        results = es_client.search(
            index=ESIndex.FORECASTING_RESULT,
            query=query,
            size=limit,
        )
        return results
    except NotFoundError:
        st.error(
            f"인덱스 '{ESIndex.FORECASTING_RESULT}'가 존재하지 않습니다. "
            "수요예측 파이프라인을 먼저 실행하여 데이터를 생성해주세요."
        )
        return []
    except Exception as e:
        st.error(f"데이터 조회 실패: {e}")
        return []


def create_forecast_chart(df: pd.DataFrame) -> go.Figure:
    """시계열 예측 그래프 생성 (신뢰구간 포함)"""
    fig = go.Figure()

    if "actual" in df.columns:
        fig.add_trace(go.Scatter(
            x=df["date"],
            y=df["actual"],
            mode="lines+markers",
            name="실제값",
            line=dict(color="blue", width=2),
            marker=dict(size=6),
        ))

    fig.add_trace(go.Scatter(
        x=df["date"],
        y=df["forecast"],
        mode="lines+markers",
        name="예측값",
        line=dict(color="red", width=2, dash="dash"),
        marker=dict(size=6),
    ))

    if "upper_bound" in df.columns and "lower_bound" in df.columns:
        fig.add_trace(go.Scatter(
            x=df["date"],
            y=df["upper_bound"],
            mode="lines",
            name="95% 신뢰구간 상한",
            line=dict(width=0),
            showlegend=False,
        ))

        fig.add_trace(go.Scatter(
            x=df["date"],
            y=df["lower_bound"],
            mode="lines",
            name="95% 신뢰구간",
            line=dict(width=0),
            fill="tonexty",
            fillcolor="rgba(255, 0, 0, 0.1)",
        ))

    fig.update_layout(
        title="수요예측 시계열 차트",
        xaxis_title="날짜",
        yaxis_title="수요량",
        height=500,
        hovermode="x unified",
        legend=dict(
            yanchor="top",
            y=0.99,
            xanchor="left",
            x=0.01,
        ),
    )

    return fig


def create_error_metrics_chart(df: pd.DataFrame) -> go.Figure:
    """예측 오차 메트릭 시각화"""
    if "actual" not in df.columns or "forecast" not in df.columns:
        return None

    df = df.copy()
    df["error"] = df["forecast"] - df["actual"]
    df["abs_error"] = df["error"].abs()
    df["pct_error"] = (df["abs_error"] / df["actual"].replace(0, 1)) * 100

    fig = go.Figure()

    fig.add_trace(go.Bar(
        x=df["date"],
        y=df["error"],
        name="예측 오차",
        marker_color=df["error"].apply(lambda x: "green" if x >= 0 else "red"),
    ))

    fig.update_layout(
        title="예측 오차 분포",
        xaxis_title="날짜",
        yaxis_title="오차 (예측 - 실제)",
        height=300,
    )

    return fig


def calculate_metrics(df: pd.DataFrame) -> Dict[str, float]:
    """예측 정확도 메트릭 계산"""
    if "actual" not in df.columns or "forecast" not in df.columns:
        return {}

    actual = df["actual"]
    forecast = df["forecast"]

    mae = abs(forecast - actual).mean()
    mse = ((forecast - actual) ** 2).mean()
    rmse = mse ** 0.5

    mask = actual != 0
    if mask.any():
        mape = (abs((actual[mask] - forecast[mask]) / actual[mask])).mean() * 100
    else:
        mape = None

    return {
        "MAE": mae,
        "MSE": mse,
        "RMSE": rmse,
        "MAPE (%)": mape,
    }


def render_page():
    """수요예측 차트 페이지 렌더링"""
    st.title("수요예측 차트")
    st.markdown("---")

    col1, col2, col3 = st.columns([1, 1, 1])

    with col1:
        product_id = st.text_input("상품 ID", placeholder="예: PROD001")

    with col2:
        model_type = st.selectbox(
            "모델 타입",
            ["", "moving_average", "prophet", "arima"],
            format_func=lambda x: "전체" if x == "" else x,
        )

    with col3:
        data_limit = st.number_input("조회 수", min_value=10, max_value=500, value=100)

    if st.button("예측 결과 조회", type="primary"):
        st.session_state["forecast_data_loaded"] = True
        st.session_state["forecast_product_id"] = product_id
        st.session_state["forecast_model_type"] = model_type
        st.session_state["forecast_limit"] = data_limit

    if st.session_state.get("forecast_data_loaded", False):
        with st.spinner("예측 데이터 로딩 중..."):
            data = fetch_forecast_data(
                product_id=st.session_state.get("forecast_product_id"),
                model_type=st.session_state.get("forecast_model_type"),
                limit=st.session_state.get("forecast_limit", 100),
            )

        if data:
            df = pd.DataFrame(data)

            if df.empty:
                st.info("조회된 데이터가 없습니다.")
                st.stop()

            required_cols = ["forecast_date", "predicted_demand"]
            missing_cols = [col for col in required_cols if col not in df.columns]

            if missing_cols:
                st.error(f"필수 필드 누락: {missing_cols}. 수요예측 파이프라인을 먼저 실행해주세요.")
                st.stop()

            df.rename(columns={
                "forecast_date": "date",
                "predicted_demand": "forecast",
            }, inplace=True)

            if "date" in df.columns:
                df["date"] = pd.to_datetime(df["date"])
                df = df.sort_values("date")

            st.subheader("시계열 예측 그래프")
            forecast_fig = create_forecast_chart(df)
            st.plotly_chart(forecast_fig, use_container_width=True)

            col1, col2 = st.columns([1, 2])

            with col1:
                st.subheader("예측 정확도 메트릭")
                metrics = calculate_metrics(df)
                if metrics:
                    for metric_name, value in metrics.items():
                        if value is not None:
                            st.metric(metric_name, f"{value:.2f}")
                else:
                    st.info("실제값 데이터가 없어 메트릭을 계산할 수 없습니다.")

            with col2:
                error_fig = create_error_metrics_chart(df)
                if error_fig:
                    st.plotly_chart(error_fig, use_container_width=True)

            st.subheader("예측 데이터 상세")
            with st.expander("데이터 테이블 보기"):
                display_cols = ["date", "actual", "forecast", "upper_bound", "lower_bound"]
                available_cols = [col for col in display_cols if col in df.columns]
                st.dataframe(df[available_cols], use_container_width=True)

            st.subheader("요약 통계")
            col1, col2, col3, col4 = st.columns(4)

            with col1:
                st.metric("예측 기간", f"{len(df)}일")
            with col2:
                if "forecast" in df.columns:
                    st.metric("평균 예측량", f"{df['forecast'].mean():.1f}")
            with col3:
                if "forecast" in df.columns:
                    st.metric("최대 예측량", f"{df['forecast'].max():.1f}")
            with col4:
                if "forecast" in df.columns:
                    st.metric("최소 예측량", f"{df['forecast'].min():.1f}")

        else:
            st.info("조회된 데이터가 없습니다. ES 인덱스를 확인해주세요.")
    else:
        st.info("'예측 결과 조회' 버튼을 클릭하여 수요예측 결과를 확인하세요.")


if __name__ == "__main__":
    render_page()
