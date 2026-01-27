"""인기 의약품 랭킹 뷰 페이지"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from typing import List, Dict, Any, Optional

from elasticsearch.exceptions import NotFoundError

from shared.clients.es_client import es_client
from shared.config import ESIndex


SALES_WEIGHT = 0.6
PRODUCT_TREND_WEIGHT = 0.3
CATEGORY_TREND_WEIGHT = 0.1


def fetch_ranking_data(
    category: str = None,
    limit: int = 100
) -> List[Dict[str, Any]]:
    """ES에서 랭킹 결과 조회"""
    if category and category != "전체":
        query = {"term": {"category2": category}}
    else:
        query = {"match_all": {}}

    try:
        results = es_client.search(
            index=ESIndex.RANKING_RESULT,
            query=query,
            size=limit,
        )
        return results
    except NotFoundError:
        st.error(
            f"인덱스 '{ESIndex.RANKING_RESULT}'가 존재하지 않습니다. "
            "랭킹 파이프라인을 먼저 실행하여 인덱스를 생성해주세요."
        )
        return []
    except Exception as e:
        st.error(f"데이터 조회 실패: {e}")
        return []


def fetch_categories() -> List[str]:
    """카테고리 목록 조회"""
    try:
        response = es_client.client.search(
            index=ESIndex.RANKING_RESULT,
            body={
                "size": 0,
                "aggs": {
                    "categories": {
                        "terms": {
                            "field": "category2",
                            "size": 100,
                        }
                    }
                }
            }
        )
        buckets = response.get("aggregations", {}).get("categories", {}).get("buckets", [])
        return ["전체"] + [bucket["key"] for bucket in buckets]
    except Exception:
        return ["전체"]


def calculate_weighted_score(
    sales_score: float,
    product_trend_score: float,
    category_trend_score: float,
) -> float:
    """가중치 점수 계산 (판매량 60% + 상품명 트렌드 30% + 카테고리 트렌드 10%)"""
    return (
        (sales_score * SALES_WEIGHT)
        + (product_trend_score * PRODUCT_TREND_WEIGHT)
        + (category_trend_score * CATEGORY_TREND_WEIGHT)
    )


def create_ranking_table(df: pd.DataFrame) -> pd.DataFrame:
    """랭킹 테이블 생성"""
    if "product_trend_score" not in df.columns:
        df["product_trend_score"] = 0.0
    if "category_trend_score" not in df.columns:
        df["category_trend_score"] = 0.0

    if "sales_score" in df.columns:
        df["weighted_score"] = df.apply(
            lambda row: calculate_weighted_score(
                row["sales_score"],
                row["product_trend_score"],
                row["category_trend_score"],
            ),
            axis=1
        )
    elif "weighted_score" not in df.columns:
        df["weighted_score"] = 0

    df = df.sort_values("weighted_score", ascending=False).reset_index(drop=True)
    df["rank"] = range(1, len(df) + 1)

    return df


def create_ranking_bar_chart(df: pd.DataFrame, top_n: int = 20) -> Optional[go.Figure]:
    """상위 N개 랭킹 바 차트"""
    if df.empty or "product_name" not in df.columns:
        return None

    top_df = df.head(top_n)

    fig = go.Figure()

    fig.add_trace(go.Bar(
        y=top_df["product_name"],
        x=top_df["sales_score"] * SALES_WEIGHT if "sales_score" in top_df.columns else [0] * len(top_df),
        name=f"판매량 ({int(SALES_WEIGHT * 100)}%)",
        orientation="h",
        marker_color="steelblue",
    ))

    fig.add_trace(go.Bar(
        y=top_df["product_name"],
        x=top_df["product_trend_score"] * PRODUCT_TREND_WEIGHT if "product_trend_score" in top_df.columns else [0] * len(top_df),
        name=f"상품명 트렌드 ({int(PRODUCT_TREND_WEIGHT * 100)}%)",
        orientation="h",
        marker_color="coral",
    ))

    fig.add_trace(go.Bar(
        y=top_df["product_name"],
        x=top_df["category_trend_score"] * CATEGORY_TREND_WEIGHT if "category_trend_score" in top_df.columns else [0] * len(top_df),
        name=f"카테고리 트렌드 ({int(CATEGORY_TREND_WEIGHT * 100)}%)",
        orientation="h",
        marker_color="mediumpurple",
    ))

    fig.update_layout(
        title=f"인기 의약품 TOP {top_n}",
        xaxis_title="가중치 점수",
        yaxis_title="상품명",
        barmode="stack",
        height=max(400, top_n * 25),
        yaxis=dict(autorange="reversed"),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
        ),
    )

    return fig


def create_score_comparison_chart(df: pd.DataFrame, top_n: int = 10) -> Optional[go.Figure]:
    """판매량 vs 상품명 트렌드 점수 비교 산점도"""
    if df.empty or "sales_score" not in df.columns or "product_trend_score" not in df.columns:
        return None

    top_df = df.head(top_n)
    if top_df.empty:
        return None

    fig = px.scatter(
        top_df,
        x="sales_score",
        y="product_trend_score",
        size="weighted_score",
        color="category2" if "category2" in top_df.columns else None,
        hover_data=["product_name", "rank", "category_trend_score"],
        title="판매량 vs 상품명 트렌드 점수 비교",
    )

    fig.update_layout(
        xaxis_title="판매량 점수",
        yaxis_title="상품명 트렌드 점수",
        height=400,
    )

    fig.add_hline(y=top_df["product_trend_score"].mean(), line_dash="dash", line_color="gray")
    fig.add_vline(x=top_df["sales_score"].mean(), line_dash="dash", line_color="gray")

    return fig


def create_category_ranking_chart(df: pd.DataFrame) -> Optional[go.Figure]:
    """카테고리별 랭킹 분포"""
    if df.empty or "category2" not in df.columns:
        return None

    category_stats = df.groupby("category2").agg(
        count=("product_id", "count"),
        avg_score=("weighted_score", "mean"),
    ).reset_index()

    fig = px.bar(
        category_stats,
        x="category2",
        y="avg_score",
        color="count",
        title="카테고리별 평균 랭킹 점수",
        color_continuous_scale="Blues",
    )

    fig.update_layout(
        xaxis_title="카테고리",
        yaxis_title="평균 가중치 점수",
        height=400,
    )

    return fig


def render_page():
    """인기 의약품 랭킹 페이지 렌더링"""
    st.title("인기 의약품 랭킹")
    st.markdown("---")

    st.info(f"랭킹 산출 기준: 판매량 {int(SALES_WEIGHT * 100)}% + 상품명 트렌드 {int(PRODUCT_TREND_WEIGHT * 100)}% + 카테고리 트렌드 {int(CATEGORY_TREND_WEIGHT * 100)}%")

    col1, col2, col3 = st.columns([1, 1, 1])

    with col1:
        categories = fetch_categories()
        selected_category = st.selectbox("카테고리", categories)

    with col2:
        top_n = st.slider("표시 개수", 10, 100, 20, step=5)

    with col3:
        data_limit = st.number_input("조회 수", min_value=50, max_value=1000, value=200)

    if st.button("랭킹 조회", type="primary"):
        st.session_state["ranking_data_loaded"] = True
        st.session_state["ranking_category"] = selected_category
        st.session_state["ranking_top_n"] = top_n
        st.session_state["ranking_limit"] = data_limit

    if st.session_state.get("ranking_data_loaded", False):
        with st.spinner("랭킹 데이터 로딩 중..."):
            data = fetch_ranking_data(
                category=st.session_state.get("ranking_category"),
                limit=st.session_state.get("ranking_limit", 200),
            )

        if data:
            df = pd.DataFrame(data)

            required_cols = ["product_id", "product_name"]
            missing_cols = [col for col in required_cols if col not in df.columns]

            if missing_cols:
                st.error(f"필수 필드 누락: {missing_cols}. 랭킹 파이프라인을 먼저 실행해주세요.")
                st.stop()

            df = create_ranking_table(df)
            top_n_display = st.session_state.get("ranking_top_n", 20)

            tab1, tab2, tab3 = st.tabs(["전체 랭킹", "카테고리별 랭킹", "점수 분석"])

            with tab1:
                st.subheader("전체 랭킹 테이블")

                bar_fig = create_ranking_bar_chart(df, top_n_display)
                if bar_fig:
                    st.plotly_chart(bar_fig, use_container_width=True)
                else:
                    st.warning("차트를 생성할 데이터가 부족합니다.")

                display_cols = ["rank", "product_name", "category2", "sales_score", "product_trend_score", "category_trend_score", "weighted_score"]
                available_cols = [col for col in display_cols if col in df.columns]
                format_dict = {
                    col: "{:.2f}" for col in ["sales_score", "product_trend_score", "category_trend_score", "weighted_score"]
                    if col in available_cols
                }

                st.dataframe(
                    df[available_cols].head(top_n_display).style.format(format_dict),
                    use_container_width=True,
                )

            with tab2:
                st.subheader("카테고리별 랭킹")

                if "category2" in df.columns:
                    cat_fig = create_category_ranking_chart(df)
                    if cat_fig:
                        st.plotly_chart(cat_fig, use_container_width=True)

                    st.subheader("카테고리별 TOP 5")
                    for category in df["category2"].unique()[:5]:
                        with st.expander(f"{category}"):
                            cat_df = df[df["category2"] == category].head(5)
                            st.dataframe(
                                cat_df[available_cols].style.format(format_dict),
                                use_container_width=True,
                            )
                else:
                    st.info("카테고리2 정보가 없습니다.")

            with tab3:
                st.subheader("점수 분석")

                scatter_fig = create_score_comparison_chart(df, top_n_display)
                if scatter_fig:
                    st.plotly_chart(scatter_fig, use_container_width=True)

                col1, col2 = st.columns(2)

                with col1:
                    if "sales_score" in df.columns:
                        fig = px.histogram(
                            df,
                            x="sales_score",
                            title="판매량 점수 분포",
                            nbins=20,
                        )
                        st.plotly_chart(fig, use_container_width=True)

                with col2:
                    if "product_trend_score" in df.columns:
                        fig = px.histogram(
                            df,
                            x="product_trend_score",
                            title="상품명 트렌드 점수 분포",
                            nbins=20,
                        )
                        st.plotly_chart(fig, use_container_width=True)

                st.subheader("요약 통계")
                col1, col2, col3, col4 = st.columns(4)

                with col1:
                    st.metric("전체 상품 수", len(df))
                with col2:
                    if "weighted_score" in df.columns:
                        st.metric("평균 점수", f"{df['weighted_score'].mean():.2f}")
                with col3:
                    if "weighted_score" in df.columns:
                        st.metric("최고 점수", f"{df['weighted_score'].max():.2f}")
                with col4:
                    if "category2" in df.columns:
                        st.metric("카테고리 수", df["category2"].nunique())

        else:
            st.info("조회된 데이터가 없습니다. ES 인덱스를 확인해주세요.")
    else:
        st.info("'랭킹 조회' 버튼을 클릭하여 인기 의약품 랭킹을 확인하세요.")


if __name__ == "__main__":
    render_page()
