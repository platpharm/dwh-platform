"""클러스터링 결과 시각화 페이지"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from typing import List, Dict, Any

from shared.clients.es_client import es_client
from shared.config import ESIndex


def fetch_clustering_data(cluster_type: str = "all", limit: int = 1000) -> List[Dict[str, Any]]:
    """ES에서 클러스터링 결과 조회"""
    if cluster_type == "all":
        query = {"match_all": {}}
    else:
        query = {"term": {"entity_type": cluster_type}}

    try:
        results = es_client.search(
            index=ESIndex.CLUSTERING_RESULT,
            query=query,
            size=limit,
        )
        return results
    except Exception as e:
        st.error(f"데이터 조회 실패: {e}")
        return []


def create_umap_scatter(df: pd.DataFrame) -> go.Figure:
    """UMAP 2D 산점도 생성"""
    fig = px.scatter(
        df,
        x="umap_x",
        y="umap_y",
        color="cluster_id",
        hover_data=["entity_id", "entity_name", "cluster_id"],
        title="UMAP 2D 클러스터링 시각화",
        color_discrete_sequence=px.colors.qualitative.Set2,
    )

    fig.update_layout(
        xaxis_title="UMAP 1",
        yaxis_title="UMAP 2",
        legend_title="클러스터",
        height=600,
    )

    fig.update_traces(marker=dict(size=8, opacity=0.7))

    return fig


def create_cluster_distribution(df: pd.DataFrame, group_by: str) -> go.Figure:
    """클러스터별 분포 차트 생성"""
    distribution = df.groupby(["cluster_id", group_by]).size().reset_index(name="count")

    fig = px.bar(
        distribution,
        x="cluster_id",
        y="count",
        color=group_by,
        title=f"클러스터별 {group_by} 분포",
        barmode="stack",
    )

    fig.update_layout(
        xaxis_title="클러스터 ID",
        yaxis_title="개수",
        height=400,
    )

    return fig


def create_cluster_stats_table(df: pd.DataFrame) -> pd.DataFrame:
    """클러스터별 통계 테이블 생성"""
    agg_dict = {"entity_id": ("entity_id", "count")}
    if "score" in df.columns:
        agg_dict["avg_score"] = ("score", "mean")

    stats = df.groupby("cluster_id").agg(**agg_dict).reset_index()

    if "score" in df.columns:
        stats.columns = ["클러스터 ID", "항목 수", "평균 점수"]
    else:
        stats.columns = ["클러스터 ID", "항목 수"]
    return stats


def render_page():
    """클러스터링 시각화 페이지 렌더링"""
    st.title("클러스터링 결과 시각화")
    st.markdown("---")

    col1, col2 = st.columns([1, 3])

    with col1:
        cluster_type = st.selectbox(
            "클러스터 타입",
            ["all", "product", "pharmacy"],
            format_func=lambda x: {
                "all": "전체",
                "product": "상품",
                "pharmacy": "약국",
            }.get(x, x),
        )

        data_limit = st.slider("데이터 수", 100, 5000, 1000, step=100)

        if st.button("데이터 조회", type="primary"):
            st.session_state["clustering_data_loaded"] = True

    if st.session_state.get("clustering_data_loaded", False):
        with st.spinner("데이터 로딩 중..."):
            data = fetch_clustering_data(cluster_type, data_limit)

        if data:
            df = pd.DataFrame(data)

            required_cols = ["cluster_id", "entity_id", "entity_type"]
            missing_cols = [col for col in required_cols if col not in df.columns]

            if missing_cols:
                st.error(f"필수 필드 누락: {missing_cols}. 클러스터링 파이프라인을 먼저 실행해주세요.")
                st.stop()

            if "umap_coords" in df.columns:
                df["umap_x"] = df["umap_coords"].apply(lambda x: x[0] if x and len(x) > 0 else 0)
                df["umap_y"] = df["umap_coords"].apply(lambda x: x[1] if x and len(x) > 1 else 0)
            elif "umap_x" not in df.columns or "umap_y" not in df.columns:
                st.warning("UMAP 좌표가 없습니다. use_umap=True로 클러스터링을 다시 실행해주세요.")
                df["umap_x"] = 0
                df["umap_y"] = 0

            if "entity_name" not in df.columns:
                if "features" in df.columns:
                    df["entity_name"] = df["features"].apply(
                        lambda x: x.get("product_name") or x.get("name") or x.get("host_name") if isinstance(x, dict) else None
                    )
                else:
                    df["entity_name"] = None

            # entity_name이 없는 행은 entity_id로 대체
            df["entity_name"] = df["entity_name"].fillna(df["entity_id"].astype(str))
            df.loc[df["entity_name"] == "", "entity_name"] = df.loc[df["entity_name"] == "", "entity_id"].astype(str)

            st.subheader("UMAP 2D 산점도")
            has_umap = not (df["umap_x"].eq(0).all() and df["umap_y"].eq(0).all())
            if has_umap:
                umap_fig = create_umap_scatter(df)
                st.plotly_chart(umap_fig, use_container_width=True)
            else:
                st.info("UMAP 좌표 데이터가 없어 산점도를 표시할 수 없습니다. use_umap=True로 클러스터링을 다시 실행해주세요.")

            col1, col2 = st.columns(2)

            with col1:
                st.subheader("클러스터별 분포")
                if "entity_type" in df.columns:
                    dist_fig = create_cluster_distribution(df, "entity_type")
                    st.plotly_chart(dist_fig, use_container_width=True)
                else:
                    cluster_counts = df["cluster_id"].value_counts().reset_index()
                    cluster_counts.columns = ["cluster_id", "count"]
                    fig = px.pie(
                        cluster_counts,
                        values="count",
                        names="cluster_id",
                        title="클러스터별 비율",
                    )
                    st.plotly_chart(fig, use_container_width=True)

            with col2:
                st.subheader("클러스터 통계")
                stats_df = create_cluster_stats_table(df)
                st.dataframe(stats_df, use_container_width=True)

            st.subheader("상세 데이터")
            with st.expander("데이터 테이블 보기"):
                st.dataframe(df, use_container_width=True)

        else:
            st.info("조회된 데이터가 없습니다. ES 인덱스를 확인해주세요.")
    else:
        st.info("'데이터 조회' 버튼을 클릭하여 클러스터링 결과를 확인하세요.")


if __name__ == "__main__":
    render_page()
