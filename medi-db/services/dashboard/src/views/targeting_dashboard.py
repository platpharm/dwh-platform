"""상품-약국 매칭 대시보드 페이지"""
import streamlit as st
import pandas as pd
import plotly.express as px
from typing import List, Dict, Any

from elasticsearch.exceptions import NotFoundError

from shared.clients.es_client import es_client
from shared.config import ESIndex


def fetch_products() -> List[Dict[str, str]]:
    """TARGETING_RESULT에서 상품 목록 조회 (중복 제거)"""
    try:
        response = es_client.client.search(
            index=ESIndex.TARGETING_RESULT,
            body={
                "size": 0,
                "aggs": {
                    "products": {
                        "terms": {
                            "field": "product_id",
                            "size": 500,
                        },
                        "aggs": {
                            "product_name": {
                                "terms": {
                                    "field": "product_name.keyword",
                                    "size": 1,
                                }
                            }
                        }
                    }
                }
            }
        )
        buckets = response.get("aggregations", {}).get("products", {}).get("buckets", [])
        products = []
        for bucket in buckets:
            name_buckets = bucket.get("product_name", {}).get("buckets", [])
            name = name_buckets[0].get("key", bucket["key"]) if name_buckets else bucket["key"]
            products.append({
                "id": bucket["key"],
                "name": name,
                "doc_count": bucket["doc_count"]
            })
        return products
    except NotFoundError:
        st.error(
            f"인덱스 '{ESIndex.TARGETING_RESULT}'를 찾을 수 없습니다. "
            "타겟팅 파이프라인을 먼저 실행하여 인덱스를 생성해주세요."
        )
        return []
    except Exception as e:
        st.error(f"상품 목록 조회 실패: {e}")
        return []


def fetch_matching_pharmacies(
    product_id: str,
    limit: int = 100,
    min_score: float = 0.0
) -> List[Dict[str, Any]]:
    """선택한 상품에 대한 추천 약국 리스트 조회 (match_score 높은 순)"""
    try:
        query = {
            "bool": {
                "must": [
                    {"term": {"product_id": product_id}}
                ],
                "filter": [
                    {"range": {"match_score": {"gte": min_score}}}
                ]
            }
        }

        response = es_client.client.search(
            index=ESIndex.TARGETING_RESULT,
            body={
                "query": query,
                "size": limit,
                "sort": [
                    {"match_score": {"order": "desc"}}
                ]
            }
        )

        return [hit["_source"] for hit in response["hits"]["hits"]]
    except NotFoundError:
        st.error(
            f"인덱스 '{ESIndex.TARGETING_RESULT}'를 찾을 수 없습니다. "
            "타겟팅 파이프라인을 먼저 실행하여 인덱스를 생성해주세요."
        )
        return []
    except Exception as e:
        st.error(f"약국 데이터 조회 실패: {e}")
        return []


def fetch_product_summary(product_id: str) -> Dict[str, Any]:
    """상품별 타겟팅 요약 통계"""
    try:
        response = es_client.client.search(
            index=ESIndex.TARGETING_RESULT,
            body={
                "query": {"term": {"product_id": product_id}},
                "size": 0,
                "aggs": {
                    "total_pharmacies": {"value_count": {"field": "pharmacy_id"}},
                    "avg_score": {"avg": {"field": "match_score"}},
                    "max_score": {"max": {"field": "match_score"}},
                    "min_score": {"min": {"field": "match_score"}},
                    "score_percentiles": {
                        "percentiles": {
                            "field": "match_score",
                            "percents": [25, 50, 75, 90]
                        }
                    }
                }
            }
        )

        aggs = response.get("aggregations", {})
        return {
            "total_pharmacies": aggs.get("total_pharmacies", {}).get("value", 0),
            "avg_score": aggs.get("avg_score", {}).get("value", 0),
            "max_score": aggs.get("max_score", {}).get("value", 0),
            "min_score": aggs.get("min_score", {}).get("value", 0),
            "percentiles": aggs.get("score_percentiles", {}).get("values", {})
        }
    except NotFoundError:
        st.error(
            f"인덱스 '{ESIndex.TARGETING_RESULT}'를 찾을 수 없습니다."
        )
        return {}
    except Exception as e:
        st.error(f"통계 조회 실패: {e}")
        return {}


def create_score_distribution_chart(df: pd.DataFrame) -> px.histogram:
    """매칭 점수 분포 히스토그램"""
    if "match_score" not in df.columns:
        return None

    fig = px.histogram(
        df,
        x="match_score",
        nbins=20,
        title="매칭 점수 분포",
        labels={"match_score": "매칭 점수", "count": "약국 수"},
        color_discrete_sequence=["#1f77b4"]
    )

    fig.update_layout(
        height=300,
        xaxis_title="매칭 점수",
        yaxis_title="약국 수"
    )

    return fig


def create_cluster_distribution_chart(df: pd.DataFrame) -> px.bar:
    """클러스터별 약국 분포"""
    cluster_col = None
    for col in ["pharmacy_cluster_id", "cluster_id", "cluster", "pharmacy_cluster"]:
        if col in df.columns:
            cluster_col = col
            break

    if cluster_col is None:
        return None

    cluster_counts = df[cluster_col].value_counts().reset_index()
    cluster_counts.columns = ["cluster", "count"]
    cluster_counts = cluster_counts.sort_values("cluster")

    fig = px.bar(
        cluster_counts,
        x="cluster",
        y="count",
        title="클러스터별 약국 분포",
        labels={"cluster": "클러스터", "count": "약국 수"},
        color_discrete_sequence=["#2ca02c"]
    )

    fig.update_layout(height=300)

    return fig


def render_page():
    """상품-약국 매칭 대시보드 페이지 렌더링"""
    st.title("상품-약국 매칭 대시보드")
    st.markdown("상품을 선택하면 마케팅 대상 약국 리스트를 확인할 수 있습니다.")
    st.markdown("---")

    # 상품 목록 조회
    products = fetch_products()

    if not products:
        st.warning("타겟팅 결과가 없습니다. 타겟팅 파이프라인을 먼저 실행해주세요.")
        st.info("Airflow DAG `medi_db_main_pipeline`을 실행하거나, Targeting 서비스를 수동으로 호출하세요.")
        return

    # 상품 선택 드롭다운
    col1, col2, col3 = st.columns([3, 1, 1])

    with col1:
        product_options = {f"{p['name']} (매칭: {p['doc_count']}건)": p["id"] for p in products}
        selected_product_label = st.selectbox(
            "상품 선택",
            list(product_options.keys()),
            help="타겟팅 대상 상품을 선택하세요"
        )
        selected_product_id = product_options[selected_product_label]
        selected_product_name = next(p["name"] for p in products if p["id"] == selected_product_id)

    with col2:
        min_score = st.slider(
            "최소 매칭 점수",
            min_value=0.0,
            max_value=1.0,
            value=0.0,
            step=0.05,
            help="이 점수 이상인 약국만 표시"
        )

    with col3:
        display_limit = st.selectbox(
            "표시 개수",
            [50, 100, 200, 500],
            index=1,
            help="표시할 약국 수"
        )

    st.markdown("---")

    # 요약 통계
    summary = fetch_product_summary(selected_product_id)

    if summary:
        st.subheader(f"'{selected_product_name}' 타겟팅 요약")

        metric_cols = st.columns(4)

        with metric_cols[0]:
            st.metric("전체 매칭 약국", f"{int(summary.get('total_pharmacies', 0)):,}")

        with metric_cols[1]:
            st.metric("평균 점수", f"{summary.get('avg_score', 0):.3f}")

        with metric_cols[2]:
            st.metric("최고 점수", f"{summary.get('max_score', 0):.3f}")

        with metric_cols[3]:
            st.metric("최저 점수", f"{summary.get('min_score', 0):.3f}")

    st.markdown("---")

    # 약국 리스트 조회
    pharmacies = fetch_matching_pharmacies(
        product_id=selected_product_id,
        limit=display_limit,
        min_score=min_score
    )

    if not pharmacies:
        st.info("조건에 맞는 약국이 없습니다.")
        return

    df = pd.DataFrame(pharmacies)

    # 차트 영역
    chart_col1, chart_col2 = st.columns(2)

    with chart_col1:
        score_fig = create_score_distribution_chart(df)
        if score_fig:
            st.plotly_chart(score_fig, use_container_width=True)

    with chart_col2:
        cluster_fig = create_cluster_distribution_chart(df)
        if cluster_fig:
            st.plotly_chart(cluster_fig, use_container_width=True)
        else:
            st.info("클러스터 정보가 없어 분포 차트를 표시할 수 없습니다.")

    st.markdown("---")

    # 약국 리스트 테이블
    st.subheader(f"추천 약국 리스트 (상위 {len(df)}개, 매칭 점수순)")

    # 표시할 컬럼 선택
    display_columns = []
    column_config = {}

    # 순위 컬럼 추가
    df = df.reset_index(drop=True)
    df.insert(0, "순위", range(1, len(df) + 1))
    display_columns.append("순위")

    # 약국명
    if "pharmacy_name" in df.columns:
        display_columns.append("pharmacy_name")
        column_config["pharmacy_name"] = st.column_config.TextColumn("약국명", width="medium")

    # 매칭 점수
    if "match_score" in df.columns:
        display_columns.append("match_score")
        column_config["match_score"] = st.column_config.ProgressColumn(
            "매칭 점수",
            format="%.3f",
            min_value=0,
            max_value=1,
        )

    # 랭킹 점수
    if "ranking_score" in df.columns:
        display_columns.append("ranking_score")
        column_config["ranking_score"] = st.column_config.NumberColumn(
            "랭킹 점수",
            format="%.2f",
        )

    # 클러스터 정보
    for cluster_col in ["pharmacy_cluster_id", "cluster_id", "cluster", "pharmacy_cluster"]:
        if cluster_col in df.columns:
            display_columns.append(cluster_col)
            column_config[cluster_col] = st.column_config.TextColumn("클러스터", width="small")
            break

    # 추가 정보 (있는 경우)
    optional_cols = ["region", "pharmacy_address", "address", "pharmacy_id"]
    for col in optional_cols:
        if col in df.columns:
            display_columns.append(col)
            if col == "region":
                column_config[col] = st.column_config.TextColumn("지역", width="small")
            elif col in ("pharmacy_address", "address"):
                column_config[col] = st.column_config.TextColumn("주소", width="large")
            elif col == "pharmacy_id":
                column_config[col] = st.column_config.TextColumn("약국 ID", width="small")

    # 데이터프레임 표시
    st.dataframe(
        df[display_columns],
        column_config=column_config,
        use_container_width=True,
        hide_index=True,
        height=500
    )

    # 다운로드 버튼
    st.markdown("---")

    # 전체 데이터 다운로드
    csv_all = df.to_csv(index=False, encoding="utf-8-sig")
    st.download_button(
        label="전체 목록 다운로드 (CSV)",
        data=csv_all,
        file_name=f"matching_pharmacies_{selected_product_id}.csv",
        mime="text/csv",
    )


if __name__ == "__main__":
    render_page()
