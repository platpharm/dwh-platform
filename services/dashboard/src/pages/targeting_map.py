"""약국 타겟팅 지도 페이지"""
import streamlit as st
import pandas as pd
import folium
from streamlit_folium import st_folium
import plotly.express as px
from typing import List, Dict, Any, Optional

from shared.clients.es_client import es_client
from shared.config import ESIndex


# 기본 지도 중심 (서울)
DEFAULT_CENTER = [37.5665, 126.9780]
DEFAULT_ZOOM = 11


def fetch_targeting_data(
    product_id: str = None,
    is_target: Optional[bool] = None,
    limit: int = 500
) -> List[Dict[str, Any]]:
    """ES에서 타겟팅 결과 조회"""
    must_clauses = []

    if product_id:
        must_clauses.append({"term": {"product_id": product_id}})
    if is_target is not None:
        must_clauses.append({"term": {"is_target": is_target}})

    if must_clauses:
        query = {"bool": {"must": must_clauses}}
    else:
        query = {"match_all": {}}

    try:
        results = es_client.search(
            index=ESIndex.TARGETING_RESULT,
            query=query,
            size=limit,
        )
        return results
    except Exception as e:
        st.error(f"데이터 조회 실패: {e}")
        return []


def fetch_products() -> List[Dict[str, str]]:
    """상품 목록 조회"""
    try:
        response = es_client.client.search(
            index=ESIndex.TARGETING_RESULT,
            body={
                "size": 0,
                "aggs": {
                    "products": {
                        "terms": {
                            "field": "product_id",
                            "size": 100,
                        },
                        "aggs": {
                            "product_name": {
                                "terms": {
                                    "field": "product_name",
                                    "size": 1,
                                }
                            }
                        }
                    }
                }
            }
        )
        buckets = response.get("aggregations", {}).get("products", {}).get("buckets", [])
        return [
            {
                "id": bucket["key"],
                "name": bucket.get("product_name", {}).get("buckets", [{}])[0].get("key", bucket["key"])
            }
            for bucket in buckets
        ]
    except Exception:
        return []


def create_pharmacy_map(df: pd.DataFrame) -> folium.Map:
    """약국 위치 지도 생성"""
    # 지도 중심 계산
    if len(df) > 0 and "latitude" in df.columns and "longitude" in df.columns:
        center = [df["latitude"].mean(), df["longitude"].mean()]
    else:
        center = DEFAULT_CENTER

    # Folium 지도 생성
    m = folium.Map(
        location=center,
        zoom_start=DEFAULT_ZOOM,
        tiles="cartodbpositron",
    )

    # 마커 추가
    for _, row in df.iterrows():
        if pd.notna(row.get("latitude")) and pd.notna(row.get("longitude")):
            # 타겟 여부에 따른 색상
            is_target = row.get("is_target", False)
            color = "red" if is_target else "blue"
            icon = "star" if is_target else "home"

            # 팝업 내용
            popup_html = f"""
            <div style="width: 200px;">
                <b>{row.get('pharmacy_name', 'N/A')}</b><br>
                <hr>
                주소: {row.get('address', 'N/A')}<br>
                타겟 여부: {'O' if is_target else 'X'}<br>
                타겟 점수: {row.get('target_score', 0):.2f}<br>
                상품: {row.get('product_name', 'N/A')}
            </div>
            """

            folium.Marker(
                location=[row["latitude"], row["longitude"]],
                popup=folium.Popup(popup_html, max_width=300),
                icon=folium.Icon(color=color, icon=icon, prefix="fa"),
                tooltip=row.get("pharmacy_name", "약국"),
            ).add_to(m)

    # 범례 추가
    legend_html = """
    <div style="position: fixed; bottom: 50px; left: 50px; z-index: 1000;
                background-color: white; padding: 10px; border-radius: 5px;
                border: 2px solid gray; font-size: 14px;">
        <b>범례</b><br>
        <i class="fa fa-star" style="color:red"></i> 타겟 약국<br>
        <i class="fa fa-home" style="color:blue"></i> 일반 약국
    </div>
    """
    m.get_root().html.add_child(folium.Element(legend_html))

    return m


def create_target_distribution_chart(df: pd.DataFrame) -> px.pie:
    """타겟 분포 파이 차트"""
    if "is_target" not in df.columns:
        return None

    target_counts = df["is_target"].value_counts().reset_index()
    target_counts.columns = ["is_target", "count"]
    target_counts["label"] = target_counts["is_target"].map({True: "타겟 약국", False: "일반 약국"})

    fig = px.pie(
        target_counts,
        values="count",
        names="label",
        title="타겟 약국 비율",
        color="label",
        color_discrete_map={"타겟 약국": "red", "일반 약국": "steelblue"},
    )

    fig.update_layout(height=300)

    return fig


def create_target_score_histogram(df: pd.DataFrame) -> px.histogram:
    """타겟 점수 분포 히스토그램"""
    if "target_score" not in df.columns:
        return None

    fig = px.histogram(
        df,
        x="target_score",
        color="is_target" if "is_target" in df.columns else None,
        title="타겟 점수 분포",
        nbins=20,
        color_discrete_map={True: "red", False: "steelblue"},
        labels={"is_target": "타겟 여부"},
    )

    fig.update_layout(height=300)

    return fig


def create_region_stats(df: pd.DataFrame) -> pd.DataFrame:
    """지역별 통계"""
    if "region" not in df.columns:
        return None

    stats = df.groupby("region").agg(
        total=("pharmacy_id", "count"),
        targets=("is_target", "sum") if "is_target" in df.columns else ("pharmacy_id", "count"),
        avg_score=("target_score", "mean") if "target_score" in df.columns else ("pharmacy_id", "count"),
    ).reset_index()

    if "is_target" in df.columns:
        stats["target_rate"] = (stats["targets"] / stats["total"] * 100).round(1)

    return stats


def render_page():
    """약국 타겟팅 지도 페이지 렌더링"""
    st.title("약국 타겟팅 지도")
    st.markdown("---")

    # 필터 옵션
    col1, col2, col3 = st.columns([2, 1, 1])

    with col1:
        products = fetch_products()
        product_options = {p["name"]: p["id"] for p in products} if products else {}
        product_options = {"전체 상품": None, **product_options}

        if not products:
            product_options = {"전체 상품": None, "샘플 상품 1": "SAMPLE_001", "샘플 상품 2": "SAMPLE_002"}

        selected_product_name = st.selectbox("상품 선택", list(product_options.keys()))
        selected_product_id = product_options[selected_product_name]

    with col2:
        target_filter = st.selectbox(
            "타겟 필터",
            ["전체", "타겟만", "비타겟만"],
        )
        is_target = None
        if target_filter == "타겟만":
            is_target = True
        elif target_filter == "비타겟만":
            is_target = False

    with col3:
        data_limit = st.number_input("조회 수", min_value=100, max_value=2000, value=500)

    if st.button("지도 조회", type="primary"):
        st.session_state["targeting_data_loaded"] = True
        st.session_state["targeting_product_id"] = selected_product_id
        st.session_state["targeting_is_target"] = is_target
        st.session_state["targeting_limit"] = data_limit

    # 데이터 로드 및 시각화
    if st.session_state.get("targeting_data_loaded", False):
        with st.spinner("타겟팅 데이터 로딩 중..."):
            data = fetch_targeting_data(
                product_id=st.session_state.get("targeting_product_id"),
                is_target=st.session_state.get("targeting_is_target"),
                limit=st.session_state.get("targeting_limit", 500),
            )

        if data:
            df = pd.DataFrame(data)

            # 필수 컬럼 확인 및 기본값 설정
            required_cols = ["pharmacy_id", "latitude", "longitude"]
            missing_cols = [col for col in required_cols if col not in df.columns]

            if missing_cols:
                st.warning(f"누락된 필드: {missing_cols}. 샘플 데이터로 시각화합니다.")
                # 샘플 데이터 생성 (서울 지역)
                import numpy as np
                n_samples = 50
                regions = ["강남구", "서초구", "송파구", "강동구", "마포구", "영등포구", "용산구", "중구"]

                df = pd.DataFrame({
                    "pharmacy_id": [f"PHARM_{i:03d}" for i in range(n_samples)],
                    "pharmacy_name": [f"행복약국 {i+1}" for i in range(n_samples)],
                    "latitude": np.random.uniform(37.45, 37.65, n_samples),
                    "longitude": np.random.uniform(126.85, 127.15, n_samples),
                    "address": [f"서울시 {np.random.choice(regions)} {np.random.randint(1, 100)}번지" for _ in range(n_samples)],
                    "region": np.random.choice(regions, n_samples),
                    "is_target": np.random.choice([True, False], n_samples, p=[0.3, 0.7]),
                    "target_score": np.random.uniform(0, 100, n_samples),
                    "product_id": "SAMPLE_001",
                    "product_name": "샘플 상품",
                })

            # 메인 지도
            st.subheader("약국 위치 지도")
            pharmacy_map = create_pharmacy_map(df)
            st_folium(pharmacy_map, width=None, height=500, use_container_width=True)

            # 통계 및 차트
            col1, col2 = st.columns(2)

            with col1:
                st.subheader("타겟 분포")
                dist_fig = create_target_distribution_chart(df)
                if dist_fig:
                    st.plotly_chart(dist_fig, use_container_width=True)

                # 요약 메트릭
                st.subheader("요약")
                metric_col1, metric_col2, metric_col3 = st.columns(3)

                with metric_col1:
                    st.metric("전체 약국", len(df))
                with metric_col2:
                    if "is_target" in df.columns:
                        st.metric("타겟 약국", int(df["is_target"].sum()))
                with metric_col3:
                    if "is_target" in df.columns:
                        rate = df["is_target"].mean() * 100
                        st.metric("타겟 비율", f"{rate:.1f}%")

            with col2:
                st.subheader("타겟 점수 분포")
                score_fig = create_target_score_histogram(df)
                if score_fig:
                    st.plotly_chart(score_fig, use_container_width=True)

                # 지역별 통계
                if "region" in df.columns:
                    st.subheader("지역별 통계")
                    region_stats = create_region_stats(df)
                    if region_stats is not None:
                        st.dataframe(region_stats, use_container_width=True)

            # 상세 데이터 테이블
            st.subheader("타겟 약국 상세")
            with st.expander("데이터 테이블 보기"):
                display_cols = ["pharmacy_name", "address", "region", "is_target", "target_score"]
                available_cols = [col for col in display_cols if col in df.columns]

                # 타겟 약국 우선 정렬
                if "is_target" in df.columns and "target_score" in df.columns:
                    df_sorted = df.sort_values(["is_target", "target_score"], ascending=[False, False])
                else:
                    df_sorted = df

                st.dataframe(
                    df_sorted[available_cols].style.format({
                        "target_score": "{:.2f}",
                    }) if "target_score" in available_cols else df_sorted[available_cols],
                    use_container_width=True,
                )

            # 타겟 약국 리스트 다운로드
            if "is_target" in df.columns:
                target_df = df[df["is_target"] == True]
                if len(target_df) > 0:
                    csv = target_df.to_csv(index=False, encoding="utf-8-sig")
                    st.download_button(
                        label="타겟 약국 목록 다운로드 (CSV)",
                        data=csv,
                        file_name="target_pharmacies.csv",
                        mime="text/csv",
                    )

        else:
            st.info("조회된 데이터가 없습니다. ES 인덱스를 확인해주세요.")
    else:
        st.info("'지도 조회' 버튼을 클릭하여 약국 타겟팅 지도를 확인하세요.")


if __name__ == "__main__":
    render_page()
