"""DWH Dashboard - Streamlit 메인 애플리케이션"""
import streamlit as st

st.set_page_config(
    page_title="DWH Dashboard",
    page_icon="",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.sidebar.title("DWH Dashboard")
st.sidebar.markdown("---")

page = st.sidebar.radio(
    "페이지 선택",
    [
        "홈",
        "클러스터링 시각화",
        "수요예측 차트",
        "인기 의약품 랭킹",
        "상품-약국 매칭",
    ],
)

if page == "홈":
    st.title("DWH Analytics Dashboard")
    st.markdown("---")

    st.markdown("""
    ### 대시보드 기능 안내

    **1. 클러스터링 시각화**
    - UMAP 2D 산점도로 상품/약국 클러스터 시각화
    - 클러스터별 분포 확인

    **2. 수요예측 차트**
    - 시계열 예측 그래프
    - 신뢰구간 포함 예측 결과

    **3. 인기 의약품 랭킹**
    - 전체/카테고리별 랭킹 테이블
    - 판매량 70% + 트렌드 30% 가중치 적용

    **4. 상품-약국 매칭**
    - 상품별 마케팅 대상 약국 리스트
    - 매칭 점수 기반 약국 순위
    - 약국별 클러스터 정보 표시
    """)

    st.markdown("---")
    st.subheader("시스템 상태")

    try:
        from shared.clients.es_client import es_client

        if es_client.health_check():
            st.success("Elasticsearch: 연결됨")
        else:
            st.error("Elasticsearch: 연결 실패")
    except Exception as e:
        st.warning(f"Elasticsearch 연결 확인 불가: {e}")

elif page == "클러스터링 시각화":
    from views.clustering_viz import render_page
    render_page()

elif page == "수요예측 차트":
    from views.forecast_chart import render_page
    render_page()

elif page == "인기 의약품 랭킹":
    from views.ranking_view import render_page
    render_page()

elif page == "상품-약국 매칭":
    from views.targeting_dashboard import render_page
    render_page()

st.sidebar.markdown("---")
st.sidebar.caption("DWH Analytics Platform v1.0")
