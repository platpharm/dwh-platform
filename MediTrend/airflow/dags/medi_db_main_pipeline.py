"""
MediDB 메인 파이프라인 DAG
일일 실행: 상품 트렌드 크롤링 → 전처리 → 클러스터링(병렬) → 랭킹 → 타겟팅

파이프라인 순서:
1. [선택] 상품명 기반 트렌드 크롤링 (DB 상품명으로 Google Trends 수집)
2. 전처리 (약국, 상품, 주문)
3. 클러스터링 (상품, 약국) - 병렬 실행
4. 랭킹 계산
5. 타겟팅
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
import json

default_args = {
    'owner': 'medi_db',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def log_task_result(**context):
    """태스크 결과 로깅"""
    ti = context['ti']
    task_id = context['task'].task_id
    result = ti.xcom_pull(task_ids=task_id)
    print(f"Task {task_id} result: {result}")


with DAG(
    'medi_db_main_pipeline',
    default_args=default_args,
    description='MediDB 메인 데이터 파이프라인',
    schedule_interval='0 2 * * *',  # 매일 새벽 2시
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['medi-db', 'main'],
) as dag:

    # ============================================================
    # 1. 상품명 기반 트렌드 크롤링 (전처리 전에 실행)
    # ============================================================
    crawl_product_trends = SimpleHttpOperator(
        task_id='crawl_product_trends',
        http_conn_id='crawler_service',
        endpoint='/crawl/product-trends',
        method='POST',
        headers={'Content-Type': 'application/json'},
        data=json.dumps({
            'limit': 100,           # 상위 100개 상품
            'include_related': True
        }),
        response_check=lambda response: response.json().get('success', False),
        log_response=True,
    )

    # ============================================================
    # 2. 전처리 (약국, 상품, 주문)
    # ============================================================
    preprocess_all = SimpleHttpOperator(
        task_id='preprocess_all_data',
        http_conn_id='preprocessor_service',
        endpoint='/preprocess/all',
        method='POST',
        headers={'Content-Type': 'application/json'},
        response_check=lambda response: response.json().get('success', False),
        log_response=True,
    )

    # ============================================================
    # 3. 클러스터링 (상품, 약국) - 병렬 실행
    # ============================================================
    with TaskGroup('clustering', tooltip='상품 및 약국 클러스터링') as clustering_group:

        # 클러스터링 - 상품
        cluster_products = SimpleHttpOperator(
            task_id='cluster_products',
            http_conn_id='clustering_service',
            endpoint='/cluster/run',
            method='POST',
            headers={'Content-Type': 'application/json'},
            data=json.dumps({
                'algorithm': 'hdbscan',
                'entity_type': 'product',
                'params': {'min_cluster_size': 10}
            }),
            response_check=lambda response: response.json().get('success', False),
            log_response=True,
        )

        # 클러스터링 - 약국
        cluster_pharmacies = SimpleHttpOperator(
            task_id='cluster_pharmacies',
            http_conn_id='clustering_service',
            endpoint='/cluster/run',
            method='POST',
            headers={'Content-Type': 'application/json'},
            data=json.dumps({
                'algorithm': 'gmm',
                'entity_type': 'pharmacy',
                'params': {'n_components': 10}
            }),
            response_check=lambda response: response.json().get('success', False),
            log_response=True,
        )

    # ============================================================
    # 4. 랭킹 계산 (클러스터링 완료 후)
    # ============================================================
    calculate_ranking = SimpleHttpOperator(
        task_id='calculate_ranking',
        http_conn_id='forecasting_service',
        endpoint='/forecast/ranking',
        method='POST',
        headers={'Content-Type': 'application/json'},
        response_check=lambda response: response.json().get('success', False),
        log_response=True,
    )

    # ============================================================
    # 5. 타겟팅 (랭킹 계산 완료 후)
    # ============================================================
    run_targeting = SimpleHttpOperator(
        task_id='run_targeting',
        http_conn_id='targeting_service',
        endpoint='/target/run',
        method='POST',
        headers={'Content-Type': 'application/json'},
        data=json.dumps({
            'top_n_products': 100,
            'top_n_pharmacies': 50
        }),
        response_check=lambda response: response.json().get('success', False),
        log_response=True,
    )

    # ============================================================
    # Dependencies (파이프라인 순서)
    # ============================================================
    # 1. 트렌드 크롤링 → 2. 전처리 → 3. 클러스터링(병렬) → 4. 랭킹 → 5. 타겟팅
    crawl_product_trends >> preprocess_all >> clustering_group >> calculate_ranking >> run_targeting
