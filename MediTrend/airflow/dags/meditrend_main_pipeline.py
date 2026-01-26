"""
MediTrend 메인 파이프라인 DAG
일일 실행: 크롤링 → 전처리 → 알고리즘(병렬) → 타겟팅
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
import json

default_args = {
    'owner': 'meditrend',
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
    'meditrend_main_pipeline',
    default_args=default_args,
    description='MediTrend 메인 데이터 파이프라인',
    schedule_interval='0 2 * * *',  # 매일 새벽 2시
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['meditrend', 'main'],
) as dag:

    # ============================================================
    # 1. 크롤링 (병렬)
    # ============================================================
    with TaskGroup('crawling', tooltip='트렌드 데이터 크롤링') as crawling_group:

        crawl_google = SimpleHttpOperator(
            task_id='crawl_google_trends',
            http_conn_id='crawler_service',
            endpoint='/crawl/google',
            method='POST',
            headers={'Content-Type': 'application/json'},
            data=json.dumps({
                'keywords': ['vitamin', 'supplement', 'pain relief', 'cold medicine',
                            'digestive', 'skin care', 'hair loss', 'diet', 'immunity']
            }),
            response_check=lambda response: response.json().get('success', False),
            log_response=True,
        )

        crawl_papers = SimpleHttpOperator(
            task_id='crawl_papers',
            http_conn_id='crawler_service',
            endpoint='/crawl/papers',
            method='POST',
            headers={'Content-Type': 'application/json'},
            data=json.dumps({
                'keywords': ['vitamin D', 'omega-3', 'probiotics', 'collagen'],
                'sources': ['pubmed'],
                'max_results': 50
            }),
            response_check=lambda response: response.json().get('success', False),
            log_response=True,
        )

    # ============================================================
    # 2. 전처리
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
    # 3. 알고리즘 (병렬)
    # ============================================================
    with TaskGroup('algorithms', tooltip='클러스터링 및 수요예측') as algorithm_group:

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

        # 수요예측
        run_forecasting = SimpleHttpOperator(
            task_id='run_forecasting',
            http_conn_id='forecasting_service',
            endpoint='/forecast/run',
            method='POST',
            headers={'Content-Type': 'application/json'},
            data=json.dumps({
                'days_ahead': 30
            }),
            response_check=lambda response: response.json().get('success', False),
            log_response=True,
        )

        # 랭킹 계산
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
    # 4. 타겟팅
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
    # Dependencies
    # ============================================================
    crawling_group >> preprocess_all >> algorithm_group >> run_targeting
