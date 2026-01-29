from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
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

with DAG(
    'medi_db_main_pipeline',
    default_args=default_args,
    description='MediDB 메인 데이터 파이프라인',
    schedule_interval='0 2 * * *',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['medi-db', 'main'],
) as dag:

    crawl_product_trends = SimpleHttpOperator(
        task_id='crawl_product_trends',
        http_conn_id='crawler_service',
        endpoint='/crawl/product-trends',
        method='POST',
        headers={'Content-Type': 'application/json'},
        data=json.dumps({
            'top_n': 100,
            'include_related': True
        }),
        response_check=lambda response: response.json().get('success', False),
        log_response=True,
    )

    preprocess_all = SimpleHttpOperator(
        task_id='preprocess_all_data',
        http_conn_id='preprocessor_service',
        endpoint='/preprocess/all',
        method='POST',
        headers={'Content-Type': 'application/json'},
        response_check=lambda response: response.json().get('success', False),
        log_response=True,
    )

    with TaskGroup('clustering', tooltip='상품 및 약국 클러스터링') as clustering_group:

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

    calculate_ranking = SimpleHttpOperator(
        task_id='calculate_ranking',
        http_conn_id='forecasting_service',
        endpoint='/forecast/ranking',
        method='POST',
        headers={'Content-Type': 'application/json'},
        data=json.dumps({
            'sales_weight': 0.6,
            'product_trend_weight': 0.3,
            'category_trend_weight': 0.1,
        }),
        response_check=lambda response: response.json().get('success', False),
        log_response=True,
    )

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

    crawl_product_trends >> preprocess_all >> clustering_group >> calculate_ranking >> run_targeting
