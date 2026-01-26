"""
MediTrend 클러스터링 전용 DAG
수동 실행용: 클러스터링 알고리즘만 독립 실행
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.task_group import TaskGroup
import json

default_args = {
    'owner': 'meditrend',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    'meditrend_clustering_only',
    default_args=default_args,
    description='MediTrend 클러스터링 단독 실행',
    schedule_interval=None,  # 수동 실행
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['meditrend', 'clustering'],
) as dag:

    # ============================================================
    # HDBSCAN 클러스터링
    # ============================================================
    with TaskGroup('hdbscan_clustering', tooltip='HDBSCAN 클러스터링') as hdbscan_group:

        hdbscan_products = SimpleHttpOperator(
            task_id='hdbscan_products',
            http_conn_id='clustering_service',
            endpoint='/cluster/hdbscan',
            method='POST',
            headers={'Content-Type': 'application/json'},
            data=json.dumps({
                'entity_type': 'product',
                'min_cluster_size': 10,
                'min_samples': 5
            }),
            response_check=lambda response: response.json().get('success', False),
            log_response=True,
        )

        hdbscan_pharmacies = SimpleHttpOperator(
            task_id='hdbscan_pharmacies',
            http_conn_id='clustering_service',
            endpoint='/cluster/hdbscan',
            method='POST',
            headers={'Content-Type': 'application/json'},
            data=json.dumps({
                'entity_type': 'pharmacy',
                'min_cluster_size': 20,
                'min_samples': 10
            }),
            response_check=lambda response: response.json().get('success', False),
            log_response=True,
        )

    # ============================================================
    # GMM 클러스터링
    # ============================================================
    with TaskGroup('gmm_clustering', tooltip='GMM 클러스터링') as gmm_group:

        gmm_products = SimpleHttpOperator(
            task_id='gmm_products',
            http_conn_id='clustering_service',
            endpoint='/cluster/gmm',
            method='POST',
            headers={'Content-Type': 'application/json'},
            data=json.dumps({
                'entity_type': 'product',
                'n_components': 15,
                'covariance_type': 'full'
            }),
            response_check=lambda response: response.json().get('success', False),
            log_response=True,
        )

        gmm_pharmacies = SimpleHttpOperator(
            task_id='gmm_pharmacies',
            http_conn_id='clustering_service',
            endpoint='/cluster/gmm',
            method='POST',
            headers={'Content-Type': 'application/json'},
            data=json.dumps({
                'entity_type': 'pharmacy',
                'n_components': 10,
                'covariance_type': 'full'
            }),
            response_check=lambda response: response.json().get('success', False),
            log_response=True,
        )

    # ============================================================
    # Mini-Batch K-Means 클러스터링
    # ============================================================
    with TaskGroup('kmeans_clustering', tooltip='K-Means 클러스터링') as kmeans_group:

        kmeans_products = SimpleHttpOperator(
            task_id='kmeans_products',
            http_conn_id='clustering_service',
            endpoint='/cluster/kmeans',
            method='POST',
            headers={'Content-Type': 'application/json'},
            data=json.dumps({
                'entity_type': 'product',
                'n_clusters': 20,
                'batch_size': 1000
            }),
            response_check=lambda response: response.json().get('success', False),
            log_response=True,
        )

        kmeans_pharmacies = SimpleHttpOperator(
            task_id='kmeans_pharmacies',
            http_conn_id='clustering_service',
            endpoint='/cluster/kmeans',
            method='POST',
            headers={'Content-Type': 'application/json'},
            data=json.dumps({
                'entity_type': 'pharmacy',
                'n_clusters': 15,
                'batch_size': 500
            }),
            response_check=lambda response: response.json().get('success', False),
            log_response=True,
        )

    # 병렬 실행 (그룹 간 의존성 없음)
    [hdbscan_group, gmm_group, kmeans_group]
