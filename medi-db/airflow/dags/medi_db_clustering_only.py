"""
MediDB 클러스터링 전용 DAG
수동 실행용: 클러스터링 알고리즘만 독립 실행
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.task_group import TaskGroup

default_args = {
    'owner': 'medi_db',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    'medi_db_clustering_only',
    default_args=default_args,
    description='MediDB 클러스터링 단독 실행',
    schedule_interval=None,  # 수동 실행
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['medi-db', 'clustering'],
) as dag:

    with TaskGroup('hdbscan_clustering', tooltip='HDBSCAN 클러스터링') as hdbscan_group:

        hdbscan_products = SimpleHttpOperator(
            task_id='hdbscan_products',
            http_conn_id='clustering_service',
            endpoint='/cluster/hdbscan?entity_type=product&min_cluster_size=10&min_samples=5',
            method='POST',
            headers={'Content-Type': 'application/json'},
            response_check=lambda response: response.json().get('success', False),
            log_response=True,
        )

        hdbscan_pharmacies = SimpleHttpOperator(
            task_id='hdbscan_pharmacies',
            http_conn_id='clustering_service',
            endpoint='/cluster/hdbscan?entity_type=pharmacy&min_cluster_size=20&min_samples=10',
            method='POST',
            headers={'Content-Type': 'application/json'},
            response_check=lambda response: response.json().get('success', False),
            log_response=True,
        )

    with TaskGroup('gmm_clustering', tooltip='GMM 클러스터링') as gmm_group:

        gmm_products = SimpleHttpOperator(
            task_id='gmm_products',
            http_conn_id='clustering_service',
            endpoint='/cluster/gmm?entity_type=product&n_components=15&covariance_type=full',
            method='POST',
            headers={'Content-Type': 'application/json'},
            response_check=lambda response: response.json().get('success', False),
            log_response=True,
        )

        gmm_pharmacies = SimpleHttpOperator(
            task_id='gmm_pharmacies',
            http_conn_id='clustering_service',
            endpoint='/cluster/gmm?entity_type=pharmacy&n_components=10&covariance_type=full',
            method='POST',
            headers={'Content-Type': 'application/json'},
            response_check=lambda response: response.json().get('success', False),
            log_response=True,
        )

    with TaskGroup('kmeans_clustering', tooltip='K-Means 클러스터링') as kmeans_group:

        kmeans_products = SimpleHttpOperator(
            task_id='kmeans_products',
            http_conn_id='clustering_service',
            endpoint='/cluster/kmeans?entity_type=product&n_clusters=20&batch_size=1000',
            method='POST',
            headers={'Content-Type': 'application/json'},
            response_check=lambda response: response.json().get('success', False),
            log_response=True,
        )

        kmeans_pharmacies = SimpleHttpOperator(
            task_id='kmeans_pharmacies',
            http_conn_id='clustering_service',
            endpoint='/cluster/kmeans?entity_type=pharmacy&n_clusters=15&batch_size=500',
            method='POST',
            headers={'Content-Type': 'application/json'},
            response_check=lambda response: response.json().get('success', False),
            log_response=True,
        )

    [hdbscan_group, gmm_group, kmeans_group]
