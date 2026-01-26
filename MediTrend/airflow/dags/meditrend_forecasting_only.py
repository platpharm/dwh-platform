"""
MediTrend 수요예측 전용 DAG
수동 실행용: 수요예측 및 랭킹 계산 독립 실행
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import BranchPythonOperator
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
    'meditrend_forecasting_only',
    default_args=default_args,
    description='MediTrend 수요예측 단독 실행',
    schedule_interval=None,  # 수동 실행
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['meditrend', 'forecasting'],
    params={
        'days_ahead': 30,
        'calculate_ranking': True,
    }
) as dag:

    # ============================================================
    # 수요예측 실행
    # ============================================================
    run_forecasting = SimpleHttpOperator(
        task_id='run_demand_forecasting',
        http_conn_id='forecasting_service',
        endpoint='/forecast/run',
        method='POST',
        headers={'Content-Type': 'application/json'},
        data=json.dumps({
            'days_ahead': '{{ params.days_ahead }}'
        }),
        response_check=lambda response: response.json().get('success', False),
        log_response=True,
    )

    # ============================================================
    # 랭킹 계산 (7:3 가중치)
    # ============================================================
    calculate_ranking = SimpleHttpOperator(
        task_id='calculate_product_ranking',
        http_conn_id='forecasting_service',
        endpoint='/forecast/ranking',
        method='POST',
        headers={'Content-Type': 'application/json'},
        data=json.dumps({
            'sales_weight': 0.7,
            'trend_weight': 0.3
        }),
        response_check=lambda response: response.json().get('success', False),
        log_response=True,
    )

    # ============================================================
    # 트렌드 분석
    # ============================================================
    extract_emerging_trends = SimpleHttpOperator(
        task_id='extract_emerging_trends',
        http_conn_id='forecasting_service',
        endpoint='/trend/emerging',
        method='GET',
        headers={'Content-Type': 'application/json'},
        log_response=True,
    )

    # ============================================================
    # Dependencies
    # ============================================================
    run_forecasting >> calculate_ranking >> extract_emerging_trends
