"""
MediTrend 크롤링 전용 DAG
수동/스케줄 실행: 트렌드 데이터 크롤링
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
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# 트렌드 수집 키워드 정의
HEALTH_KEYWORDS_EN = [
    'vitamin', 'supplement', 'pain relief', 'cold medicine',
    'digestive', 'skin care', 'hair loss', 'diet', 'immunity',
    'sleep aid', 'probiotics', 'omega-3', 'collagen', 'eye health'
]

PAPER_KEYWORDS = [
    'vitamin D supplementation',
    'omega-3 fatty acids',
    'probiotics gut health',
    'collagen skin',
    'melatonin sleep'
]

with DAG(
    'meditrend_crawling_only',
    default_args=default_args,
    description='MediTrend 트렌드 크롤링',
    schedule_interval='0 6 * * *',  # 매일 오전 6시
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['meditrend', 'crawling'],
) as dag:

    # ============================================================
    # 구글 트렌드 크롤링
    # ============================================================
    with TaskGroup('google_crawling', tooltip='구글 트렌드') as google_group:

        crawl_google_health = SimpleHttpOperator(
            task_id='crawl_google_health_keywords',
            http_conn_id='crawler_service',
            endpoint='/crawl/google',
            method='POST',
            headers={'Content-Type': 'application/json'},
            data=json.dumps({
                'keywords': HEALTH_KEYWORDS_EN[:5],
                'include_related': True
            }),
            response_check=lambda response: response.json().get('success', False),
            log_response=True,
        )

        crawl_google_health_2 = SimpleHttpOperator(
            task_id='crawl_google_health_keywords_2',
            http_conn_id='crawler_service',
            endpoint='/crawl/google',
            method='POST',
            headers={'Content-Type': 'application/json'},
            data=json.dumps({
                'keywords': HEALTH_KEYWORDS_EN[5:10],
                'include_related': True
            }),
            response_check=lambda response: response.json().get('success', False),
            log_response=True,
        )

        crawl_google_health_3 = SimpleHttpOperator(
            task_id='crawl_google_health_keywords_3',
            http_conn_id='crawler_service',
            endpoint='/crawl/google',
            method='POST',
            headers={'Content-Type': 'application/json'},
            data=json.dumps({
                'keywords': HEALTH_KEYWORDS_EN[10:14],
                'include_related': True
            }),
            response_check=lambda response: response.json().get('success', False),
            log_response=True,
        )

    # ============================================================
    # 논문 크롤링
    # ============================================================
    crawl_papers = SimpleHttpOperator(
        task_id='crawl_research_papers',
        http_conn_id='crawler_service',
        endpoint='/crawl/papers',
        method='POST',
        headers={'Content-Type': 'application/json'},
        data=json.dumps({
            'keywords': PAPER_KEYWORDS,
            'sources': ['pubmed', 'arxiv'],
            'max_results': 100
        }),
        response_check=lambda response: response.json().get('success', False),
        log_response=True,
    )

    # 병렬 실행
    [google_group, crawl_papers]
