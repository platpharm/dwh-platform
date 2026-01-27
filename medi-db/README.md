# MediDB

약국 마케팅 및 수요예측을 위한 MSA 기반 데이터 파이프라인

## 아키텍처

```
Airflow → Crawler → Preprocessor → [Clustering, Forecasting] → Targeting → Dashboard
                ↓           ↓                ↓                      ↓
              ES저장      ES저장           ES저장                 ES저장
```

- **PostgreSQL**: 원본 데이터 조회
- **Elasticsearch**: 분석 결과 저장
- **Airflow**: 각 서비스를 HTTP로 오케스트레이션

## MSA 서비스

| 서비스 | 역할 | Output ES Index |
|--------|------|-----------------|
| Crawler | 구글/네이버 트렌드 크롤링 | medi-db-trend-data |
| Preprocessor | PG 데이터 전처리, 키워드 추출 | medi-db-trend-product-mapping |
| Clustering | HDBSCAN, K-Prototype, UMAP, GMM | medi-db-clustering-result |
| Forecasting | 시계열 수요예측, 인기 랭킹 | medi-db-forecasting-result |
| Targeting | 의약품-약국 매칭 | medi-db-targeting-result |
| Dashboard | Streamlit 시각화 | - |

## 기술 스택

- **Language**: Python 3.10+
- **Framework**: FastAPI, Streamlit
- **Orchestration**: Apache Airflow
- **Infra**: Docker, Docker Compose
- **ML/Data**: scikit-learn, hdbscan, umap-learn

## 디렉토리 구조

```
medi-db/
├── airflow/
│   ├── dags/                    # Airflow DAG 정의
│   └── plugins/
├── services/
│   ├── crawler/                 # 트렌드 데이터 수집
│   ├── preprocessor/            # 데이터 전처리
│   ├── clustering/              # 클러스터링 분석
│   ├── forecasting/             # 수요예측
│   ├── targeting/               # 타겟팅 매칭
│   └── dashboard/               # 시각화 대시보드
├── shared/
│   ├── clients/                 # DB 클라이언트 (ES, PG)
│   ├── models/                  # 공통 스키마
│   └── config.py
├── docker-compose.yaml
└── .env.example
```

## 시작하기

### 사전 요구사항

- Docker & Docker Compose
- PostgreSQL 접속 (터널링)
- Elasticsearch 접속 (SSM 터널링)

### 환경 설정

```bash
# 환경변수 설정
cp .env.example .env
# .env 파일에 실제 값 입력
```

### 실행

```bash
# 전체 서비스 실행
docker-compose up -d

# 특정 서비스만 실행
docker-compose up -d crawler preprocessor
```

### 서비스 포트

| 서비스 | 포트 |
|--------|------|
| Airflow | 8080 |
| Crawler | 8001 |
| Preprocessor | 8002 |
| Clustering | 8003 |
| Forecasting | 8004 |
| Targeting | 8005 |
| Dashboard | 8501 |

## ES 인덱스 네이밍 규칙

```
medi-db-{데이터명}-v{버전}  (실제)
medi-db-{데이터명}          (Alias)
```

## 핵심 로직

### 인기 의약품 랭킹 (6:3:1)

```
ranking_score = (판매량_정규화 × 0.6) + (상품명_트렌드_정규화 × 0.3) + (카테고리_트렌드_정규화 × 0.1)
```

### 키워드 추출 소스

- `product.name` → 상품명 핵심어
- `product.efficacy` → 효능 키워드
- `product.ingredient` → 주성분
- `product.category2` → 카테고리 매핑

## License

Private - Platpharm Inc.
