# DWH Platform Deployment Plan

## 현재 상태 (2026-01-26 업데이트)

### 서비스 상태

| 서비스 | 상태 | 포트 | 비고 |
|--------|------|------|------|
| airflow-postgres | ✅ healthy | - | 내부용 |
| airflow-webserver | ✅ healthy | 8080 | admin/admin |
| airflow-scheduler | ✅ running | - | |
| meditrend-crawler | ✅ healthy | 8001 | Google Trends, Papers |
| meditrend-preprocessor | ✅ healthy | 8002 | ES 인덱싱 추가됨 |
| meditrend-clustering | ✅ healthy | 8003 | HDBSCAN 작동 확인 |
| meditrend-forecasting | ✅ healthy | 8004 | 주문 데이터 필요 |
| meditrend-targeting | ✅ healthy | 8005 | |
| meditrend-dashboard | ✅ healthy | 8501 | Streamlit |
| monitoring | ✅ healthy | 8888 | 통합 모니터링 |

### ES 인덱스 현황

| 인덱스 | 문서 수 | 용도 |
|--------|---------|------|
| medi-trend-trend-data | 155 | Google Trends 데이터 |
| medi-trend-product-mapping | 33,728 | 트렌드-상품 매핑 |
| medi-trend-preprocessed-order | 432,915 | 주문 데이터 |
| medi-trend-clustering-result | 10,000 | 클러스터링 결과 |
| medi-trend-forecasting-result | 27,660 | 수요예측 결과 |
| medi-trend-ranking-result | 1,422 | 인기도 랭킹 |

---

## 완료된 작업

### Phase 1: 인프라 설정 ✅
- [x] Docker Compose 통합 설정
- [x] ES/PG 연결 설정 (AWS Secrets Manager)
- [x] 모니터링 서비스 구축

### Phase 2: 코드 수정 ✅
- [x] Naver API 제거 (전체 코드베이스)
- [x] ES HTTPS 연결 수정 (`verify_certs=False`)
- [x] Order processor DB 스키마 수정 (`ordered_at` → `created_at`, `qty` → `order_qty`)
- [x] Clustering ES 인덱싱 수정 (`entity_id` type: integer → long)
- [x] Order processor ES 인덱싱 추가

### Phase 3: 파이프라인 검증 ✅
- [x] Google Trends 크롤러 실행 (155건)
- [x] Preprocessor 상품 처리 (14,855건 → 33,728 매핑)
- [x] Clustering 실행 (5개 클러스터, 10,000건)

---

### Phase 4: Forecasting 검증 ✅
- [x] Order 데이터 ES 인덱싱 (432,915건)
- [x] Forecasting 서비스 실행 (27,660건)
- [x] 랭킹 계산 실행 (1,422건)

### Phase 5: Targeting 검증 ✅
- [x] Targeting 서비스 실행 (매칭 로직 작동 확인)

---

## 남은 작업

### Phase 6: Dashboard 검증
- [ ] Dashboard에서 전체 결과 시각화 확인
- [ ] 약국 클러스터링 데이터 추가 (현재 상품만 클러스터링됨)

---

## 데이터 파이프라인

```
[크롤링]
Google Trends ──────────────────────┐
Papers (PubMed, arXiv) ─────────────┤
                                    ▼
                          medi-trend-trend-data (ES)
                                    │
[전처리]                            │
PostgreSQL (상품 14,855건) ─────────┼──► Preprocessor
                                    │         │
                                    ▼         ▼
                     medi-trend-product-mapping (33,728건)
                                    │
[알고리즘]                          │
        ┌───────────────────────────┴───────────────────────────┐
        ▼                                                       ▼
   Clustering (HDBSCAN)                              Forecasting (Prophet)
        │                                                       │
        ▼                                                       ▼
medi-trend-clustering-result                    medi-trend-forecasting-result
   (10,000건, 5 clusters)                            (주문 데이터 필요)
        │                                                       │
        └───────────────────────┬───────────────────────────────┘
                                ▼
                           Targeting
                                │
                                ▼
                    medi-trend-targeting-result
                                │
                                ▼
                           Dashboard
```

---

## 환경 변수

```bash
# .env 파일 (AWS Secrets Manager에서 가져옴)
ES_HOST=host.docker.internal
ES_PORT=54321
ES_SCHEME=https
ES_USERNAME=platpharm
ES_PASSWORD=***

PG_HOST=host.docker.internal
PG_PORT=12345
PG_DATABASE=platpharm
PG_USER=postgres
PG_PASSWORD=***
```

---

## 명령어 참조

### 전체 서비스 시작
```bash
docker-compose up -d
```

### 파이프라인 수동 실행
```bash
# 1. 크롤링
curl -X POST http://localhost:8001/crawl/google \
  -H "Content-Type: application/json" \
  -d '{"keywords": ["비타민", "영양제", "감기약"]}'

# 2. 전처리
curl -X POST http://localhost:8002/preprocess/products
curl -X POST http://localhost:8002/preprocess/orders

# 3. 클러스터링
curl -X POST http://localhost:8003/cluster/run \
  -H "Content-Type: application/json" \
  -d '{"algorithm": "hdbscan", "entity_type": "product"}'

# 4. 수요예측
curl -X POST http://localhost:8004/forecast/run \
  -H "Content-Type: application/json" \
  -d '{"days_ahead": 30}'

# 5. 타겟팅
curl -X POST http://localhost:8005/target/run \
  -H "Content-Type: application/json" \
  -d '{"top_n_products": 100}'
```

### 상태 확인
```bash
# 모니터링 대시보드
curl http://localhost:8888/

# ES 인덱스 확인
curl http://localhost:8888/es/indices
```
