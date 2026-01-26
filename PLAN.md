# DWH Platform Deployment Plan

## 병렬 에이전트로 수행 가능한 작업 목록

### Phase 1: 서비스 준비 (병렬 수행 가능)

| 에이전트 | 작업 | 설명 |
|---------|------|------|
| Agent-1 | MediTrend Crawler 서비스 수정/빌드 | psycopg2, shared/models 의존성 수정 |
| Agent-2 | MediTrend Preprocessor 서비스 수정/빌드 | 의존성 확인 및 빌드 |
| Agent-3 | MediTrend Clustering 서비스 수정/빌드 | 의존성 확인 및 빌드 |
| Agent-4 | MediTrend Forecasting 서비스 수정/빌드 | models 디렉토리 생성 및 빌드 |
| Agent-5 | MediTrend Targeting 서비스 수정/빌드 | 의존성 확인 및 빌드 |
| Agent-6 | MediTrend Dashboard 서비스 수정/빌드 | 의존성 확인 및 빌드 |
| Agent-7 | Pharm-Clustering 서비스 빌드 | Dockerfile 확인 및 빌드 |
| Agent-8 | Monitoring 서비스 빌드 | 모니터링 대시보드 빌드 |

### Phase 2: 서비스 시작 (의존성에 따라 순차/병렬)

```
[병렬 그룹 1 - 독립 서비스]
├── airflow-postgres
├── monitoring

[병렬 그룹 2 - Airflow 초기화 후]
├── airflow-webserver
├── airflow-scheduler

[병렬 그룹 3 - MSA 독립 서비스]
├── meditrend-crawler
├── meditrend-preprocessor
├── meditrend-clustering
├── meditrend-forecasting

[순차 그룹 - 의존성 있음]
├── meditrend-targeting (depends: clustering, forecasting)
└── meditrend-dashboard (depends: targeting)
```

### Phase 3: 헬스체크 및 모니터링 (병렬 수행 가능)

| 에이전트 | 작업 | 엔드포인트 |
|---------|------|-----------|
| Monitor-1 | Crawler 헬스체크 | http://localhost:8001/health |
| Monitor-2 | Preprocessor 헬스체크 | http://localhost:8002/health |
| Monitor-3 | Clustering 헬스체크 | http://localhost:8003/health |
| Monitor-4 | Forecasting 헬스체크 | http://localhost:8004/health |
| Monitor-5 | Targeting 헬스체크 | http://localhost:8005/health |
| Monitor-6 | Dashboard 헬스체크 | http://localhost:8501 |
| Monitor-7 | Airflow 헬스체크 | http://localhost:8080/health |
| Monitor-8 | Monitoring 대시보드 | http://localhost:8888/health |

### Phase 4: 오류 수정 (병렬 리서치 에이전트)

| 리서치 에이전트 | 담당 서비스 | 작업 |
|----------------|------------|------|
| Research-1 | Crawler | 로그 분석 및 오류 진단 |
| Research-2 | Preprocessor | 로그 분석 및 오류 진단 |
| Research-3 | Clustering | 로그 분석 및 오류 진단 |
| Research-4 | Forecasting | 로그 분석 및 오류 진단 |
| Research-5 | Targeting | 로그 분석 및 오류 진단 |
| Research-6 | Dashboard | 로그 분석 및 오류 진단 |

## 현재 상태

### 실행 중인 서비스
- [x] dwh-airflow-postgres (healthy)
- [x] dwh-airflow-webserver (healthy)
- [x] dwh-airflow-scheduler (running)
- [x] dwh-monitoring (healthy)
- [ ] dwh-meditrend-targeting (healthy but ES 연결 필요)

### 수정 필요 서비스
- [ ] dwh-meditrend-crawler (unhealthy - ES 연결 실패)
- [ ] dwh-meditrend-preprocessor (unhealthy - ES 연결 실패)
- [ ] dwh-meditrend-clustering (unhealthy - ES 연결 실패)
- [ ] dwh-meditrend-forecasting (starting - 확인 필요)
- [ ] dwh-meditrend-dashboard (unhealthy - 의존성 문제)

## 서비스 포트 매핑

| 서비스 | 포트 | 설명 |
|--------|------|------|
| Airflow Webserver | 8080 | Airflow UI (admin/admin) |
| Crawler | 8001 | 크롤링 서비스 |
| Preprocessor | 8002 | 전처리 서비스 |
| Clustering | 8003 | 클러스터링 서비스 |
| Forecasting | 8004 | 수요예측 서비스 |
| Targeting | 8005 | 타겟팅 서비스 |
| Dashboard | 8501 | Streamlit 대시보드 |
| Monitoring | 8888 | 모니터링 대시보드 |

## 병렬 수행 명령어

### 모든 서비스 병렬 빌드
```bash
docker-compose build \
  meditrend-crawler \
  meditrend-preprocessor \
  meditrend-clustering \
  meditrend-forecasting \
  meditrend-targeting \
  meditrend-dashboard \
  monitoring \
  --parallel
```

### 서비스 병렬 헬스체크
```bash
curl -s http://localhost:8001/health & \
curl -s http://localhost:8002/health & \
curl -s http://localhost:8003/health & \
curl -s http://localhost:8004/health & \
curl -s http://localhost:8005/health & \
curl -s http://localhost:8888/health & \
wait
```

## 다음 단계

1. **병렬 에이전트로 각 서비스 오류 진단 및 수정**
2. **병렬 에이전트로 헬스체크 모니터링**
3. **오케스트레이션 에이전트 구현**
