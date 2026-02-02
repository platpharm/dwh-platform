# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Overview

Git Subtree-based monorepo containing two data pipeline projects for pharmacy/medical analytics:

- **MediDB**: Medical trend data collection and preprocessing (2 microservices + Airflow)
- **PharmDB**: Parallel data collection from 8 sources for pharmacy taxonomy (no ML)

## Build and Run Commands

### MediDB (Docker Compose)

```bash
cd medi-db
docker-compose build                    # Build all services
docker-compose up -d                    # Start all services
docker-compose up -d crawler preprocessor  # Start specific services
docker-compose logs -f [service-name]   # View logs
docker-compose down                     # Stop all
```

Services run on ports: Crawler (8001), Preprocessor (8002), Airflow (8080).

### PharmDB

```bash
cd pharm-db/crawler
pip install -r requirements.txt
python main.py                          # Run all collectors
python main.py --collectors pharmacy_nic pharmacy_hira  # Run specific collectors
python main.py --workers 5 -v           # Custom worker count with verbose
python main.py --dry-run                # Test configuration
```

## Architecture

### MediDB MSA Pipeline

HTTP-orchestrated microservices with Airflow:

```
Crawlers (Google Trends/Naver DataLab) → Preprocessor (Regex-based NLP)
```

**Tech Stack**: FastAPI 0.109, Airflow 2.7, Elasticsearch (all data via CDC indices)

**NLP**: All text processing is regex-based

**Key files**:
- `shared/clients/es_client.py` - Elasticsearch wrapper (singleton `es_client`)
- `shared/config.py` - Centralized configuration with ESIndex constants
- `shared/models/schemas.py` - Pydantic request/response models

### PharmDB

Plugin-based collector architecture with ThreadPoolExecutor (10 workers default). Pure data collection/ETL system with no ML code.

**Collectors** (8): pharmacy_nic, pharmacy_hira, hospital_nic, hospital_hira, store_semas, medical_type, health_stat, building_ledger

**Key files**:
- `crawler/main.py` - Orchestrator with CLI (COLLECTOR_REGISTRY, CollectorResult)
- `crawler/base_collector.py` - Abstract base with HTTP retry (3 attempts, exponential backoff)
- `crawler/collectors/` - Individual collector implementations
- `crawler/utils/es_client.py` - ES bulk indexing (chunk_size=500)
- `crawler/utils/api_client.py` - PublicDataAPIClient with error code mapping

## Git Subtree Management

```bash
# Pull updates from original repos
git subtree pull --prefix=medi-db https://github.com/platpharm/MediTrend.git main --squash
git subtree pull --prefix=pharm-db https://github.com/platpharm/pharm-clustering.git main --squash

# Push changes back
git subtree push --prefix=medi-db https://github.com/platpharm/MediTrend.git main
git subtree push --prefix=pharm-db https://github.com/platpharm/pharm-clustering.git main
```

## Environment Configuration

Both projects require `.env` files (see `.env.example` in each project):
- Elasticsearch: `localhost:54321` (HTTPS, SSM tunnel required for Prod)
- API keys: data.go.kr (PharmDB), Naver DataLab (MediDB)

### AWS Secrets Manager

Credentials are stored in AWS Secrets Manager:
```bash
# Elasticsearch credentials
aws secretsmanager get-secret-value --secret-id platpharm-elasticsearch-prod --query SecretString --output text

# PharmDB 공공데이터포털 API Key
aws secretsmanager get-secret-value --secret-id pharm-db/data-go-kr-api-key --query SecretString --output text
```

### SSH Tunnels (for local development)

```bash
# Elasticsearch tunnel (port 54321)
ssh -L 54321:elasticsearch-prod-endpoint:9200 bastion-host
```

## Code Style

- **No comments**: Do not add inline comments (`#`) to code. Code should be self-documenting through clear naming. Only exceptions:
  - Docstrings for public modules, classes, and functions
  - Comments explaining non-obvious business logic or algorithmic decisions that cannot be expressed through naming alone
  - Regulatory/compliance annotations required by external standards

## Key Conventions

- **ES Index Naming**: `medi-db-{dataname}-v{version}` with aliases
- **Timezone**: Asia/Seoul
- **Python**: 3.11 for MediDB (Docker), 3.9+ for PharmDB

## Elasticsearch Indices

### CDC Source Indices (MediDB reads via CDC)
- `platpharm.public.orders` / `platpharm.public.orders_detail`
- `platpharm.public.product` / `platpharm.public.account`
- `platpharm.public.product_kims_mapping` / `platpharm.public.kims_edis_indexdb`

### MediDB Pipeline Indices
- `medi-db-raw`, `medi-db-trend-data`, `medi-db-product-mapping`
- `medi-db-preprocessed-order`, `medi-db-preprocessed-product`, `medi-db-preprocessed-pharmacy`

### PharmDB Indices (8)
- `pharmacy_nic`, `pharmacy_hira`, `hospital_nic`, `hospital_hira`
- `store_semas`, `medical_type`, `health_stat`, `building_ledger`

### PharmDB Kibana Data Views

| Data View Name | Index | Time Field |
|----------------|-------|------------|
| PharmDB - 약국 (국립중앙의료원) | pharmacy_nic | collected_at |
| PharmDB - 약국 (건강보험심사평가원) | pharmacy_hira | - |
| PharmDB - 병원 (국립중앙의료원) | hospital_nic | collected_at |
| PharmDB - 병원 (건강보험심사평가원) | hospital_hira | - |
| PharmDB - 상가정보 (소상공인) | store_semas | - |
| PharmDB - 의료기관종별 | medical_type | collected_at |
| PharmDB - 보건의료 통계 | health_stat | collected_at |
| PharmDB - 건축물대장 | building_ledger | - |

## PharmDB Excel Export

데이터 흐름: API/파일 수집 → Elasticsearch 적재 → Excel 내보내기

### 규칙

- **파일명**: 한글 (`약국_국립중앙의료원.xlsx`, `보건의료통계.xlsx`)
- **컬럼명**: 한글 (`기관ID`, `병원명`, `주소`, `환자수` 등)
- **저장 위치**: `pharm-db/excel/`
- **시트명**: `Sheet1` (단일 시트 기본, 필요시 복수 시트)

### Excel 파일 목록

| 파일명 | Collector | ES Index | 주요 컬럼 |
|--------|-----------|----------|-----------|
| 약국_국립중앙의료원.xlsx | pharmacy_nic | pharmacy_nic | 기관ID, 약국명, 주소, 전화번호, 위도, 경도, 운영시간 |
| 약국_건강보험심사평가원.xlsx | pharmacy_hira | pharmacy_hira | 요양기관코드, 약국명, 시도명, 시군구명, 경도, 위도, 개설일자 |
| 병원_국립중앙의료원.xlsx | hospital_nic | hospital_nic | 기관ID, 병원명, 주소, 기관구분명, 응급실운영여부 |
| 병원_건강보험심사평가원.xlsx | hospital_hira | hospital_hira | 요양기관코드, 병원명, 종별코드명, 시도명, 의사총수 |
| 상가정보_소상공인.xlsx | store_semas | store_semas | 상가업소번호, 상호명, 상권업종대분류명, 도로명주소, 경도, 위도 |
| 의료기관종별.xlsx | medical_type | medical_type | 요양기관코드, 기관명, 종별코드명, 의사총수, 간호사수 |
| 보건의료통계.xlsx | health_stat | health_stat | 통계유형, 기준연도, 시도명, 시군구명, 환자수, 요양급여비용총액 |
| 건축물대장.xlsx | building_ledger | building_ledger | 건축물대장PK, 건물명, 대지위치, 연면적, 주용도코드명 |

## PharmDB API Sources

### REST API (data.go.kr)

| Collector | API Service | 응답형식 |
|-----------|-------------|----------|
| pharmacy_nic | B552657/ErmctInsttInfoInqireService | XML |
| pharmacy_hira | B551182/pharmacyInfoService | XML |
| hospital_nic | B552657/HsptlAsembySearchService | XML |
| hospital_hira | B551182/hospInfoServicev2 | XML |
| store_semas | B553077/api/open/sdsc2 | JSON |
| medical_type | B551182/hospInfoServicev2 | JSON/XML |
| building_ledger | 1613000/BldRgstHubService | XML |

### CSV 파일 다운로드 (data.go.kr)

| Collector | 데이터셋 | 데이터셋 ID |
|-----------|----------|------------|
| health_stat (시군구별 진료비) | 의료기관 시군구별 진료비 통계 | 15055561 |
| health_stat (의료기관종별 진료비) | 시도별 의료기관종별 진료비 통계 | 15139381 |

### API 구독 필요 (미활성)

| Collector | API Service | 데이터셋 ID | 비고 |
|-----------|-------------|------------|------|
| health_stat (약품사용정보) | B551182/msupUserInfoService1.2 | 15047819 | data.go.kr 활용 신청 필요 |

## Known Issues

- PharmDB building_ledger requires bldMngNo input, cannot run standalone
- health_stat 약품사용정보 API는 data.go.kr에서 별도 활용 신청 필요 (msupUserInfoService1.2)
- data.go.kr CSV 파일의 `atchFileId`는 데이터 업데이트 시 변경될 수 있음
