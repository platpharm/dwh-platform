Generate a comprehensive pipeline report for this DWH platform monorepo.

## Instructions

Analyze the actual source code of both MediDB and PharmDB pipelines, then generate a structured report in Korean. The report must reflect the CURRENT state of the codebase, not cached or static information.

### Step 1: Analyze MediDB Pipeline

Read and analyze the following files to understand the current pipeline:

**Services (read each `src/main.py` and key modules):**
1. `medi-db/services/crawler/src/` - Trend data crawlers (Google Trends, Naver DataLab)
2. `medi-db/services/preprocessor/src/processors/` - All 4 processors (order, product, pharmacy, keyword)

**Shared:**
- `medi-db/shared/config.py` - ES index definitions
- `medi-db/shared/models/schemas.py` - Data models

### Step 2: Analyze PharmDB Pipeline

Read and analyze:
- `pharm-db/crawler/main.py` - Orchestrator
- `pharm-db/crawler/base_collector.py` - Base class
- `pharm-db/crawler/collectors/` - All collector implementations
- `pharm-db/crawler/config.py` - Configuration
- `pharm-db/crawler/utils/` - ES client, API client

### Step 3: Generate Report

Output the report in the following structure (in Korean):

```
# DWH Platform 파이프라인 리포트

생성일시: [current timestamp]

---

## 1. 시스템 아키텍처 개요
- 전체 구조도 (ASCII diagram)
- 기술 스택 요약

## 2. MediDB 파이프라인

### 2.1 데이터 수집 (Crawler Service - Port 8001)
- 수집 소스 및 방법
- 입력/출력 데이터 구조
- ES 인덱스: 입력 → 출력

### 2.2 전처리 (Preprocessor Service - Port 8002)
각 프로세서별:
- 입력 데이터 (CDC 인덱스)
- 변환 로직 상세
- 출력 데이터 및 인덱스

## 3. PharmDB 파이프라인

### 3.1 오케스트레이션
- 실행 방법 및 CLI 옵션
- ThreadPoolExecutor 병렬 처리 구조

### 3.2 컬렉터 상세
각 컬렉터별:
- 데이터 소스 (API)
- 수집 데이터 종류 및 필드
- 데이터 변환 로직
- 출력 ES 인덱스
- 알려진 제한사항

## 4. Elasticsearch 인덱스 맵

### 4.1 CDC 소스 인덱스 (원본 데이터)
### 4.2 MediDB 파이프라인 인덱스 (가공 데이터)
### 4.3 PharmDB 인덱스

## 5. 데이터 흐름도
- 전체 End-to-End 데이터 흐름 (ASCII diagram)
- 각 스테이지별 입력/출력 테이블
```

### Important Notes
- Read the ACTUAL source code before generating each section
- Include specific variable names, weights, formulas from the code
- Mark any hardcoded values or magic numbers found
- Note any TODO/FIXME/known issues found in the code
- The report should be detailed enough for a new team member to understand the entire system
