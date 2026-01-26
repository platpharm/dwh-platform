# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Overview

Git Subtree-based monorepo containing two data pipeline projects for pharmacy/medical analytics:

- **MediTrend**: MSA-based medical trend analysis and demand forecasting (6 microservices + Airflow)
- **pharm-clustering**: Parallel data collection from 10 sources for pharmacy taxonomy

## Build and Run Commands

### MediTrend (Docker Compose)

```bash
cd MediTrend
docker-compose build                    # Build all services
docker-compose up -d                    # Start all services
docker-compose up -d crawler preprocessor  # Start specific services
docker-compose logs -f [service-name]   # View logs
docker-compose down                     # Stop all
```

Services run on ports: Crawler (8001), Preprocessor (8002), Clustering (8003), Forecasting (8004), Targeting (8005), Dashboard (8501), Airflow (8080).

### pharm-clustering

```bash
cd pharm-clustering/crawler
pip install -r requirements.txt
python main.py                          # Run all collectors
python main.py --collectors pharmacy_nic pharmacy_hira  # Run specific collectors
python main.py --workers 5 -v           # Custom worker count with verbose
python main.py --dry-run                # Test configuration
```

## Architecture

### MediTrend MSA Pipeline

HTTP-orchestrated microservices with Airflow:

```
Crawlers (Naver/Google/Papers) → Preprocessor (KoNLPy) → [Clustering | Forecasting] → Targeting → Dashboard
```

**Tech Stack**: FastAPI 0.109, Airflow 2.7, PostgreSQL (source), Elasticsearch (results), Streamlit (dashboard)

**ML Libraries**: scikit-learn, HDBSCAN, UMAP, KModes, Prophet, Statsmodels

**Key files**:
- `airflow/dags/meditrend_main_pipeline.py` - Main daily pipeline (2 AM UTC)
- `shared/clients/pg_client.py`, `shared/clients/es_client.py` - Database wrappers
- `shared/config.py` - Centralized configuration

### pharm-clustering

Plugin-based collector architecture with ThreadPoolExecutor (10 workers default):

**Collectors**: pharmacy_nic, pharmacy_hira, hospital_nic, hospital_hira, review_naver, store_semas, medical_type, health_stat, order_platpharm, building_ledger

**Key files**:
- `crawler/main.py` - Orchestrator with CLI
- `crawler/base_collector.py` - Abstract base with retry logic
- `crawler/collectors/` - Individual collector implementations

## Git Subtree Management

```bash
# Pull updates from original repos
git subtree pull --prefix=MediTrend https://github.com/platpharm/MediTrend.git main --squash
git subtree pull --prefix=pharm-clustering https://github.com/platpharm/pharm-clustering.git main --squash

# Push changes back
git subtree push --prefix=MediTrend https://github.com/platpharm/MediTrend.git main
git subtree push --prefix=pharm-clustering https://github.com/platpharm/pharm-clustering.git main
```

## Environment Configuration

Both projects require `.env` files (see `.env.example` in each project):
- PostgreSQL tunnel: `host.docker.internal:12345`
- Elasticsearch credentials
- API keys: Naver Search API, data.go.kr

## Key Conventions

- **ES Index Naming**: `medi-trend-{dataname}-v{version}` with aliases
- **Scoring Logic**: `popularity = (sales_normalized × 0.7) + (trend_normalized × 0.3)`
- **Timezone**: Asia/Seoul
- **Python**: 3.10+ for MediTrend, 3.9+ for pharm-clustering
