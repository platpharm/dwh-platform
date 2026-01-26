# Pharm Clustering

약국 택소노미 구축을 위한 데이터 수집 크롤러

## 개요

9개 데이터 소스에서 약국/병원/상가 정보를 병렬 수집하여 Elasticsearch에 저장합니다.

## 데이터 소스

| 수집기 | 데이터 소스 | 설명 |
|--------|-------------|------|
| `pharmacy_nic` | 국립중앙의료원 | 약국 정보 |
| `pharmacy_hira` | 건강보험심사평가원 | 약국 정보 |
| `hospital_nic` | 국립중앙의료원 | 병원 정보 |
| `hospital_hira` | 건강보험심사평가원 | 병원 정보 |
| `store_semas` | 소상공인시장진흥공단 | 상가 정보 |
| `medical_type` | 건강보험심사평가원 | 의료기관종별 |
| `health_stat` | 건강보험심사평가원 | 보건의료 통계 |
| `order_platpharm` | 플랫팜 RDBMS | 의약품 발주 |
| `building_ledger` | 국토교통부 | 건축물대장 (면적) |

## 설치

```bash
cd crawler
pip install -r requirements.txt
```

## 환경 변수 설정

`.env.example`을 복사하여 `.env` 파일을 생성하고 값을 설정합니다.

```bash
cp .env.example .env
```

```env
# 공공데이터 API (data.go.kr)
DATA_GO_KR_API_KEY=your_api_key

# 네이버 API
NAVER_CLIENT_ID=your_client_id
NAVER_CLIENT_SECRET=your_client_secret

# Elasticsearch
ES_HOST=localhost
ES_PORT=54321

# RDBMS (플랫팜)
DB_HOST=localhost
DB_PORT=12345
DB_USER=your_user
DB_PASSWORD=your_password
DB_NAME=platpharm
```

## 사용법

### 전체 수집기 실행

```bash
python crawler/main.py
```

### 특정 수집기만 실행

```bash
python crawler/main.py --collectors pharmacy_nic pharmacy_hira
```

### 옵션

```bash
python crawler/main.py --help

Options:
  -c, --collectors    실행할 수집기 목록
  -w, --workers       병렬 워커 수 (기본값: 10)
  -d, --dry-run       실제 실행 없이 설정 확인
  -v, --verbose       상세 로그 출력
  -l, --list          사용 가능한 수집기 목록 출력
```

## 프로젝트 구조

```
crawler/
├── main.py              # 병렬 실행 오케스트레이터
├── config.py            # 환경 변수 설정
├── base_collector.py    # 기본 수집기 클래스
├── requirements.txt
├── collectors/
│   ├── pharmacy_nic.py
│   ├── pharmacy_hira.py
│   ├── hospital_nic.py
│   ├── hospital_hira.py
│   ├── store_semas.py
│   ├── medical_type.py
│   ├── health_stat.py
│   ├── order_platpharm.py
│   └── building_ledger.py
└── utils/
    ├── api_client.py    # 공공데이터 API 클라이언트
    └── es_client.py     # Elasticsearch 클라이언트
```

## 기술 스택

- Python 3.9+
- Elasticsearch 8.x
- PostgreSQL (플랫팜 발주 데이터)
