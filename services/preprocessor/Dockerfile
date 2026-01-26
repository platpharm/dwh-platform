FROM python:3.11-slim

WORKDIR /app

# 시스템 패키지 설치
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# 공유 모듈 복사
COPY shared /app/shared

# 서비스별 requirements 복사 및 설치
COPY services/preprocessor/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# 서비스 소스 복사
COPY services/preprocessor/src /app/src

# Python 경로 설정
ENV PYTHONPATH=/app

# 포트 노출
EXPOSE 8000

# 서비스 실행
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]
