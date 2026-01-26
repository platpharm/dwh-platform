FROM python:3.11-slim

WORKDIR /app

# 시스템 의존성 설치
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Python 의존성 설치
COPY services/targeting/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 공유 모듈 복사
COPY shared /app/shared

# 서비스 코드 복사
COPY services/targeting/src /app/src

# 환경 변수
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

EXPOSE 8000

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]
