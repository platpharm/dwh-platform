FROM python:3.11-slim

WORKDIR /app

# 시스템 패키지 설치 (MeCab 및 한국어 형태소 분석기 포함)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    libpq-dev \
    curl \
    git \
    make \
    autoconf \
    automake \
    mecab \
    libmecab-dev \
    mecab-ipadic-utf8 \
    && rm -rf /var/lib/apt/lists/*

# mecab-ko-dic 설치 (한국어 사전)
RUN curl -fsSL https://bitbucket.org/eunjeon/mecab-ko-dic/downloads/mecab-ko-dic-2.1.1-20180720.tar.gz -o mecab-ko-dic.tar.gz \
    && tar -xzf mecab-ko-dic.tar.gz \
    && cd mecab-ko-dic-2.1.1-20180720 \
    && ./autogen.sh || true \
    && ./configure \
    && make \
    && make install \
    && cd .. && rm -rf mecab-ko-dic-2.1.1-20180720 mecab-ko-dic.tar.gz \
    && ldconfig

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
