FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    curl \
    git \
    make \
    autoconf \
    automake \
    mecab \
    libmecab-dev \
    mecab-ipadic-utf8 \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://bitbucket.org/eunjeon/mecab-ko-dic/downloads/mecab-ko-dic-2.1.1-20180720.tar.gz -o mecab-ko-dic.tar.gz \
    && tar -xzf mecab-ko-dic.tar.gz \
    && cd mecab-ko-dic-2.1.1-20180720 \
    && ./autogen.sh || true \
    && ./configure \
    && make \
    && make install \
    && cd .. && rm -rf mecab-ko-dic-2.1.1-20180720 mecab-ko-dic.tar.gz \
    && ldconfig

COPY shared /app/shared

COPY services/preprocessor/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY services/preprocessor/src /app/src

ENV PYTHONPATH=/app

EXPOSE 8000

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]
