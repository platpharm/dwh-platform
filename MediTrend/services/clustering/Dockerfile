FROM python:3.11-slim

WORKDIR /app

# Install system dependencies for HDBSCAN and scientific computing
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    g++ \
    gfortran \
    libopenblas-dev \
    liblapack-dev \
    pkg-config \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip and install build tools first
RUN pip install --upgrade pip setuptools wheel

# Install Cython and numpy first (required for hdbscan build)
RUN pip install --no-cache-dir "cython>=0.29.0,<3.0" "numpy==1.26.3"

# Copy shared modules first (context is project root)
COPY shared/ /app/shared/

# Copy service requirements and install
COPY services/clustering/requirements.txt /app/requirements.txt

# Install all requirements
RUN pip install --no-cache-dir -r requirements.txt

# Copy service source code
COPY services/clustering/src/ /app/src/

# Set Python path for shared modules
ENV PYTHONPATH=/app

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Run the application
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]
