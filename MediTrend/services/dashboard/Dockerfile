FROM python:3.11-slim

WORKDIR /app

# System dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy shared modules first
COPY shared/ /app/shared/

# Copy service requirements and install
COPY services/dashboard/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy service source code
COPY services/dashboard/src/ /app/src/

# Set Python path to include shared modules
ENV PYTHONPATH=/app

# Expose Streamlit port
EXPOSE 8501

# Health check
HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health || exit 1

# Run Streamlit
CMD ["streamlit", "run", "src/app.py", "--server.port=8501", "--server.address=0.0.0.0"]
