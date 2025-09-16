# Stage 1: build dependencies
FROM python:3.11-slim AS builder

WORKDIR /app

# Install build deps for psycopg2 + marker
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    wget \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install requirements first (to leverage Docker layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Pre-download marker-pdf models so Cloud Run doesn't fetch them at runtime
RUN python -c "from marker.models import create_model_dict; create_model_dict()"

# Stage 2: runtime image
FROM python:3.11-slim

WORKDIR /app

# Install only runtime deps
RUN apt-get update && apt-get install -y \
    libpq-dev \
    tesseract-ocr \
    && rm -rf /var/lib/apt/lists/*

# Copy installed packages and pre-fetched models from builder
COPY --from=builder /usr/local /usr/local

# Copy app code
COPY . .

ENV PORT=8080
# Keep only one worker (so model loads once), add threads for concurrency
CMD ["gunicorn", "-b", ":8080", "main:app", "--timeout", "120", "--workers", "1", "--threads", "4"]
