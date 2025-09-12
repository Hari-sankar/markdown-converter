FROM python:3.11-slim

# Install system deps
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install
COPY requirements.txt /app/requirements.txt
WORKDIR /app
RUN pip install --no-cache-dir -r requirements.txt

# Copy app
COPY . /app

ENV PORT=8080
CMD ["gunicorn", "-b", ":8080", "main:app", "--timeout", "120", "--workers", "1"]
