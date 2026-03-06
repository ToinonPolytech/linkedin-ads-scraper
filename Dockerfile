# Build stage
FROM python:3.11-slim as builder

RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Runtime stage
FROM python:3.11-slim

COPY --from=builder /usr/local/lib/python3.11/site-packages/ /usr/local/lib/python3.11/site-packages/
COPY --from=builder /usr/local/bin/ /usr/local/bin/

# Playwright runtime deps
RUN apt-get update && apt-get install -y \
    libgconf-2-4 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libasound2 \
    libpango-1.0-0 \
    libpangocairo-1.0-0 \
    libnspr4 \
    libnss3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .

RUN python -m playwright install chromium
RUN python -m playwright install-deps

# Data directory for SQLite (mount Railway volume here)
RUN mkdir -p /data
ENV DB_PATH=/data/linkedin_ads.db

ENV PYTHONUNBUFFERED=1
ENV PORT=8080
ENV PYTHONPATH=/app

CMD ["python", "-m", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
