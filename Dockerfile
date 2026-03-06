# Build stage
FROM python:3.11-slim as builder

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Runtime stage
FROM python:3.11-slim

COPY --from=builder /usr/local/lib/python3.11/site-packages/ /usr/local/lib/python3.11/site-packages/
COPY --from=builder /usr/local/bin/ /usr/local/bin/

WORKDIR /app
COPY . .

# Let Playwright install chromium + ALL its required system dependencies
RUN python -m playwright install --with-deps chromium

# Data directory for SQLite (mount Railway volume here)
RUN mkdir -p /data
ENV DB_PATH=/data/linkedin_ads.db

ENV PYTHONUNBUFFERED=1
ENV PORT=8080
ENV PYTHONPATH=/app

CMD ["python", "-m", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
