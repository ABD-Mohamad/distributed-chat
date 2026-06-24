FROM python:3.11-slim

WORKDIR /app
COPY auth-service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY auth-service/app/ ./app/

HEALTHCHECK --interval=10s --timeout=3s --start-period=15s --retries=5 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')"
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
