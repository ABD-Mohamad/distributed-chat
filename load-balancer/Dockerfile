FROM python:3.11-slim

WORKDIR /app
COPY load-balancer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY load-balancer/app/ ./app/

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
