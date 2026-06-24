FROM python:3.11-slim

WORKDIR /app
COPY chat-service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY proto/ ./proto/
RUN python -m grpc_tools.protoc -I proto --python_out=. --grpc_python_out=. proto/chat.proto
COPY chat-service/app/ ./app/

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
