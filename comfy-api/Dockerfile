FROM python:3.12-slim

WORKDIR /app

RUN pip install "fastapi" "uvicorn[standard]" pika requests websockets wsproto

COPY app.py /app/app.py

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "9000", "--ws", "websockets"]
