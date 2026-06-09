FROM python:3.12-slim
WORKDIR /app
RUN pip install pika requests
COPY worker.py /app/worker.py
CMD ["python", "/app/worker.py"]
