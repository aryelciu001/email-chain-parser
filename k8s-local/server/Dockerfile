FROM python:3.11-slim
WORKDIR /app
RUN pip install confluent-kafka requests
COPY main.py .
COPY frontend/ frontend/
CMD ["python", "main.py"]
