FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY exporter.py ./
COPY config.yaml ./
EXPOSE 8000
ENTRYPOINT ["python", "exporter.py", "--config", "config.yaml", "--port", "8000"]
