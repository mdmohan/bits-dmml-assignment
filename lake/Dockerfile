FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir --prefer-binary -r requirements.txt

COPY consumer/*.py /app/
COPY config.docker.yaml /app/config.docker.yaml

WORKDIR /app/

ENTRYPOINT ["python3", "/app/dumper.py", "--config", "/app/config.docker.yaml"]
