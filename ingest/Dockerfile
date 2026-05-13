FROM python:3.11-slim

# ── Install deps ──────────────────────────────────────────────────────────────
# --prefer-binary avoids source compilation (confluent-kafka & pandas
# both ship pre-built wheels for linux/amd64 and linux/arm64).
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir --prefer-binary -r requirements.txt

# ── Copy application code and data ───────────────────────────────────────────
COPY producer/ ./producer/
COPY data/     ./data/
COPY config.docker.yaml ./config.docker.yaml

# ── Run from inside producer/ so relative imports (event_builder etc.) work ──
WORKDIR /app/producer

ENTRYPOINT ["python3", "simulator.py"]
# Default: use the Docker-specific config (kafka:9092 instead of localhost:9094)
CMD ["--config", "/app/config.docker.yaml"]
