# Lightweight image for the producer and replay services.
FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir .
ENTRYPOINT []
CMD ["hsl-producer"]
