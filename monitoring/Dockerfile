FROM python:3.12-slim

WORKDIR /app

COPY monitoring/requirements.txt /app/requirements.txt
RUN python -m pip install --no-cache-dir --requirement /app/requirements.txt

COPY monitoring/platform_exporter.py /app/platform_exporter.py

# Exporter configuration arrives through environment variables; no filesystem
# write or privileged capability is needed at runtime.
USER 65534

ENTRYPOINT ["python", "/app/platform_exporter.py"]
