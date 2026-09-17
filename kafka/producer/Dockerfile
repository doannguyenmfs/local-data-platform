FROM python:3.12-slim

WORKDIR /app

COPY kafka/producer/requirements.txt /app/requirements.txt
RUN python -m pip install --no-cache-dir --requirement /app/requirements.txt

COPY kafka/producer/publish_orders.py /app/publish_orders.py

USER 65534

ENTRYPOINT ["python", "/app/publish_orders.py"]
