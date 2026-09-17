# Local Data Platform

Một data platform thương mại điện tử chạy hoàn toàn trên máy local nhưng mô phỏng
các ranh giới quan trọng của production: orchestration, incremental ingestion,
analytics engineering, distributed compute, lakehouse, streaming, data quality,
CI và observability.

## Kiến trúc

```text
PostgreSQL source
       │
       ▼
Airflow incremental extract ──► staging PostgreSQL
       │                              │
       │                              ├──► dbt ──► analytics marts
       │                              │
       │                              ├──► Spark ──► Iceberg / MinIO
       │                              │
       │                              └──► Kafka ──► Spark Streaming
       │                                             │
       └──── commit watermark ◄──────────────────────┘

Exporter ──► Prometheus ──► Grafana
                    └─────► Alertmanager
```

Topology chi tiết gồm 26 service definitions, 18 container chạy dài hạn, 3 init
job, 5 one-shot tools, network/port/volume và lý do chọn số node được mô tả tại
[Kiến trúc hệ thống](docs/architecture.md). Tra cứu trách nhiệm từng file tại
[Bản đồ code](docs/code-map.md).

Watermark chỉ được commit sau khi dbt, Iceberg và Kafka publication đều thành
công. Vì không có distributed transaction xuyên ba hệ thống, mọi sink dùng
at-least-once delivery kết hợp khóa ổn định, merge/upsert và checkpoint để retry
không nhân đôi dữ liệu.

## Thành phần

| Thành phần | Vai trò | Địa chỉ local |
| --- | --- | --- |
| PostgreSQL | source, staging, marts, Iceberg catalog | `localhost:5432` |
| Airflow 3 | lịch chạy và dependency | <http://localhost:8080> |
| Spark 3.5 | batch, Iceberg, streaming | <http://localhost:8081> |
| Spark job gateway | API allow-list cho Airflow | <http://localhost:8090/health> |
| Kafka 4 | durable event log | `localhost:9092` |
| MinIO | S3-compatible Iceberg warehouse | <http://localhost:9001> |
| Prometheus | metric và alert rules | <http://localhost:9090> |
| Alertmanager | nhóm/trạng thái alert | <http://localhost:9093> |
| Grafana | dashboard | <http://localhost:3000> |

## Khởi động nhanh

Yêu cầu: Docker Compose v2, tối thiểu khoảng 8 GB RAM dành cho Docker và các
cổng trong bảng trên chưa bị chiếm.

```bash
cp .env.example .env
# Đổi toàn bộ giá trị change-me trước khi dùng lâu dài.

docker compose \
  --profile spark \
  --profile lakehouse \
  --profile streaming \
  --profile monitoring \
  up -d --build

docker compose ps
```

Profile `tools` chỉ chứa job chạy một lần nên không cần bật khi `up`. Các lệnh
thường dùng:

```bash
# Spark batch ra Parquet
docker compose --profile '*' run --rm --no-deps spark-submit

# PostgreSQL -> Iceberg, chạy lại an toàn nhờ MERGE
docker compose --profile '*' run --rm --no-deps iceberg-submit

# Xử lý backlog Kafka rồi thoát
docker compose --profile '*' run --rm --no-deps order-stream-once

# Bảo trì Iceberg
docker compose --profile '*' run --rm --no-deps iceberg-maintenance

# Kiểm tra row count và business grain của các bảng Iceberg
docker compose exec -T spark-gateway python3 -c \
  "import urllib.request; print(urllib.request.urlopen(urllib.request.Request('http://127.0.0.1:8090/jobs/validate-platform', method='POST'), timeout=3600).read().decode())"
```

`--profile '*'` làm Compose render đầy đủ dependency nằm ở profile khác;
`--no-deps` bảo đảm lệnh one-shot dùng stack đang chạy thay vì tạo lại dịch vụ.

Mở Airflow, unpause và trigger DAG `ecommerce_pipeline`. Scheduled run dùng
watermark; backfill dùng cửa sổ `[start, end)` và không thay watermark thường:

```bash
docker compose exec -T airflow-scheduler airflow dags trigger ecommerce_pipeline \
  --conf '{
    "run_mode": "backfill",
    "backfill_start": "2026-08-01T00:00:00+00:00",
    "backfill_end": "2026-08-02T00:00:00+00:00"
  }'
```

## Kiểm thử

```bash
# Static validation
python3 -m py_compile \
  airflow/dags/ecommerce_pipeline.py \
  airflow/dags/lakehouse_maintenance.py \
  kafka/producer/publish_orders.py \
  monitoring/platform_exporter.py \
  spark/gateway/job_gateway.py \
  spark/jobs/*.py \
  spark/tests/*.py
docker compose config --quiet

# dbt: model + snapshot + tests
docker compose exec -T airflow-worker \
  /opt/dbt-venv/bin/dbt build \
  --project-dir /opt/airflow/dbt \
  --profiles-dir /opt/airflow/dbt \
  --target dev --no-partial-parse

# Spark unit tests trong đúng runtime image
docker compose --profile '*' run --rm --no-deps \
  --entrypoint /opt/spark/bin/spark-submit \
  spark-submit --master 'local[2]' /opt/spark/tests/test_sales_batch.py
docker compose --profile '*' run --rm --no-deps \
  --entrypoint /opt/spark/bin/spark-submit \
  spark-submit --master 'local[2]' /opt/spark/tests/test_order_stream.py
```

GitHub Actions gồm hai lớp: `dbt-ci.yml` dựng database tối thiểu rồi chứng minh
incremental/idempotency; `platform-ci.yml` kiểm tra Compose, Python, monitoring
config và build/test các container còn lại.

## Môi trường dbt

- `dev`: schema cá nhân/local, tối ưu vòng lặp phát triển.
- `ci`: schema tạm trong pipeline, được seed dữ liệu nhỏ và xóa sau job.
- `prod`: schema `analytics`; chỉ pipeline triển khai được quyền dùng.

`target` là cấu hình kết nối được chọn lúc chạy, không tự động biến một lệnh
thành production deployment. Luôn truyền `--target` rõ ràng trong automation.

## Tài liệu học và vận hành

Đọc theo thứ tự:

1. [Nền tảng data platform local](docs/learning/01-platform-foundations.md)
2. [Airflow orchestration](docs/learning/02-airflow-orchestration.md)
3. [Data warehouse và SCD2](docs/learning/03-warehouse-modeling.md)
4. [Incremental, idempotency và backfill](docs/learning/04-incremental-backfill.md)
5. [dbt incremental](docs/learning/05-dbt-incremental.md)
6. [Spark batch](docs/learning/06-spark.md)
7. [Iceberg lakehouse](docs/learning/07-iceberg.md)
8. [Kafka streaming](docs/learning/08-kafka.md)
9. [Production integration](docs/learning/09-production-integration.md)
10. [Monitoring và alerting](docs/learning/10-observability.md)
11. [System verification và production trade-offs](docs/learning/11-verification-and-tradeoffs.md)
12. [Kiến trúc logical và physical](docs/architecture.md)
13. [Bản đồ code theo file](docs/code-map.md)
14. [Runbook vận hành](docs/runbook.md)
15. [Roadmap và phạm vi](ROADMAP.txt)

## Phạm vi production-shaped

Project thể hiện đúng dependency, retry boundary, idempotency, CI và khả năng
quan sát, nhưng vẫn là local lab: mỗi dịch vụ chỉ có một node, giao tiếp nội bộ
chưa TLS/SASL, secret nằm trong `.env`, Alertmanager chưa gửi ra Slack/email và
chưa có backup off-host. Spark worker cần 3 GB riêng; toàn stack nên được cấp ít
nhất khoảng 8 GB Docker RAM. Không triển khai nguyên trạng này cho dữ liệu thật.
