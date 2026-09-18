# Local Data Platform

Một data platform thương mại điện tử chạy hoàn toàn trên máy local nhưng mô phỏng
các ranh giới quan trọng của production: orchestration, incremental ingestion,
analytics engineering, distributed compute, lakehouse, streaming, data quality,
CI và observability.

## Trạng thái repository

Code hiện tại là baseline **P0 đã hoàn thành** tại commit `33d76fc`: batch
incremental cho bốn entity, customer CDC end-to-end, schema contract, dbt marts,
Iceberg/Kafka và monitoring local. Đọc [Trạng thái hiện tại](docs/current-state.md)
để biết chính xác chức năng nào đang tồn tại và bằng chứng nào dùng để xác minh.

Các hạng mục production tiếp theo như external alerting/SLO, backup/PITR,
security, HA, lineage và centralized telemetry **chưa nằm trong baseline này**.
Chúng được mô tả cùng lý do, thiết kế dự kiến và Definition of Done tại
[Backlog sau P0](docs/backlog.md).

## Kiến trúc

```text
PostgreSQL source
       │
       ▼
Airflow incremental extract (4 batch tables) ──► staging PostgreSQL
       │                              │
       │                              ├──► dbt ──► analytics marts
       │                              │
       │                              ├──► Spark ──► Iceberg / MinIO
       │                              │
       │                              └──► Kafka ──► Spark Streaming
       │                                             │
       └──── commit watermark ◄──────────────────────┘

PostgreSQL WAL ─► Debezium ─► Avro/Registry ─► CDC Bronze ─► Silver current
                                                               │
                                                               └─► dbt customer SCD2

Exporter ──► Prometheus ──► Grafana
                    └─────► Alertmanager
```

Topology chi tiết gồm 35 service definitions, 21 container chạy dài hạn, 4 init
job, 10 one-shot tools, 9 named volume, network/port và lý do chọn số node được
mô tả tại [Kiến trúc hệ thống](docs/architecture.md). Tra cứu trách nhiệm từng file tại
[Bản đồ code](docs/code-map.md). Để xem theo góc độ người dùng/operator thay vì
theo component, đọc [Platform có thể làm gì?](docs/platform-capabilities.md).

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
| Debezium Connect | PostgreSQL WAL CDC | <http://localhost:8083> |
| Apicurio Registry | Avro schema/compatibility | <http://localhost:8084> |
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

# Chứng minh CDC capture đủ create/update/delete/tombstone
docker compose --profile '*' run --rm --no-deps cdc-smoke-test

# Chứng minh compatibility policy và breaking schema bị từ chối
docker compose --profile '*' run --rm schema-contract-test

# Chứng minh CDC đã apply create/update/delete vào Bronze/Silver
docker compose --profile '*' run --rm cdc-materialization-test

# Đối soát toàn bộ source customer với Silver current state
docker compose --profile '*' run --rm cdc-reconcile

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
  cdc/*.py \
  schema_registry/*.py \
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

GitHub Actions gồm hai lớp: PR dbt dùng baseline manifest + `state:modified+`
và defer; main chạy full initial/idempotent/mutation proof. `platform-ci.yml`
kiểm tra Compose, Python, monitoring config và build/test runtime image.

## Môi trường dbt

- `dev`: schema cá nhân/local, tối ưu vòng lặp phát triển.
- `ci`: schema tạm trong pipeline, được seed dữ liệu nhỏ và xóa sau job.
- `prod`: schema `analytics`; chỉ pipeline triển khai được quyền dùng.

`target` là cấu hình kết nối được chọn lúc chạy, không tự động biến một lệnh
thành production deployment. Luôn truyền `--target` rõ ràng trong automation.

## Tài liệu học và vận hành

Đọc theo thứ tự, hoặc bắt đầu từ [mục lục theo vai trò](docs/learning/README.md):

1. [Trạng thái P0 thực tế](docs/current-state.md)
2. [Nền tảng data platform local](docs/learning/01-platform-foundations.md)
3. [Airflow orchestration](docs/learning/02-airflow-orchestration.md)
4. [Data warehouse và SCD2](docs/learning/03-warehouse-modeling.md)
5. [Incremental, idempotency và backfill](docs/learning/04-incremental-backfill.md)
6. [dbt incremental](docs/learning/05-dbt-incremental.md)
7. [Spark batch](docs/learning/06-spark.md)
8. [Iceberg lakehouse](docs/learning/07-iceberg.md)
9. [Kafka streaming](docs/learning/08-kafka.md)
10. [Production integration](docs/learning/09-production-integration.md)
11. [Monitoring và alerting](docs/learning/10-observability.md)
12. [System verification và production trade-offs](docs/learning/11-verification-and-tradeoffs.md)
13. [CDC với PostgreSQL và Debezium](docs/learning/12-cdc-debezium.md)
14. [Schema Registry và Avro](docs/learning/13-schema-registry-avro.md)
15. [CDC Bronze/Silver và cutover](docs/learning/14-cdc-bronze-silver-cutover.md)
16. [dbt contracts và slim CI](docs/learning/15-dbt-contracts-slim-ci.md)
17. [Bản đồ nhập môn từ dbt incremental đến CDC](docs/learning/00-beginner-map.md)
18. [Kiến trúc logical và physical](docs/architecture.md)
19. [Bản đồ code theo file](docs/code-map.md)
20. [Runbook vận hành](docs/runbook.md)
21. [Roadmap và phạm vi](ROADMAP.txt)
22. [Functional capabilities](docs/platform-capabilities.md)
23. [Backlog chưa hoàn thành](docs/backlog.md)

## Phạm vi production-shaped

Project thể hiện đúng dependency, retry boundary, idempotency, CI và khả năng
quan sát, nhưng vẫn là local lab: mỗi dịch vụ chỉ có một node, giao tiếp nội bộ
chưa TLS/SASL, secret nằm trong `.env`, Alertmanager chưa gửi ra Slack/email và
chưa có backup off-host. Spark worker cần 3 GB riêng; toàn stack nên được cấp ít
nhất khoảng 8 GB Docker RAM. Không triển khai nguyên trạng này cho dữ liệu thật.

Checklist “đã có / local-only / còn thiếu” theo correctness, HA, security,
observability, DR, delivery, scale và governance nằm trong
[System verification và production trade-offs](docs/learning/11-verification-and-tradeoffs.md).
Danh sách công việc chưa làm và tiêu chí hoàn thành nằm trong
[Backlog sau P0](docs/backlog.md).
