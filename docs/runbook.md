# Runbook vận hành Local Data Platform

> Điều hướng: [Trung tâm tài liệu](README.md) · [Kiến trúc](architecture.md) ·
> [Trạng thái P0](current-state.md) · [Bản đồ code](code-map.md)

Tài liệu này ưu tiên hành động. Phần “tại sao” nằm trong `docs/learning`.
Topology, node/container, port và volume được giải thích trong
[`docs/architecture.md`](architecture.md); trách nhiệm từng file nằm trong
[`docs/code-map.md`](code-map.md).

Runbook này chỉ vận hành baseline P0 mô tả trong
[`current-state.md`](current-state.md). Các lệnh cho external alerting, backup,
security, HA, lineage hay centralized telemetry chưa tồn tại; đó là công việc
trong [`backlog.md`](backlog.md), không phải bước setup bị thiếu ở đây.

## 1. Chuẩn bị và khởi động

```bash
cp .env.example .env
# Đổi password/secret change-me; không commit .env.

docker compose \
  --profile spark \
  --profile lakehouse \
  --profile streaming \
  --profile monitoring \
  up -d --build

docker compose ps
```

Trạng thái mong đợi:

- PostgreSQL, Redis, Kafka, Schema Registry, Debezium Connect và MinIO healthy.
- `airflow-init`, `minio-init`, `kafka-init`, `cdc-bootstrap` exited với code 0; đây là đúng vì
  chúng là init job.
- Airflow services, Spark master/worker/gateway, order stream, customer CDC
  materializer, exporter, Prometheus, Alertmanager và Grafana đang running.

Kiểm tra nhanh:

```bash
curl --fail http://localhost:8090/health
curl --fail http://localhost:8083/connectors/ecommerce-postgres-cdc-avro/status
curl --fail http://localhost:8084/apis/ccompat/v7/config
curl --fail http://localhost:9100/metrics
curl --fail http://localhost:9090/-/healthy
curl --fail http://localhost:3000/api/health

docker compose exec -T kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:19092 \
  --describe --topic ecommerce.order-events.v1
```

## 2. Chạy pipeline

Scheduled/manual incremental run: trigger `ecommerce_pipeline` trong Airflow.
Thứ tự phải là:

```text
validate config
  -> 4 batch extract song song (customer do CDC Silver sở hữu)
  -> record-count validation
  -> relationship validation
  -> dbt + Iceberg + Kafka song song
  -> advance watermark
```

Backfill không thay committed watermark:

```bash
docker compose exec -T airflow-scheduler airflow dags trigger ecommerce_pipeline \
  --conf '{
    "run_mode": "backfill",
    "backfill_start": "2026-08-01T00:00:00+00:00",
    "backfill_end": "2026-08-02T00:00:00+00:00"
  }'
```

Luôn dùng ISO-8601 có timezone, cửa sổ half-open `[start, end)` để hai backfill
liền nhau không chồng record ở biên.

## 3. Xác minh dữ liệu

dbt:

```bash
docker compose exec -T airflow-worker \
  /opt/dbt-venv/bin/dbt build \
  --project-dir /opt/airflow/dbt \
  --profiles-dir /opt/airflow/dbt \
  --target dev --no-partial-parse
```

Watermark/candidate (shell hiện tại phải có biến từ `.env`):

```bash
set -a; source .env; set +a
docker compose exec -T postgres psql \
  -U "$ECOMMERCE_POSTGRES_USER" -d "$ECOMMERCE_POSTGRES_DB" \
  -c "select pipeline_name, active, watermark_value, candidate_value, updated_at from metadata.etl_watermark order by 1;"
```

Kafka:

```bash
docker compose exec -T kafka \
  /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:19092 \
  --all-groups --describe
```

CDC connector và PostgreSQL slot:

```bash
curl --fail http://localhost:8083/connectors/ecommerce-postgres-cdc-avro/status

docker compose exec -T postgres psql \
  -U "$ECOMMERCE_POSTGRES_USER" -d "$ECOMMERCE_POSTGRES_DB" \
  -c "select slot_name, active, restart_lsn, confirmed_flush_lsn, pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) as retained_bytes from pg_replication_slots;"

docker compose --profile '*' run --rm cdc-smoke-test
docker compose --profile '*' run --rm schema-contract-test
docker compose --profile '*' run --rm cdc-materialization-test
docker compose --profile '*' run --rm cdc-reconcile
```

Danh sách connector bình thường chỉ có
`ecommerce-postgres-cdc-avro`; slot tương ứng là
`ecommerce_cdc_avro_slot`. Connector/slot JSON cũ đã retire sau cutover. Không
xóa slot đang active chỉ để dọn alert: phải chứng minh connector đã
ngừng, luồng thay thế reconcile bằng 0 và retention evidence còn đủ.

Smoke test phải thấy `operations=c,u,d; delete_tombstone=true`. Nó dùng customer
UUID riêng và delete source row sau cùng.

Materialization test phải thấy `Bronze=c,u,d,t; Silver delete applied`;
reconcile phải có `differences=0, duplicate_offsets=0`.

Kiểm tra grain và row count của toàn bộ bảng Iceberg qua allow-listed gateway:

```bash
docker compose exec -T spark-gateway python3 -c \
  "import urllib.request; print(urllib.request.urlopen(urllib.request.Request('http://127.0.0.1:8090/jobs/validate-platform', method='POST'), timeout=3600).read().decode())"
```

Kết quả phải có `return_code: 0`; `row_count` phải bằng
`distinct_key_count` cho `fact_sales`, `order_events` và `current_orders`.

## 4. Failure recovery

| Triệu chứng | Kiểm tra | Phục hồi an toàn |
| --- | --- | --- |
| DAG lỗi trước watermark | task log, `candidate_value` | sửa nguyên nhân rồi retry DAG; sinks idempotent |
| dbt test fail | tên test và failure rows | sửa model/data, chạy `dbt build` rồi retry |
| Spark gateway trả non-2xx | gateway + master/worker logs | bảo đảm MinIO/Spark healthy rồi clear task |
| Producer fail giữa batch | Kafka/producer logs | retry; event ID ổn định, sink merge dedupe |
| Stream restart loop | `order-stream` logs, DLQ | sửa schema/config, giữ checkpoint, restart service |
| Consumer lag tăng | consumer group describe | tăng resource/partition hoặc giảm producer rate |
| Debezium task `FAILED` | Connect status `tasks[].trace` | sửa privilege/config rồi chạy lại `cdc-bootstrap` |
| CDC slot inactive/WAL tăng | `pg_replication_slots`, exporter metrics | khôi phục Connect trước safety cap; không drop slot để né alert |
| Customer Silver lag/difference | materializer log, group lag, `cdc-reconcile` | giữ offset/Bronze, sửa consumer rồi replay; không bật lại dual writer tùy tiện |
| Registry/Avro lỗi | Registry health/subjects, connector task trace | rollback schema/DDL hoặc version topic; không tắt compatibility để cho qua |
| Candidate treo | Prometheus alert + Airflow run | xử lý downstream; không advance watermark bằng tay |
| MinIO đầy | bucket usage, Iceberg snapshots | chạy maintenance; tăng disk trước khi ingest lại |
| Grafana trống | Prometheus targets | sửa exporter/scrape trước, không sửa dashboard vội |

Lệnh log thường dùng:

```bash
docker compose logs --tail=200 airflow-worker
docker compose logs --tail=200 spark-gateway spark-master spark-worker
docker compose logs --tail=200 order-stream kafka
docker compose logs --tail=200 schema-registry debezium-connect cdc-bootstrap customer-cdc-materializer
docker compose logs --tail=200 platform-exporter prometheus
```

## 5. Retry boundary và điều không nên làm

- Retry DAG hoặc task downstream là an toàn; không tự sửa watermark để “cho qua”.
- Không xóa Kafka checkpoint khi còn backlog; làm vậy có thể replay toàn topic.
- Không `DROP` Iceberg metadata thủ công trong MinIO; catalog và object metadata
  phải nhất quán.
- Không dùng `docker compose down -v` trong vận hành thường: `-v` xóa toàn bộ
  PostgreSQL, Kafka, MinIO, checkpoint, Prometheus và Grafana volumes.
- `dbt --full-refresh` rebuild model incremental; chỉ dùng có chủ đích và đo thời
  gian/dung lượng trước ở production.

## 6. Bảo trì và shutdown

Airflow chạy `lakehouse_maintenance` hằng tuần. Chạy tay khi cần:

```bash
docker compose --profile '*' run --rm --no-deps iceberg-maintenance
```

Profile wildcard giúp Compose resolve toàn bộ dependency chéo profile;
`--no-deps` giữ nguyên các service đang chạy và chỉ tạo container của job.

Shutdown giữ dữ liệu:

```bash
docker compose \
  --profile spark \
  --profile lakehouse \
  --profile streaming \
  --profile monitoring \
  down
```

Reset toàn bộ lab là destructive và chỉ thực hiện sau khi đã backup:

```bash
docker compose \
  --profile spark \
  --profile lakehouse \
  --profile streaming \
  --profile monitoring \
  down -v
```

## 7. Backup tối thiểu trước thay đổi lớn

Local lab chưa tự động backup. Trước migration hoặc reset:

1. `pg_dump` cả ecommerce database và Airflow metadata database.
2. Sao chép MinIO `warehouse` bucket bằng `mc mirror` sang vị trí ngoài Compose
   volume.
3. Ghi lại Kafka offsets/retention; Kafka volume không thay cho archival backup.
4. Với CDC, ghi lại connector config, slot/publication và phối hợp PostgreSQL
   restore point với Kafka Connect offset topics.
5. Kiểm thử restore vào volume/database khác. Backup chưa restore thử chưa được
   coi là backup đáng tin cậy.

## 8. Monitoring triage

Triage theo thứ tự:

1. `platform_component_up`: dependency nào thực sự không reachable?
2. `pipeline_candidate_pending`: có batch đang chờ commit không?
3. `pipeline_watermark_lag_seconds`: dữ liệu đã cũ bao lâu?
4. Airflow task logs và Spark/Kafka logs: lỗi kỹ thuật cụ thể.
5. dbt tests/DLQ: hệ thống sống nhưng dữ liệu có đúng không?

Không dùng riêng “container running” làm bằng chứng pipeline khỏe; freshness và
data-quality mới phản ánh kết quả người dùng nhận được.
