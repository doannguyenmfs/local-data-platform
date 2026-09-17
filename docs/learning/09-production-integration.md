# Level 9 — Production integration

## Mục tiêu

Một platform không hoàn thành chỉ vì từng tool chạy độc lập. Integration phải
xác định rõ dependency, transaction boundary, retry owner và failure recovery.

```text
Airflow daily DAG
       │
       ├── extract staging in parallel
       ├── validate count + relationships
       ├── dbt build ───────────────► PostgreSQL analytics
       ├── Spark gateway/job ───────► Iceberg on MinIO
       ├── replay-safe producer ────► Kafka
       └── all successful ──────────► commit Airflow watermarks

Kafka ──► long-running Spark stream ──► Iceberg event/current/DLQ tables
Airflow weekly DAG ───────────────────► compact + expire Iceberg snapshots
```

## Tại sao streaming không nằm trong Airflow task?

Airflow tối ưu cho công việc có điểm bắt đầu/kết thúc. Streaming query chạy liên
tục, tự quản lý checkpoint và restart. Nếu đặt nó vào task, task sẽ không bao giờ
success, chiếm worker slot và retry semantics trở nên sai.

Airflow quản lý batch control plane; Docker/service manager giữ streaming
process. Production Kubernetes thường thay Docker restart policy bằng Deployment
hoặc Spark operator.

## Spark job gateway

Spark standalone không hỗ trợ PySpark ở `cluster` deploy mode. Mount Docker
socket vào Airflow để tạo container sẽ trao quyền root-equivalent và không an
toàn. Project dùng một gateway allow-list:

- Chỉ chấp nhận `iceberg-sales` và `iceberg-maintenance`.
- Chạy driver ở client mode trong Spark image chuẩn.
- Executor vẫn được master phân bổ sang worker.
- Một lock ngăn hai job maintenance/batch tranh tài nguyên local cùng lúc.
- Trả exit code và log tail cho Airflow.

Đây là adapter phù hợp local environment. Ở cluster thật, thay bằng Apache Livy,
Spark Operator hoặc managed job API; DAG dependency không cần đổi về mặt ý
nghĩa.

## Watermark là commit boundary

`candidate_value` được tạo trong extract nhưng chỉ chuyển thành
`watermark_value` sau khi:

1. dbt build/test thành công.
2. Iceberg merge thành công.
3. Kafka broker ack toàn bộ event.

Nếu một nhánh lỗi, watermark chưa advance. Retry có thể đọc/publish lại batch:

- staging upsert theo primary key;
- dbt merge theo `order_item_id`;
- Iceberg merge theo `order_item_id`;
- Kafka producer retry + deterministic event id;
- streaming sink merge theo event/Kafka coordinate.

Không có distributed transaction xuyên PostgreSQL, MinIO và Kafka. Thay vào đó,
platform dùng at-least-once delivery kết hợp idempotent consumers/sinks.

Kafka publication thành công là đủ để batch DAG tiếp tục; không cần đợi streaming
consumer ghi Iceberg. Kafka là durable hand-off boundary, consumer có thể catch
up sau khi restart.

## Khởi động full platform

```bash
docker compose \
  --profile spark \
  --profile lakehouse \
  --profile streaming \
  up -d --build
```

Kiểm tra control plane:

```bash
docker compose ps
curl --fail http://localhost:8090/health
docker compose exec -T kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:19092 \
  --describe \
  --topic ecommerce.order-events.v1
```

Sau đó trigger `ecommerce_pipeline` trên Airflow UI. DAG thành công phải có ba
nhánh song song trước `advance_watermarks`:

- `run_dbt_transform`
- `run_iceberg_batch`
- `publish_order_events`

## Failure matrix

| Failure | Điều gì đã commit? | Hành động |
| --- | --- | --- |
| Extract lỗi | Một số staging upsert có thể xong, watermark chưa advance | Retry DAG; upsert idempotent |
| dbt test lỗi | Staging có batch, candidate còn giữ | Sửa model/data rồi retry |
| Spark job lỗi trước Iceberg commit | Không có snapshot mới | Retry |
| Spark job lỗi sau Iceberg commit | Snapshot có thể đã commit, Airflow thấy lỗi | Retry; merge idempotent |
| Producer lỗi giữa batch | Một số events đã ack | Retry; deterministic event id dedupe ở sink |
| Stream chết | Kafka giữ events, checkpoint giữ offsets đã hoàn tất | Restart `order-stream` |
| DLQ có row | Main stream tiếp tục | Sửa producer/schema, republish/reprocess có kiểm soát |
| Maintenance lỗi | Ingest snapshot không bị rollback | Retry maintenance DAG riêng |

## Backfill

Airflow backfill hiện không advance incremental watermark. dbt và Iceberg merge
vẫn idempotent; producer có thể republish nhưng event ID giữ nguyên. Với backfill
rất lớn, nên tạm dừng stream hoặc điều chỉnh `maxOffsetsPerTrigger` để tránh làm
ảnh hưởng realtime SLA.

## Security gap được chấp nhận cho local

- Kafka PLAINTEXT, MinIO development credentials, PostgreSQL password trong
  `.env` và các UI mở ở localhost.
- Single Kafka broker, single Spark worker, single PostgreSQL và single MinIO.
- Không HA, không TLS/SASL, không secret manager.

Đây là local production-shaped platform, không phải production deployment.
Production thật phải thêm HA, encryption, network policy, external secrets,
backup/restore test và resource autoscaling.

## Điều kiện hoàn thành

- Full compose stack healthy.
- Daily DAG chạy cả ba downstream branch rồi mới advance watermark.
- Airflow retry không làm tăng grain counts.
- `order-stream` tự phục hồi từ checkpoint sau restart.
- Weekly maintenance DAG độc lập với ingest DAG.
- Runbook mô tả được recovery mà không cần đọc source code.
