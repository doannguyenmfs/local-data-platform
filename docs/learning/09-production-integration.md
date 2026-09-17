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

- Chỉ chấp nhận `iceberg-sales`, `iceberg-maintenance` và read-only
  `validate-platform`.
- Chạy driver ở client mode trong Spark image chuẩn.
- Executor vẫn được master phân bổ sang worker.
- Một lock ngăn hai job maintenance/batch tranh tài nguyên local cùng lúc.
- Trả exit code và log tail cho Airflow.

Đây là adapter phù hợp local environment. Ở cluster thật, thay bằng Apache Livy,
Spark Operator hoặc managed job API; DAG dependency không cần đổi về mặt ý
nghĩa.

Gateway nhận job name trong URL, không nhận shell command/path từ request. Đây
là khác biệt security quan trọng: Airflow có thể yêu cầu một capability đã định
nghĩa nhưng không thể biến gateway thành remote-code-execution endpoint. Một
process lock serializes finite jobs vì local Spark worker chỉ có 2 cores/3 GiB.

HTTP status thể hiện lỗi gateway/protocol; JSON `return_code` thể hiện Spark job
exit status. Airflow log cả `log_tail` để lỗi executor/driver không bị che bởi
một thông báo HTTP chung chung.

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

## Ownership của retry

| Boundary | Ai retry? | State dùng để tiếp tục |
| --- | --- | --- |
| Airflow task | Airflow | task instance + idempotent sink |
| Spark finite job | Airflow gọi lại gateway | Iceberg snapshot/high-water |
| Kafka producer request | producer library + Airflow | broker ack + deterministic ID |
| Continuous stream | Docker restart policy/Spark | Kafka log + checkpoint |
| Iceberg maintenance | maintenance DAG | Iceberg procedures trên current state |

Không để hai scheduler cùng retry vô hạn một failure mà không có ownership rõ;
điều đó tạo retry storm.

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

## Vì sao downstream chạy song song?

dbt warehouse, Iceberg batch và Kafka publication đều đọc staging đã được
validate và không phụ thuộc output của nhau. Chạy song song giảm critical path.
Đổi lại, khi một nhánh fail, hai nhánh khác có thể đã commit; vì vậy từng sink
bắt buộc idempotent và watermark barrier phải nằm sau cả ba.

Nếu cố rollback cross-system sẽ cần distributed transaction giữa PostgreSQL,
object storage và Kafka—không thực tế cho kiến trúc này. Retry-forward với stable
identity đơn giản và vận hành tốt hơn.

## Configuration boundary

Airflow task chạy trong container nên dùng `postgres:5432`, `kafka:19092`,
`spark-gateway:8090`. dbt CLI từ host dùng `localhost:<published-port>`. Project
giữ hai biến port có ý nghĩa rõ thay vì để một giá trị “hoạt động tình cờ” ở một
execution context.

Producer được chạy bằng `/opt/dbt-venv/bin/python` vì dependency Kafka/psycopg
được cài trong venv đó. Airflow task-runner Python là runtime framework và không
được giả định có mọi project dependency.

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

## File cần đọc

- `airflow/dags/ecommerce_pipeline.py`: orchestration và final barrier.
- `spark/gateway/job_gateway.py`: allow-list, subprocess và serialized submit.
- `airflow/dags/lakehouse_maintenance.py`: housekeeping control flow riêng.
- `docker-compose.yml`: dependency/health/profile/runtime wiring.
- `docs/runbook.md`: operator action cho từng failure.
