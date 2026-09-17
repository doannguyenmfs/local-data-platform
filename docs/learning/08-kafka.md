# Level 8 — Kafka và event-driven streaming

## Khái niệm

Kafka là distributed append-only log. Producer ghi record vào topic; record
được định vị bằng `(topic, partition, offset)`. Consumer không “lấy message ra
khỏi queue”; nhiều consumer group có thể đọc độc lập và replay cùng log.

Project chạy Kafka 4.3.1 ở KRaft mode nên không cần ZooKeeper. Local chỉ có một
broker/controller; production cần nhiều broker, replication và security.

## Kiến trúc áp dụng

```text
PostgreSQL staging.orders
        │ replayable producer
        ▼
ecommerce.order-events.v1 (3 partitions)
        │ Spark Structured Streaming
        ├── valid history ──► Iceberg order_events
        ├── latest state  ──► Iceberg current_orders
        └── invalid       ──► Iceberg dead_letter_events
```

### Event envelope

```json
{
  "event_id": "deterministic UUID",
  "event_type": "order.upserted",
  "schema_version": 1,
  "occurred_at": "business timestamp",
  "produced_at": "producer timestamp",
  "data": {
    "order_id": "...",
    "source_updated_at": "..."
  }
}
```

Kafka key là `order_id`; vì vậy các event của cùng order vào cùng partition và
giữ thứ tự trong partition. Kafka không bảo đảm thứ tự toàn topic.

Topic có 3 partitions để minh họa parallelism/key ordering. Local chỉ có một
broker nên ba partition vẫn nằm cùng node; chúng tăng logical concurrency chứ
không tạo fault tolerance. Replication factor 1 là hệ quả của one-broker lab.

`event_id` được tạo từ `(order_id, source_updated_at)` bằng UUID5. Replay cùng
source version sinh cùng id, cho phép sink dedupe. Producer idempotence giảm
duplicate do retry trong một producer session; deterministic id và Iceberg
merge bảo vệ qua nhiều lần chạy producer.

UUID5 khác UUID4 ở tính quyết định: cùng namespace/name luôn ra cùng id. Nếu chỉ
dùng UUID4, chạy producer lần hai sẽ tạo event identity mới và sink không biết đó
là replay hay business version mới.

## Producer đọc window nào?

Scheduled run đọc `(watermark_value, candidate_value]` của `staging_orders`.
Candidate đã được extract task chụp trước, nên retry đọc đúng batch chưa commit.
Backfill dùng `[start, end)` explicit. Producer sort theo `updated_at, order_id`
để output dễ quan sát; correctness không dựa vào global publish order.

`acks=all` yêu cầu broker xác nhận theo durability policy; `enable.idempotence`
bảo vệ duplicate do network retry trong producer session. Sau `flush()`, code
so sánh delivered count và errors; enqueue vào client buffer chưa được coi là
publish thành công.

## Structured Streaming hoạt động thế nào?

Spark đọc offset range thành micro-batch. Sau khi `foreachBatch` commit xong,
checkpoint ghi progress. Nếu process chết sau Iceberg commit nhưng trước
checkpoint, batch có thể được chạy lại. Vì sink `MERGE` bằng `event_id` và
Kafka coordinates, replay không nhân đôi dữ liệu.

Đây là effectively-once ở output, không phải lời hứa “không bao giờ xử lý lại”.
Exactly-once end-to-end chỉ có nghĩa khi source, state và sink cùng thỏa điều
kiện transaction/idempotency.

Ba bảng có ba mục đích:

- `order_events`: immutable logical event history, dedupe bằng `event_id`.
- `current_orders`: projection mới nhất, merge bằng `order_id` và chỉ nhận
  `source_updated_at` mới hơn.
- `dead_letter_events`: payload không parse/không đúng schema, dedupe bằng
  `(topic, partition, offset)` đã hash.

`current_orders` dùng Iceberg v2 `merge-on-read`: micro-batch ghi update/delete
delta trước, còn weekly maintenance sẽ compact data/delete files sau. Cách này phù hợp bảng trạng
thái bị upsert thường xuyên hơn `copy-on-write`, vốn phải rewrite data file ngay
ở mỗi batch. `order_events` vẫn là lịch sử logic bất biến và `fact_sales` batch
vẫn dùng copy-on-write. Lần nạp đầu vào bảng rỗng dùng append; từ lần sau mới
MERGE để tránh dựng row-level plan không cần thiết.

## Vì sao vừa checkpoint vừa MERGE?

Checkpoint giúp Spark nhớ offset, query plan state và micro-batch progress. Nó
giảm replay bình thường nhưng không xóa crash window giữa “sink đã commit” và
“checkpoint đã ghi”. MERGE bằng stable key đóng crash window đó ở output. Chỉ
checkpoint mà append sink vẫn có thể duplicate; chỉ MERGE mà không checkpoint sẽ
đọc lại quá nhiều và mất progress/state operational.

Trong một micro-batch, `dropDuplicates(event_id)` xử lý duplicate nội batch.
Iceberg MERGE xử lý duplicate xuyên batch/restart. Hai lớp giải quyết hai phạm vi
khác nhau.

## Chạy demo one-shot

```bash
docker compose \
  --profile spark --profile lakehouse --profile streaming \
  up -d --build \
  postgres minio minio-init kafka kafka-init spark-master spark-worker

docker compose --profile '*' run --rm --no-deps order-producer
docker compose --profile '*' run --rm --no-deps order-stream-once
```

Chạy lại cả hai lệnh. Số row của `order_events` không được tăng nếu source
`updated_at` không đổi.

Chạy consumer liên tục:

```bash
docker compose \
  --profile spark --profile lakehouse --profile streaming \
  up -d order-stream
```

## Schema evolution

Order-event demo vẫn dùng JSON và `schema_version` trong envelope để
consumer route/validate. Quy tắc an toàn:

- Chỉ thêm optional field cho thay đổi backward-compatible.
- Không đổi ý nghĩa/type của field cũ tại chỗ.
- Breaking change tạo schema version mới và consumer phải hỗ trợ song song
  trong giai đoạn migration.
- JSON thuận tiện để học nhưng không tự enforce compatibility tập trung.

Customer CDC đã đi xa hơn phần demo này: Debezium dùng Avro converter,
schema được version trong Apicurio Registry và policy
`BACKWARD_TRANSITIVE` chặn thay đổi phá vỡ. Vì vậy không nên suy rộng
nhận xét “project đang dùng JSON” cho luồng CDC. Xem
`13-schema-registry-avro.md` và `14-cdc-bronze-silver-cutover.md`.

Consumer hiện chỉ chấp nhận `event_type='order.upserted'` và
`schema_version=1`. Event parse lỗi/thiếu key/version khác được giữ nguyên raw
payload và Kafka coordinates trong DLQ, thay vì silent drop hoặc làm stream dừng
mãi trên poison pill.

## Lưu ý production

- Topic local có replication factor 1; mất volume/broker là mất availability và
  có thể mất dữ liệu. Production thường RF=3 và `min.insync.replicas` phù hợp.
- Listener đang PLAINTEXT vì chỉ chạy local. Production cần TLS, SASL/ACL và
  không public broker trực tiếp.
- Retention topic là 7 ngày, độc lập với retention Iceberg.
- Merge-on-read giảm write amplification nhưng tạo delete files; maintenance
  định kỳ không phải tùy chọn khi lưu lượng tăng.
- `startingOffsets=earliest` chỉ áp dụng khi chưa có checkpoint. Sau đó checkpoint
  quyết định vị trí đọc.
- Không xóa checkpoint để “chạy lại thử” trên cùng sink khi chưa hiểu hậu quả;
  hãy dùng checkpoint path mới có chủ đích.
- Producer demo đọc đúng candidate watermark window của `staging_orders`; retry
  đọc lại cùng window và sinh cùng event id. Production nên dùng transactional
  outbox hoặc CDC thay vì dual-write database + Kafka trong application.

## Vì sao chưa dùng transactional outbox?

Source demo không có application transaction viết cả order và outbox. Airflow
đọc current-state staging rồi publish; watermark barrier + deterministic event
id làm retry an toàn nhưng vẫn là batch-derived event, không phải source event
log chính chủ. Production cần outbox/CDC khi yêu cầu không bỏ lỡ mọi mutation và
ordering theo database commit log.

## File cần đọc

- `kafka/producer/publish_orders.py`: window, envelope, UUID5, delivery ack.
- `spark/jobs/order_stream.py`: schema, validation, foreachBatch, three sinks.
- `spark/tests/test_order_stream.py`: parser contract không cần Kafka broker.
- `docker-compose.yml`: KRaft listener, 3 partitions, 7-day retention, RF=1.

## Câu hỏi phỏng vấn

**Offset có phải event id không?**  
Không. Offset chỉ duy nhất trong một topic partition và thay đổi khi republish;
event id đại diện business event xuyên qua retry/replay.

**Kafka bảo đảm ordering ở đâu?**  
Trong một partition. Chọn key quyết định các event nào cần cùng ordering domain.

**Checkpoint khác Kafka consumer commit thế nào?**  
Spark Kafka source quản lý offset trong checkpoint cùng streaming progress; nó
không dựa vào `enable.auto.commit` của Kafka consumer.

**Vì sao cần DLQ?**  
Một event lỗi không nên chặn toàn partition mãi mãi. DLQ giữ payload và tọa độ
Kafka để điều tra/replay sau khi sửa schema hoặc dữ liệu.

## Điều kiện hoàn thành

- Kafka healthy và topic có 3 partitions.
- Producer dùng `acks=all`, in đúng số event delivered.
- Streaming checkpoint được tạo.
- Replay không nhân đôi `event_id`.
- Event mới hơn cập nhật `current_orders`; event cũ không ghi đè.
- Payload hỏng xuất hiện đúng một lần trong dead-letter table.

## Tài liệu chính thức

- [Apache Kafka downloads](https://kafka.apache.org/community/downloads/)
- [Spark Structured Streaming Kafka integration](https://spark.apache.org/docs/3.5.9/structured-streaming-kafka-integration.html)
- [Iceberg Structured Streaming](https://iceberg.apache.org/docs/latest/spark-structured-streaming/)
