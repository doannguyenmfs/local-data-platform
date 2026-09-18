# Trạng thái hiện tại — P0 hoàn thành

> Điều hướng: [Trung tâm tài liệu](README.md) · [Kiến trúc](architecture.md) ·
> [Runbook](runbook.md) · [Backlog](backlog.md)

Tài liệu này là nguồn sự thật ngắn gọn về **những gì repository đang thực sự
có ở commit P0**. Nó tách “đã triển khai” khỏi “muốn triển khai” để người đọc
không nhầm một mục backlog với capability đang chạy.

- Baseline: `33d76fc` — `complete P0 CDC contracts and cutover`
- Ngày rà soát tài liệu: 2026-09-18
- Phạm vi: local production-shaped lab, không phải production deployment
- Backlog chưa làm: [`docs/backlog.md`](backlog.md)

## 1. P0 giải quyết bài toán gì?

P0 đưa project từ một batch demo thành platform có hai ingestion mode và các
contract end-to-end:

1. `products`, `orders`, `order_items`, `payments` đi bằng daily incremental
   polling với watermark/backfill;
2. `customers` đi bằng log-based CDC từ PostgreSQL WAL;
3. dbt, Spark/Iceberg và Kafka nhận state đã validate;
4. mọi retry hội tụ nhờ stable key + UPSERT/MERGE/checkpoint;
5. schema/data/operational checks có command tái chạy được.

Điểm quan trọng không phải số công nghệ. Kết quả P0 là ownership, progress state
và success boundary của từng luồng đã được định nghĩa rõ.

## 2. Kiến trúc logical hiện tại

```text
                              CONTROL PLANE
┌──────────────────────────────────────────────────────────────────────┐
│ Airflow: schedule, retry, dependency, candidate/commit barrier      │
│     ├─ 4 batch extracts                                              │
│     ├─ dbt build                                                     │
│     ├─ Spark finite job qua allow-listed gateway                     │
│     └─ deterministic Kafka producer                                  │
└──────────────────────────────────────────────────────────────────────┘
                │                                │
                ▼                                ▼
          BATCH DATA PLANE                 CDC DATA PLANE
┌──────────────────────────────┐  ┌───────────────────────────────────┐
│ PostgreSQL source            │  │ PostgreSQL WAL                   │
│  └─ updated_at window        │  │  └─ Debezium + Avro Registry    │
│      └─ staging UPSERT       │  │      └─ Kafka CDC topic          │
│          ├─ dbt marts        │  │          └─ Bronze audit         │
│          ├─ Iceberg fact     │  │              └─ Silver current   │
│          └─ order events     │  │                  └─ dbt customer │
└──────────────────────────────┘  └───────────────────────────────────┘
                │                                │
                └──────────────┬─────────────────┘
                               ▼
                     OBSERVABILITY PLANE
            exporter → Prometheus → Grafana / Alertmanager UI
```

### Vì sao tách control plane và data plane?

Airflow quyết định **khi nào** và **theo thứ tự nào** công việc chạy; nó không
phải nơi giữ business data hay chạy distributed transformation. PostgreSQL,
Kafka và Iceberg giữ durable state. Ranh giới này cho phép Airflow task retry
mà không biến scheduler thành compute/storage engine.

### Vì sao customer đi CDC nhưng bốn entity còn lại đi batch?

P0 chọn một vertical slice customer để học đủ WAL → event contract → Bronze →
Silver → delete → SCD2 trước khi mở rộng diện rộng. Mở publication cho mọi table
ngay lập tức sẽ tăng schema surface, WAL retention và ownership risk mà chưa có
capacity/DR/security tương ứng.

## 3. Physical topology hiện tại

Docker Compose định nghĩa:

- 35 service definitions;
- 21 long-running processes;
- 4 finite init jobs;
- 10 one-shot tools;
- 9 named volumes;
- một bridge network local.

Chi tiết từng container, port, node và volume nằm trong
[`docs/architecture.md`](architecture.md). Các con số trên là inventory có thể
render từ Compose, không phải tuyên bố HA: toàn bộ vẫn nằm trên một laptop.

## 4. Chức năng đã hoàn thành

### 4.1 Incremental batch + watermark commit protocol

Airflow chụp một upper bound, UPSERT source window vào staging và ghi
`candidate_value`. Chỉ task cuối mới promote candidate thành
`watermark_value` sau khi các nhánh bắt buộc thành công.

```text
committed watermark
    ↓ extract/upsert
candidate watermark
    ↓ validate → dbt + Iceberg + Kafka
committed watermark mới
```

Thiết kế hai trạng thái tránh mất dữ liệu khi staging đã commit nhưng downstream
lỗi. Nếu advance watermark ngay trong extract, retry sẽ bỏ qua window chưa được
publish đầy đủ.

### 4.2 Backfill và idempotency

Backfill nhận khoảng half-open `[start, end)`, không thay scheduled watermark.
Hai cửa sổ liền nhau không chồng record tại biên. Retry an toàn nhờ:

| Sink | Identity | Cơ chế hội tụ |
| --- | --- | --- |
| staging | source primary key | PostgreSQL UPSERT |
| dbt fact | `order_item_id` | incremental MERGE |
| daily mart | `sales_date` | recompute affected dates |
| Iceberg fact | `order_item_id` | Iceberg MERGE |
| Kafka event | deterministic UUID5 | same logical event ID |
| stream history/current/DLQ | event/business/Kafka coordinate keys | MERGE + checkpoint |

Đây là at-least-once + idempotent sinks, không phải distributed transaction.

### 4.3 Warehouse/dbt

dbt thực hiện:

```text
source() → stg_* → int_* → dim_*/fact_sales → daily_sales
```

- staging chuẩn hóa tên/type, không chứa business aggregation;
- intermediate giữ reusable joins và grain;
- marts là consumer-facing contract;
- customer snapshot tạo SCD2 và invalidate current version khi CDC hard delete;
- fact/daily models dùng incremental materialization;
- generic + singular tests bảo vệ key, relationship, accepted value, grain,
  non-negative measures và reconciliation;
- public marts enforce name/type contract;
- PR CI dùng state/defer, main CI chạy full initial/replay/mutation proof.

### 4.4 Spark và Iceberg

Project giữ cả plain Parquet demo và Iceberg implementation để thấy sự khác
nhau giữa “file output” và “transactional table format”. Iceberg path có:

- JDBC catalog trong PostgreSQL;
- object data/metadata trong MinIO;
- incremental MERGE;
- snapshot/time travel;
- file compaction và snapshot expiration;
- read-only grain/count validation.

Catalog pointer và object tree là một logical table state; backup một phía không
đủ để phục hồi table.

### 4.5 Kafka streaming

Order-event flow dùng deterministic event identity, 3 partitions, Spark
Structured Streaming checkpoint và ba Iceberg sinks:

- `order_events`: lịch sử logical event;
- `current_orders`: version mới nhất theo order;
- `dead_letter_events`: payload/schema không hợp lệ.

Nếu driver chết sau sink commit nhưng trước checkpoint, Kafka batch có thể được
replay; MERGE keys làm final state hội tụ.

### 4.6 Customer CDC end-to-end

Debezium đọc WAL bằng replication role/publication allow-list, serialize key và
envelope bằng Avro qua Apicurio Registry, rồi ghi topic versioned.

Materializer áp event theo hai contract:

- Bronze: một row trên mỗi Kafka topic/partition/offset, immutable audit;
- Silver: một row current trên mỗi `customer_id`, có tombstone state.

`r/c/u` upsert Silver, `d` đánh dấu deleted, Kafka tombstone `t` chỉ vào Bronze.
dbt đọc `customers_current`; delete làm snapshot đóng current version nhưng giữ
lịch sử. Batch customer writer đã retire nên không còn dual-writer.

### 4.7 Schema contract

Apicurio Registry giữ schema ID/version và áp `BACKWARD_TRANSITIVE` cho customer
CDC. Read-only contract test chứng minh current schema hợp lệ và breaking change
bị từ chối mà không đăng ký version rác.

Order-event demo vẫn dùng JSON + parser validation. Không được nói toàn bộ Kafka
domain đã có Registry contract; đó là backlog riêng nếu domain này thành public.

### 4.8 Monitoring hiện có

Exporter biến state thật thành bounded Prometheus metrics:

- component semantic health;
- committed watermark freshness;
- candidate pending;
- staging row count;
- Kafka required topic/partition;
- Debezium connector/task state;
- replication slot active và retained WAL bytes.

Prometheus lưu/evaluate rules, Grafana hiển thị, Alertmanager group alert trong
local UI. P0 **chưa** có external receiver/on-call, SLO/error budget, centralized
logs hay distributed tracing.

## 5. State owner và recovery owner

| State | Durable owner | Recovery mechanism |
| --- | --- | --- |
| batch progress | `metadata.etl_watermark` | retry same candidate/window |
| Airflow task/run | Airflow PostgreSQL | scheduler/Celery retry |
| CDC source position | PostgreSQL slot + Connect offset topic | connector restart/catch-up |
| Bronze apply identity | Kafka coordinates PK | replay becomes no-op |
| Silver current state | PostgreSQL Silver table | rebuild/reconcile from retained Bronze/Kafka |
| streaming progress | Spark checkpoint | restart from committed offset |
| Iceberg table | JDBC catalog + MinIO objects | snapshot/metadata-aware recovery |

## 6. Bằng chứng hoàn thành P0

P0 có executable proof, không chỉ sơ đồ:

- Compose render và runtime image build gates;
- dbt initial build, no-op replay và mutation assertions;
- Spark batch/parser unit tests trong image;
- Iceberg no-op/grain validation;
- CDC smoke test thấy `c,u,d,tombstone`;
- materialization test chứng minh Bronze/Silver create/update/delete;
- full source/Silver reconcile có `differences=0`, `duplicate_offsets=0`;
- Registry compatibility negative test;
- Airflow end-to-end commit barrier;
- Prometheus/Alertmanager config validation.

Run command và recovery action nằm trong [`docs/runbook.md`](runbook.md).

## 7. Những gì P0 chưa tuyên bố

P0 không có:

- external alert delivery, on-call escalation hoặc SLO/error budget;
- automated backup/PITR/restore drill hay off-host copy;
- TLS/SASL, secret manager, fine-grained runtime RBAC và network segmentation;
- multi-node/high-availability failure domains;
- OpenLineage/catalog/ownership service;
- centralized logs hoặc distributed traces;
- event-time window/allowed lateness;
- anomaly detection, capacity/cost benchmarks, semantic serving, privacy
  governance hoặc cloud/IaC deployment.

Các mục này được định nghĩa có thứ tự và Definition of Done tại
[`docs/backlog.md`](backlog.md). Không suy diễn chúng từ việc container đang
`running` hoặc vì project đã có một tool cùng họ công nghệ.
