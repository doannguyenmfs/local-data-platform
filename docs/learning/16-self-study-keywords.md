# Từ khóa tự học Data Engineering theo project

Tài liệu này không thay thế các bài Level 1–15. Nó là danh sách từ khóa để tự
tìm tài liệu, video, bài viết hoặc câu hỏi phỏng vấn bên ngoài repository.

Khi tìm kiếm, ưu tiên tài liệu chính thức và thêm version đang dùng trong
project nếu kết quả giữa các phiên bản khác nhau, ví dụ `Airflow 3`, `Spark 3.5`,
`dbt 1.12` hoặc `Kafka 4 KRaft`.

## 1. Cách học một từ khóa

Đừng chỉ ghi nhớ định nghĩa. Với mỗi từ khóa, trả lời năm câu:

1. Nó giải quyết failure hoặc nhu cầu nào?
2. State của nó được lưu ở đâu?
3. Khi process chết và chạy lại, chuyện gì xảy ra?
4. Delivery/correctness guarantee thực tế là gì?
5. Project đang áp dụng nó ở file hoặc service nào?

Ví dụ với `Airflow retry`: state task nằm trong Airflow metadata database;
retry không tự rollback dữ liệu đã ghi vào PostgreSQL/Kafka/Iceberg; vì vậy sink
phải idempotent.

## 2. Airflow — nên học trước

### Nhóm A: khái niệm bắt buộc

| Keyword | Cần hiểu | Liên hệ project |
| --- | --- | --- |
| DAG | Đồ thị công việc có hướng, không phải nơi chứa business data | `ecommerce_pipeline` mô tả dependency của một batch |
| task / task instance | Định nghĩa task khác với một lần thực thi cụ thể | Cùng task có instance khác nhau theo DAG run |
| DAG run / logical date | Một lần chạy và khoảng dữ liệu nó đại diện | Manual run và scheduled run không đồng nghĩa wall-clock time |
| data interval | Khoảng dữ liệu mà scheduled run chịu trách nhiệm | Giúp hiểu daily schedule và backfill |
| scheduler | Tạo task instance đủ điều kiện chạy | Không trực tiếp chạy transformation nặng |
| executor | Cách Airflow phân phối task để thực thi | Project dùng CeleryExecutor |
| worker | Process nhận và chạy task | `airflow-worker`, concurrency 4 trong lab |
| metadata database | Lưu DAG run, task state, scheduling state | Tách khỏi ecommerce PostgreSQL |
| retry | Chạy lại task thất bại | Chỉ an toàn khi side effect idempotent |
| dependency / upstream / downstream | Điều kiện thứ tự giữa task | Extract fan-out, validate fan-in, commit barrier |
| trigger rule | Quy tắc quyết định task có chạy từ trạng thái upstream không | Cần khi thiết kế cleanup hoặc partial failure |
| XCom | Metadata nhỏ trao đổi giữa task | Không dùng để chuyển dataset lớn |
| connection / hook | Connection metadata và client wrapper | `PostgresHook` mở kết nối ecommerce database |
| Variable / Param | Runtime configuration có phạm vi khác nhau | Backfill dùng DAG params; secret không nên đặt tùy tiện |
| catchup | Có tự tạo các scheduled run lịch sử bị thiếu hay không | Khác với backfill cửa sổ timestamp của project |
| backfill | Chạy lại dữ liệu lịch sử theo interval | Project còn hỗ trợ `[start, end)` tùy chỉnh |
| max_active_runs | Giới hạn số DAG run chạy đồng thời | P0 đặt 1 để tránh tranh watermark |
| pool / concurrency | Giới hạn tài nguyên theo task hoặc worker | Tránh nhiều Spark/database job làm quá tải lab |
| sensor / deferrable operator / triggerer | Chờ điều kiện mà không giữ worker slot | Project có triggerer nhưng DAG hiện dùng synchronous calls |

### Nhóm B: cơ chế vận hành nên học tiếp

| Keyword | Câu hỏi trọng tâm |
| --- | --- |
| idempotent Airflow task | Chạy task lần hai có đưa sink về cùng trạng thái không? |
| atomic task boundary | Một task nên commit những state nào cùng transaction? |
| fan-out / fan-in | Vì sao extract chạy song song nhưng validation phải đợi đủ? |
| commit barrier | Vì sao watermark chỉ được promote ở task cuối? |
| zombie task / orphaned task | Scheduler phát hiện task mất heartbeat ra sao? |
| task timeout / execution timeout | Làm sao chặn task treo vô hạn? |
| SLA vs deadline alert | Airflow version hiện tại hỗ trợ và diễn giải deadline thế nào? |
| dynamic task mapping | Khi nào tạo task từ danh sách runtime thay vì viết cố định? |
| DAG serialization / DAG processor | Vì sao import DAG phải side-effect free? |
| Celery broker / result backend | Redis và PostgreSQL metadata tham gia execution ra sao? |
| remote logging | Log task được giữ ở đâu khi worker biến mất? |
| secrets backend | Lấy connection/credential từ Vault hoặc cloud secret manager ra sao? |

### Cụm tìm kiếm gợi ý cho Airflow

```text
Apache Airflow 3 DAG run logical date data interval explained
Airflow scheduler executor worker metadata database architecture
Airflow retry idempotent task best practices
Airflow catchup vs backfill
Airflow trigger rules fan in fan out
Airflow CeleryExecutor broker result backend
Airflow DAG import side effects DAG processor
Airflow deferrable operator triggerer sensor
Airflow max_active_runs concurrency pools
Airflow production deployment security remote logging
```

## 3. Kafka — nên học theo thứ tự log → ordering → recovery

### Nhóm A: data model và storage

| Keyword | Cần hiểu | Liên hệ project |
| --- | --- | --- |
| event / record / message | Key, value, headers, timestamp tạo thành record | Order event có envelope versioned và deterministic ID |
| topic | Named append-only event stream | `ecommerce.order-events.v1` và CDC topic |
| partition | Đơn vị ordering và parallelism | Order chỉ được bảo đảm bên trong một partition |
| offset | Vị trí record trong một topic-partition | Không phải business event ID |
| record key | Quyết định partition và ordering domain | Cùng business key nên đi cùng partition khi cần order |
| broker | Server lưu partition và phục vụ producer/consumer | P0 có một broker local |
| replication factor | Số bản sao partition | Lab RF=1; production thường cần nhiều broker |
| leader / follower / ISR | Cơ chế replication và acknowledged write | Liên quan `acks=all` và `min.insync.replicas` |
| retention | Thời gian/kích thước broker giữ log | Không đồng nghĩa dữ liệu đã được consumer apply |
| log compaction | Giữ latest record theo key thay vì mọi record mãi mãi | Tombstone có ý nghĩa đặc biệt trên compacted topic |
| tombstone | Record có key nhưng value null | Debezium có thể phát sau delete event |
| KRaft | Kafka tự quản metadata bằng Raft, không cần ZooKeeper | Broker P0 chạy combined broker/controller |

### Nhóm B: producer correctness

| Keyword | Câu hỏi trọng tâm |
| --- | --- |
| `acks=all` | Broker xác nhận khi điều kiện replica nào đã đạt? |
| idempotent producer | Chống duplicate trong producer session ra sao? |
| transactional producer | Atomic write nhiều partition/topic và offset có giới hạn gì? |
| delivery callback / flush | Vì sao enqueue vào client buffer chưa phải success? |
| batching / linger / compression | Đổi latency lấy throughput thế nào? |
| deterministic event ID | Vì sao retry qua process mới vẫn cần stable logical identity? |
| transactional outbox | Làm sao tránh database commit thành công nhưng Kafka publish thất bại? |

### Nhóm C: consumer và recovery

| Keyword | Cần hiểu | Liên hệ project |
| --- | --- | --- |
| consumer group | Chia partition cho các consumer cùng logical application | Mỗi partition chỉ thuộc một member trong group tại một thời điểm |
| group coordinator | Theo dõi membership và committed offsets | Restart có thể dẫn tới rebalance |
| rebalance | Phân phối lại partition khi membership thay đổi | Consumer phải xử lý revoke/commit đúng |
| offset commit | Ghi nhận progress đã xử lý | Commit trước sink có thể mất dữ liệu; commit sau sink có thể replay |
| auto offset reset | `earliest`/`latest` chỉ dùng khi chưa có valid committed offset | Không phải nút “đọc lại từ đầu” cho mọi trường hợp |
| consumer lag | Khoảng cách giữa log end và consumer progress | Đo backlog, không trực tiếp chứng minh correctness |
| at-most-once | Có thể mất, không replay duplicate | Commit progress trước processing |
| at-least-once | Không chủ động mất nhưng có thể replay | Project kết hợp với idempotent sink |
| exactly-once semantics | Guarantee có phạm vi cụ thể, không phải phép màu xuyên mọi database | PostgreSQL + Kafka + Iceberg không có một distributed transaction |
| poison pill / DLQ | Record lỗi có nên chặn cả partition? | Stream ghi `dead_letter_events` |
| replay | Đọc lại log để rebuild/recover | Sink key và schema evolution phải chịu được dữ liệu cũ |

### Nhóm D: schema và CDC trên Kafka

| Keyword | Cần hiểu | Liên hệ project |
| --- | --- | --- |
| Schema Registry | Quản lý schema ID/version ngoài payload | Customer CDC dùng Apicurio Registry |
| Avro | Binary serialization dựa trên writer/reader schema | Debezium key/envelope được serialize bằng Avro |
| subject naming strategy | Schema được group/version theo tên nào | Ảnh hưởng compatibility scope |
| backward compatibility | Reader mới đọc được dữ liệu writer cũ | P0 dùng `BACKWARD_TRANSITIVE` cho customer CDC |
| schema evolution | Add/remove/change field nào an toàn? | Breaking type change phải bị gate từ chối |
| Debezium envelope | `before`, `after`, `op`, `source`, transaction metadata | Materializer dùng `r/c/u/d` để apply state |
| WAL / LSN | Database log và vị trí thay đổi | Source of ordering/progress cho PostgreSQL CDC |
| replication slot | Giữ WAL cho consumer chưa đọc | Consumer dừng lâu có thể làm đầy disk |
| publication | Allow-list table/change được PostgreSQL phát | P0 chỉ publish customer |
| CDC snapshot | Chụp state ban đầu trước khi stream change liên tục | Khác dbt snapshot/SCD2 |

### Cụm tìm kiếm gợi ý cho Kafka

```text
Apache Kafka topic partition offset key ordering explained
Kafka KRaft broker controller architecture
Kafka acks all idempotent producer transactions difference
Kafka consumer group rebalance offset commit at least once
Kafka commit before processing vs after processing
Kafka consumer lag meaning limitations
Kafka log retention vs log compaction tombstone
Kafka exactly once semantics scope external database
transactional outbox pattern vs CDC
Debezium PostgreSQL WAL LSN replication slot publication
Debezium event envelope op c u d r tombstone
Avro schema evolution backward transitive compatibility
```

## 4. PostgreSQL và incremental ingestion

| Keyword | Vì sao cần tìm hiểu |
| --- | --- |
| primary key / unique constraint | Nền tảng để UPSERT và idempotency hội tụ |
| `INSERT ... ON CONFLICT DO UPDATE` | Cơ chế staging UPSERT của project |
| transaction / isolation level | Nhóm write và progress state thành atomic unit |
| MVCC | Giải thích snapshot visibility, update và vacuum |
| high-water mark / watermark | Cursor biểu diễn phần source đã xử lý an toàn |
| candidate watermark | Progress tạm đã extract nhưng chưa qua commit barrier |
| half-open interval `[start,end)` | Ghép backfill liên tiếp mà không trùng biên |
| late-arriving data | Row tới muộn nhưng timestamp nằm sau cursor logic như thế nào? |
| hard delete detection | Polling current table không thấy row đã biến mất |
| ingestion time vs event time vs processing time | Ba loại thời gian không được dùng lẫn nhau |
| query plan / `EXPLAIN ANALYZE` | Kiểm tra index và scan cost thay vì đoán |
| logical replication | Nền tảng PostgreSQL CDC |
| PITR / WAL archive | Recovery database về một thời điểm |

```text
PostgreSQL upsert ON CONFLICT idempotent ingestion
high water mark incremental data pipeline
late arriving data watermark lookback window deduplication
incremental load hard delete detection reconciliation
PostgreSQL MVCC transaction isolation explained
PostgreSQL logical replication slot WAL retention
PostgreSQL PITR base backup WAL archive
```

## 5. dbt và analytics engineering

| Keyword | Liên hệ project |
| --- | --- |
| `source()` / `ref()` | Tạo dependency graph và environment-aware relation |
| staging / intermediate / marts | Tách source conformance, reusable logic và public contract |
| materialization: view/table/incremental | Chọn compute/storage behavior khi model chạy lại |
| incremental model / `is_incremental()` | Chỉ xử lý delta nhưng vẫn phải sửa affected state |
| `unique_key` / merge strategy | Xác định business grain khi incremental MERGE |
| generic test / singular test | Constraint tái dùng và invariant SQL riêng |
| model contract | Enforce column name/type trước consumer |
| snapshot / SCD Type 2 | Lưu lịch sử thay đổi dimension |
| `dbt_valid_from/to`, `dbt_scd_id` | Metadata columns do dbt snapshot quản lý |
| `invalidate_hard_deletes` | Đóng version khi source current set mất key |
| state selection / `state:modified+` | Slim CI chỉ build graph bị ảnh hưởng |
| defer | Dùng relation baseline cho node chưa được build trong CI |
| manifest / run_results / catalog artifacts | Metadata đầu ra phục vụ CI/docs/lineage |

```text
dbt staging intermediate marts best practices
dbt incremental model unique_key merge is_incremental
dbt snapshots SCD type 2 timestamp strategy
dbt model contracts vs data tests
dbt state modified defer slim CI manifest
dbt source freshness lineage artifacts
```

## 6. Spark và distributed processing

| Keyword | Cần hiểu |
| --- | --- |
| driver / executor | Driver lập kế hoạch; executor chạy task trên partition |
| application / job / stage / task | Các cấp execution khác nhau trong Spark UI |
| transformation / action | Lazy plan chỉ chạy khi có action |
| narrow / wide transformation | Wide dependency tạo shuffle |
| partition | Đơn vị parallelism và file/output layout |
| shuffle / skew | Network/disk movement và hot partition |
| Catalyst / logical plan / physical plan | Spark tối ưu query như thế nào |
| broadcast join | Đổi network shuffle lấy memory khi một side nhỏ |
| Structured Streaming micro-batch | Stream được thực thi thành các batch có progress |
| checkpoint | State/progress phục vụ restart |
| `foreachBatch` | Tự kiểm soát sink transaction theo micro-batch |
| event time / watermark / window | Semantics cho out-of-order/late events |

```text
Apache Spark driver executor job stage task explained
Spark narrow wide transformation shuffle data skew
Spark partitioning coalesce repartition file size
Spark Structured Streaming checkpoint micro batch foreachBatch
Spark event time watermark allowed lateness window
Spark explain physical plan broadcast join
```

## 7. Iceberg và lakehouse

| Keyword | Cần hiểu |
| --- | --- |
| table format | Metadata/transaction layer trên object files |
| catalog | Nơi lưu pointer đến current table metadata |
| metadata JSON / manifest list / manifest | Cây metadata dẫn tới data/delete files |
| snapshot | Một committed table state |
| optimistic concurrency | Writer commit khi base metadata chưa bị thay |
| `MERGE INTO` | Upsert theo stable business key |
| copy-on-write / merge-on-read | Trade-off write cost và read/delete files |
| hidden partitioning | Query không phụ thuộc trực tiếp folder convention |
| partition evolution | Đổi layout mà không rewrite toàn bộ ngay lập tức |
| time travel | Đọc snapshot/version cũ |
| compaction | Gộp small files/delete files |
| snapshot expiration / orphan files | Retention và garbage collection phải tách bạch |

```text
Apache Iceberg architecture catalog metadata manifest snapshot
Iceberg MERGE INTO idempotent upsert
Iceberg copy on write vs merge on read
Iceberg hidden partitioning partition evolution
Iceberg time travel snapshot expiration orphan files compaction
Iceberg JDBC catalog S3 object storage recovery
```

## 8. Observability và vận hành

| Keyword | Cần hiểu |
| --- | --- |
| metric / log / trace | Ba signal trả lời câu hỏi khác nhau |
| RED / USE methods | Request rate/error/duration và utilization/saturation/errors |
| freshness / completeness / correctness | Data health khác process health |
| Prometheus scrape model | Prometheus chủ động đọc exporter endpoint |
| gauge / counter / histogram | Chọn metric type đúng semantics |
| label cardinality | Không đưa customer/order/run ID vô hạn vào label |
| alert `for` duration | Chống alert do spike thoáng qua |
| Alertmanager grouping/dedup/inhibition | Giảm notification storm |
| SLI / SLO / error budget | Đo reliability theo outcome/cửa sổ |
| RPO / RTO | Mất tối đa bao nhiêu dữ liệu và phục hồi trong bao lâu |
| backup vs restore drill | File backup tồn tại chưa chứng minh phục hồi được |
| OpenLineage / data catalog | Dataset đến từ đâu, owner và downstream impact |

```text
data pipeline observability freshness completeness correctness
Prometheus counter gauge histogram label cardinality
Prometheus alert for duration Alertmanager grouping deduplication
SLI SLO error budget data pipeline
RPO RTO backup restore disaster recovery data platform
OpenLineage data catalog ownership impact analysis
```

## 9. Docker, CI/CD và security

| Keyword | Cần hiểu |
| --- | --- |
| image / container / volume / network | Code/runtime/state/connectivity là bốn thứ khác nhau |
| healthcheck | Process sẵn sàng phục vụ hay chỉ đang chạy |
| bind mount vs named volume | Source code và durable runtime state có lifecycle khác nhau |
| multi-stage build | Giảm image size và build/runtime dependency |
| non-root container | Giảm quyền khi process bị compromise |
| CI fixture | Dataset nhỏ, deterministic để feedback nhanh |
| immutable artifact promotion | Dev/CI/prod dùng cùng artifact thay vì rebuild khác nhau |
| secret manager / rotation | Không coi `.env` là production secret management |
| least privilege / RBAC / ACL | Mỗi service chỉ có quyền đúng nhiệm vụ |
| TLS / SASL | Mã hóa và xác thực transport/database/broker |
| SBOM / vulnerability scanning | Biết dependency/image chứa gì và có CVE nào |

```text
Docker image container volume network lifecycle explained
Docker Compose depends_on healthcheck profiles
container non root least privilege read only filesystem
data pipeline CI deterministic fixtures integration tests
immutable artifact promotion dev staging production
Kafka SASL ACL TLS production security
PostgreSQL least privilege roles data pipeline
software SBOM container vulnerability scanning
```

## 10. Thứ tự tự học đề xuất

Nếu thời gian có hạn, học theo sáu chặng:

1. Airflow DAG/task/run/data interval/retry/dependency.
2. Idempotency, watermark, transaction và backfill.
3. Kafka topic/partition/key/offset/consumer group/commit/replay.
4. dbt grain/incremental/test/contract/snapshot.
5. Spark partition/shuffle/checkpoint và Iceberg snapshot/MERGE/catalog.
6. CDC WAL/LSN/slot/envelope/schema evolution, rồi observability/DR/security.

Mỗi chặng nên kết thúc bằng một failure exercise trong project, không chỉ xem
video. Ví dụ dừng worker, publish lại cùng event, chạy dbt hai lần, restart stream
hoặc tạo một customer rồi update/delete để quan sát state trước và sau recovery.

## 11. Tài liệu project nên mở cùng lúc

- [`00-beginner-map.md`](00-beginner-map.md): nối state/retry giữa các công nghệ.
- [`02-airflow-orchestration.md`](02-airflow-orchestration.md): Airflow trong project.
- [`08-kafka.md`](08-kafka.md): Kafka producer/streaming path.
- [`12-cdc-debezium.md`](12-cdc-debezium.md): WAL và Debezium.
- [`../architecture.md`](../architecture.md): container/node/state ownership.
- [`../code-map.md`](../code-map.md): từ keyword quay về đúng file code.
- [`../runbook.md`](../runbook.md): command để quan sát và thực hành failure.
- [`../backlog.md`](../backlog.md): phần nâng cao chưa được triển khai.
