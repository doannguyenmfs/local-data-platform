# Project có thể làm gì? — Functional capabilities và hướng nâng cao

> Điều hướng: [Trung tâm tài liệu](README.md) · [Trạng thái P0](current-state.md) ·
> [Backlog authoritative](backlog.md)

Tài liệu này mô tả platform từ góc nhìn chức năng, không bắt đầu bằng tên công
nghệ. Mục tiêu là trả lời:

- người phát triển dữ liệu có thể yêu cầu hệ thống làm gì;
- dữ liệu đi vào và kết quả đi ra ở đâu;
- cơ chế nào bảo đảm retry/tính đúng;
- giới hạn hiện tại là gì;
- nên nâng cấp phần nào tiếp theo và theo thứ tự nào.

**Phạm vi hiện tại:** các mục 1–14 là capability của baseline P0. Phần
“Advanced roadmap” từ A6 trở đi là kế hoạch, không phải code đang chạy. Nguồn
sự thật ngắn gọn cho trạng thái hiện tại là [`current-state.md`](current-state.md);
backlog có Definition of Done chi tiết tại [`backlog.md`](backlog.md).

Ký hiệu trạng thái:

- **Hoàn thành**: đã có code, wiring và bằng chứng chạy.
- **Local-ready**: hoạt động đầy đủ trong lab nhưng thiếu HA/security/quy trình
  tổ chức để dùng production.
- **Một phần**: đã có đầu vào hoặc raw state, nhưng chưa materialize end-to-end.
- **Chưa triển khai**: nằm trong advanced roadmap ở cuối tài liệu.

## 1. Tóm tắt capability

| # | Platform có thể làm gì? | Trạng thái | Kết quả chính |
| ---: | --- | --- | --- |
| 1 | Dựng lại toàn bộ môi trường bằng code | Local-ready | 35 Compose services, healthcheck, volume và profile |
| 2 | Nạp tăng dần 4 bảng batch + customer CDC | Hoàn thành | watermark UPSERT và WAL/offset materialization |
| 3 | Chạy lại an toàn khi task/DAG lỗi | Hoàn thành | idempotent staging/mart/Iceberg/Kafka sinks |
| 4 | Backfill một khoảng thời gian tùy chọn | Hoàn thành | cửa sổ `[start, end)`, không đổi scheduled watermark |
| 5 | Chặn batch sai trước khi publish | Hoàn thành | count, relationship, grain và business assertions |
| 6 | Tạo warehouse mart phục vụ phân tích | Hoàn thành | dimensions, SCD2 customer, fact sales, daily sales |
| 7 | Chỉ transform phần dữ liệu thay đổi | Hoàn thành | dbt incremental MERGE và affected-date recompute |
| 8 | Ghi fact vào lakehouse ACID | Hoàn thành | Iceberg MERGE, snapshot, time travel, maintenance |
| 9 | Publish và xử lý event gần realtime | Local-ready | Kafka history/current state/DLQ và checkpoint |
| 10 | Capture và apply customer CDC kể cả delete | Hoàn thành | Avro `r/c/u/d/t`, Bronze, Silver, reconcile, SCD2 invalidate |
| 11 | Điều phối nhiều sink với commit barrier | Hoàn thành | Airflow chỉ advance watermark sau mọi nhánh bắt buộc |
| 12 | Theo dõi health, freshness và stuck state | Local-ready | exporter, Prometheus, 8 alerts, Grafana, Alertmanager |
| 13 | Kiểm tra thay đổi trước khi merge Git | Hoàn thành | dbt CI, Python/Compose/config tests và image builds |
| 14 | Phục hồi lỗi thường gặp theo runbook | Local-ready | retry-forward, checkpoint/slot/volume-aware recovery |

## 2. Dựng môi trường tái lập được

### Chức năng

Một developer có thể clone repository, cung cấp `.env` rồi dựng PostgreSQL,
Airflow, dbt runtime, Spark, Kafka, Debezium, MinIO và monitoring bằng Docker
Compose. Schema, topic, bucket, dashboard và connector được tạo bằng code thay
vì click tay.

### Cơ chế bảo đảm

- image ghim version;
- init job có thể chạy lại;
- named volume giữ runtime state khi recreate container;
- healthcheck và `depends_on` bảo vệ startup order;
- profile tách core, Spark, lakehouse, streaming, monitoring và tools.

### Giới hạn

Tái lập container không đồng nghĩa backup dữ liệu. Named volume chỉ bảo vệ khi
container bị thay, không bảo vệ khi host/disk hỏng hoặc `down -v`.

## 3. Nạp incremental từ OLTP vào staging

### Chức năng

Daily DAG nạp `products`, `orders`, `order_items` và `payments` song song. Mỗi
bảng chỉ đọc phần có `updated_at` trong source window mới. Customer do consumer
CDC liên tục sở hữu và không tham gia watermark commit barrier.

```text
committed watermark < source.updated_at <= captured upper bound
                         ↓
                 staging UPSERT by primary key
```

### Cơ chế bảo đảm

- upper bound được chụp trước query để tạo cửa sổ đóng;
- UPSERT cập nhật row cũ thay vì nhân đôi;
- staging write và candidate watermark nằm cùng database transaction;
- committed watermark chỉ đổi ở task cuối.

### Kết quả người dùng nhận được

Staging là current state gần nhất mà batch đã nạp thành công. Chạy lại cùng cửa
sổ hội tụ về cùng row set.

### Giới hạn

Polling chỉ thấy row còn tồn tại và có `updated_at` đúng contract. Nó không tự
nhìn thấy hard delete và có thể bỏ late record mang timestamp cũ.

## 4. Retry và idempotency xuyên nhiều sink

### Chức năng

Nếu dbt, Spark hoặc Kafka publication lỗi sau khi nhánh khác đã commit, operator
có thể retry thay vì xóa dữ liệu hoặc rollback thủ công.

### Cơ chế bảo đảm

| Sink | Stable identity | Cách hội tụ khi retry |
| --- | --- | --- |
| staging | source primary key | PostgreSQL UPSERT |
| dbt fact | `order_item_id` | incremental MERGE |
| daily mart | `sales_date` | tính lại affected date rồi MERGE |
| Iceberg fact | `order_item_id` | Iceberg MERGE |
| Kafka event | UUID5 từ order version | stable event identity |
| Stream history | `event_id` | Iceberg MERGE/dedupe |
| Stream current state | `order_id` + source version | latest-version MERGE |
| DLQ | Kafka topic/partition/offset hash | deterministic MERGE |

Đây là retry-forward với at-least-once delivery, không phải distributed
transaction giữa PostgreSQL, Kafka và object storage.

## 5. Backfill dữ liệu lịch sử

### Chức năng

Operator có thể truyền `backfill_start` và `backfill_end` để xử lý lại một
khoảng thời gian chính xác.

### Cơ chế bảo đảm

- interval half-open `[start, end)` giúp hai cửa sổ liên tiếp không chồng biên;
- backfill không advance scheduled watermark;
- sink MERGE/UPSERT làm replay an toàn;
- timestamp bắt buộc có timezone.

### Trường hợp sử dụng

- sửa transformation rồi chạy lại dữ liệu cũ;
- bổ sung record đến muộn đã được xác định;
- phục hồi một cửa sổ từng lỗi;
- đối soát source và target theo ngày.

## 6. Kiểm tra chất lượng dữ liệu

### Chức năng

Pipeline có thể dừng trước khi publish một batch không đạt contract.

Các lớp kiểm tra gồm:

- source/staging row count guardrail;
- foreign-key relationship;
- `not_null`, `unique`, `accepted_values`;
- non-negative measure;
- fact grain;
- daily-to-fact reconciliation;
- SCD2 validity range và đúng một current version;
- Spark persisted-table grain validation;
- invalid streaming payload được chuyển sang DLQ.

### Ý nghĩa

Container `running` chỉ nói process còn sống. Data test mới kiểm tra output có
đúng grain và business invariant không.

## 7. Xây warehouse phục vụ phân tích

### Chức năng

dbt biến staging thành các contract có lineage:

```text
source() → stg_* → int_* → dim_*/fact_sales → daily_sales
```

- `dim_product`: current product attributes;
- `dim_customer`: customer history kiểu SCD2;
- `fact_sales`: một row trên mỗi order item;
- `daily_sales`: KPI theo sales date.

### Điểm đáng chú ý

- customer surrogate key được resolve theo thời điểm order xảy ra;
- order cũ hơn snapshot đầu có fallback được đánh dấu rõ;
- payment/order update lịch sử vẫn được chọn bằng ingestion time;
- order chuyển ngày làm cả ngày cũ và mới được tính lại;
- test và docs chạy cùng dependency graph bằng `dbt build`.

## 8. Xử lý incremental trong dbt

### Chức năng

Daily run không rebuild toàn bộ fact và aggregate.

- `fact_sales` chỉ chọn intermediate row có `source_loaded_at` mới hơn target;
- dbt sinh MERGE theo `order_item_id`;
- `daily_sales` chỉ tìm ngày bị ảnh hưởng nhưng tính lại trọn ngày từ canonical
  fact state;
- no-change replay không làm tăng row count.

### Giới hạn

dbt incremental không tự biết source delete. Nó chỉ transform tín hiệu upstream
đã cung cấp.

## 9. Lakehouse ACID và lịch sử commit

### Chức năng

Spark có thể ghi fact vào Iceberg thay vì chỉ tạo file Parquet rời:

- append lần đầu;
- incremental MERGE các lần sau;
- no-op nếu không có delta;
- đọc snapshot cũ bằng time travel;
- compact small/delete files;
- expire snapshot theo retention policy.

Catalog nằm trong PostgreSQL; data/metadata files nằm ở MinIO. Backup phải phối
hợp cả hai phía.

## 10. Event streaming và current state

### Chức năng

Batch order mới có thể được publish vào Kafka và Spark Structured Streaming
materialize thành ba bảng Iceberg:

1. `order_events`: logical event history;
2. `current_orders`: version mới nhất mỗi order;
3. `dead_letter_events`: payload lỗi hoặc schema version không hỗ trợ.

### Cơ chế bảo đảm

- event key giữ ordering cho cùng order trong một partition;
- deterministic `event_id` chống duplicate khi producer retry;
- Kafka giữ durable backlog;
- Spark checkpoint giữ source progress;
- sink MERGE đóng crash window giữa Iceberg commit và checkpoint commit.

### Giới hạn

Order-event path vẫn dùng JSON code validation. Customer CDC path dùng Avro,
Apicurio Registry và `BACKWARD_TRANSITIVE` compatibility; hai event domain có
contract maturity khác nhau và được ghi rõ thay vì gọi chung “Kafka”.

## 11. Database Change Data Capture

### Chức năng đã có

Debezium đọc PostgreSQL WAL và lưu Avro change của `public.customers` vào topic
`ecommerce_cdc_avro.public.customers`:

- initial state: `op=r`;
- insert: `op=c`;
- update: `op=u`;
- delete: `op=d` rồi tombstone.

Replication slot giữ WAL chưa xử lý; Kafka Connect offset giữ LSN đã đọc. Role
CDC riêng không phải superuser, publication chỉ allow-list `customers`.

Consumer ghi immutable Bronze theo Kafka coordinate và fold Silver theo
LSN/offset. `r/c/u` upsert, `d` đặt tombstone, Kafka tombstone `t` chỉ vào Bronze.
dbt đọc Silver current; hard delete đóng version SCD2 hiện hành nhưng giữ lịch
sử. Airflow customer poller đã retire, watermark cũ giữ `active=false` để audit.

## 12. Orchestration và commit barrier

### Chức năng

Airflow điều phối:

```text
validate config
  → 4 extract task song song (customer đi CDC liên tục)
  → count/relationship validation
  → [dbt, Iceberg batch, Kafka publish] song song
  → advance all watermarks
```

Watermark cuối là business commit marker của batch. Một nhánh fail thì candidate
vẫn còn để quan sát/retry; không công bố rằng toàn pipeline đã hoàn thành.

Weekly maintenance tách khỏi ingest DAG để housekeeping lỗi không rollback một
batch dữ liệu hợp lệ.

## 13. Monitoring và alerting

### Chức năng đã có

Exporter đọc state thật rồi Prometheus lưu/evaluate:

- semantic component health;
- committed-watermark freshness;
- candidate batch bị treo;
- staging volume guardrail;
- Kafka required topic/partition;
- Debezium connector/task state;
- CDC slot active và WAL retained bytes.

Prometheus có 8 alert rules. Grafana dashboard được provision từ Git.
Alertmanager group/deduplicate alert trong local UI.

### Giới hạn

Chưa có Slack/email/PagerDuty receiver, on-call escalation, centralized logs,
distributed tracing hoặc SLO/error-budget automation.

## 14. CI và khả năng vận hành

### Chức năng

GitHub Actions kiểm tra:

- slim PR graph bằng state/defer;
- full main initial build, no-op replay và mutation behavior;
- enforced name/type contract cho public marts;
- Python compile;
- Docker Compose render;
- Prometheus/Alertmanager syntax;
- Spark unit tests trong runtime image;
- container image build.

Runbook mô tả startup, backfill, validation, failure recovery, maintenance,
shutdown và backup tối thiểu. Đây là nền tảng operability, chưa phải CD.

## 15. Những việc project chưa làm được

Để tránh đọc capability theo hướng quá lạc quan, project hiện chưa:

- bảo đảm HA khi một node/host mất;
- mã hóa/authenticate traffic nội bộ;
- gửi alert tới người trực;
- backup off-host và tự động restore test;
- enforce schema registry cho order-event JSON domain (customer CDC đã có);
- cung cấp semantic/API serving layer cho BI/application;
- tự động scale hoặc chứng minh capacity bằng load/soak test;
- triển khai environment thật bằng IaC/Kubernetes.

## 16. Advanced roadmap

Roadmap dưới đây là bản tóm tắt để giữ ngữ cảnh với capability P0. Từ A6 trở đi
đều **chưa triển khai**; [`backlog.md`](backlog.md) mới là nơi quản lý chi tiết,
thứ tự, thiết kế dự kiến và Definition of Done. Ưu tiên tính đúng và
recoverability trước việc tăng số lượng công nghệ.

| ID | Ưu tiên | Nâng cấp | Capability được mở khóa |
| --- | --- | --- | --- |
| A1 | P0 ✅ | Schema Registry/compatibility | event contract có thể enforce và evolve |
| A2 | P0 ✅ | CDC Bronze | raw database change audit/replay bằng SQL |
| A3 | P0 ✅ | CDC Silver | current state thực sự apply insert/update/delete |
| A4 | P0 ✅ | CDC cutover | chuyển ownership an toàn, retire batch customer poller |
| A5 | P0 ✅ | dbt contracts/slim CI | chặn breaking mart và rút ngắn PR feedback |
| A6 | P1 | External alerting/SLO | alert tới đúng owner và đo reliability |
| A7 | P1 | Backup/PITR/DR | phục hồi có RPO/RTO được chứng minh |
| A8 | P1 | Security hardening | encryption, authentication và least privilege |
| A9 | P1 | High availability | chịu lỗi node/failure domain |
| A10 | P1 | OpenLineage/catalog | biết dataset đến từ đâu, ai sở hữu và ảnh hưởng ai |
| A11 | P1 | Centralized logs/traces | điều tra xuyên component bằng correlation |
| A18 | P1 | Batch late row/hard delete | polling correctness có reconcile/proof |
| A12 | P2 | Event-time/late streaming | xử lý window/out-of-order có state |
| A13 | P2 | Data observability | phát hiện drift/anomaly ngoài rule tĩnh |
| A14 | P2 | Capacity/cost engineering | sizing bằng workload và benchmark thật |
| A15 | P2 | Semantic/serving layer | metric/API contract cho consumer |
| A16 | P2 | Governance/privacy | PII, retention, deletion và access audit |
| A19 | P2 | CDC + Registry theo domain | mở rộng capture/contract có capacity review |
| A20 | P2 | Concurrency fencing/outbox | nhiều run/writer vẫn giữ đúng ordering |
| A17 | P3 | IaC/Kubernetes/cloud | promotion/deployment tái lập trên hạ tầng thật |

### P0 — Hoàn thành data contract và CDC end-to-end

P0 đã hoàn thành ngày 2026-09-17. Các đoạn dưới giữ nguyên “vì sao/cơ chế/Done”
như decision record để người học hiểu thiết kế, không còn là backlog mở.

#### A1. Schema Registry và compatibility gate — Hoàn thành

**Vì sao cần:** JSON schema đang lặp trong message và producer/consumer có thể
thay đổi không tương thích mà broker không ngăn được.

**Đã triển khai:** Apicurio Registry KafkaSQL, Debezium Avro converter, versioned
subject/topic, `BACKWARD_TRANSITIVE`, FULL validity và read-only breaking test.

**Done khi:** incompatible schema bị CI/registry từ chối; consumer đọc được cả
version cũ và mới; runbook có quy trình rollout/rollback.

#### A2. CDC Bronze raw table — Hoàn thành

**Vì sao cần:** Kafka retention không nên là kho lưu audit duy nhất; cần replay
theo LSN/topic coordinates và query/debug bằng SQL.

**Đã triển khai:** Python consumer append Debezium envelope vào PostgreSQL
Bronze, giữ key, `op`, LSN, transaction, Kafka coordinates và JSONB payload.
PostgreSQL phù hợp volume lab/query học; production lớn có thể chuyển contract
này sang Iceberg mà không đổi identity semantics.

**Done khi:** replay cùng offset không nhân đôi Bronze identity và source event
được trace từ PostgreSQL LSN tới PostgreSQL Bronze row. Iceberg chỉ là
hướng scale-out sau này, không phải storage đang được P0 sử dụng.

#### A3. CDC Silver current state và delete apply — Hoàn thành

**Vì sao cần:** đây là phần còn thiếu để delete thực sự ảnh hưởng downstream.

**Đã triển khai:** dedupe theo source coordinates; áp `r/c/u` thành upsert và `d`
thành delete/closed-state; xử lý out-of-order/replay bằng LSN/version rule.

**Done khi:** source insert/update/delete tạo đúng Silver state sau restart và
replay; có reconciliation với source current keys.

#### A4. Parallel run, reconcile và cutover customer ownership — Hoàn thành

**Vì sao cần:** không thể để Airflow batch và CDC âm thầm cùng ghi staging.

**Đã triển khai:** chạy song song raw/Silver, so count/key/attribute/delete, chuyển dbt
source sang Silver, đóng băng batch customer writer rồi retire watermark riêng.

**Done khi:** không có dual-writer, rollback path rõ và `dim_customer` đóng đúng
current version sau source delete.

#### A5. dbt model contracts và slim CI — Hoàn thành

**Vì sao cần:** tests kiểm tra data sau chạy nhưng chưa enforce đầy đủ column
name/type contract trước consumer; CI hiện chưa chọn state-aware modified graph.

**Đã triển khai:** enforced name/type contracts cho public marts;
`state:modified+` và defer tới base-SHA manifest/relation trong PR; full
idempotency/mutation build trên main. Artifact promotion/version-deprecation ở
quy mô nhiều team vẫn là bước tổ chức ngoài local lab.

**Done khi:** breaking mart change fail trước deploy và PR chỉ build graph bị
ảnh hưởng nhưng vẫn an toàn.

### P1 — Reliability, security và operability

#### A6. External alerting, SLO và on-call

- Slack/email/PagerDuty receiver lấy secret từ secret manager.
- Alert có owner, runbook URL, severity và escalation.
- Định nghĩa availability/freshness/correctness SLI, SLO và error budget.
- Kiểm thử firing/resolved notification định kỳ.

#### A7. Backup, PITR và disaster-recovery drill

- PostgreSQL base backup + WAL archive/PITR.
- Versioning/replication cho object storage.
- Kafka/Connect config-offset recovery strategy.
- Backup secrets/config/catalog phối hợp.
- Tự động restore sang environment cô lập và đo RPO/RTO.

#### A8. Security hardening

- TLS cho database, Kafka, Connect, object store và UI/API.
- Kafka SASL/ACL, PostgreSQL least privilege theo service.
- secret manager + rotation, không để credential trong `.env` production.
- private network, ingress allow-list, audit log và image vulnerability scan.

#### A9. High availability và failure-domain separation

- PostgreSQL primary/replica hoặc managed HA.
- Kafka >=3 broker/controller, replication factor phù hợp.
- nhiều Kafka Connect workers; Airflow scheduler/workers HA.
- distributed object store và external monitoring.
- chaos/failover test chứng minh recovery, không chỉ tăng node count.

#### A10. OpenLineage, catalog và ownership

- emit lineage từ Airflow/dbt/Spark;
- catalog dataset, schema, owner, SLA, freshness và quality result;
- column-level lineage cho mart critical;
- liên kết alert → dataset → owner → runbook.

#### A11. Centralized logging và tracing

- log aggregation có correlation theo DAG run, Spark application, Kafka
  partition/offset và connector task;
- retention/search/redaction policy;
- OpenTelemetry trace qua gateway/job boundaries khi có giá trị điều tra.

#### A18. Batch late-arrival và hard-delete correctness

- ingestion timestamp/lookback/reconciliation hoặc CDC theo source contract;
- late/delete fixture và source-target control total;
- index/query-plan proof để polling vẫn nằm trong SLA;
- giữ explicit backfill như recovery tool, không coi nó là detector tự động.

### P2 — Streaming nâng cao, scale và data product

#### A12. Event-time, late data và stateful streaming

- event-time watermark;
- window aggregation;
- allowed lateness và late-event side output;
- state-store sizing/checkpoint migration;
- deterministic test cho out-of-order data.

#### A13. Data observability nâng cao

- source-to-target control totals;
- distribution/null/cardinality drift;
- volume/freshness anomaly detection theo seasonality;
- data incident history và impact analysis;
- tránh metric label chứa business ID.

#### A14. Performance, capacity và cost engineering

- benchmark data volume/throughput/latency chuẩn;
- query plan, Spark shuffle/skew và Iceberg file-size metrics;
- Kafka partition/consumer lag capacity model;
- soak test, backpressure test và failure injection;
- cost per pipeline/dataset ở cloud deployment.

#### A15. Semantic/serving layer

- metric definitions dùng chung cho BI;
- row/column access policy;
- caching/query acceleration khi có workload thật;
- API hoặc reverse ETL chỉ sau khi mart contract ổn định.

#### A16. Governance và privacy

- classify PII, masking/tokenization;
- retention và right-to-delete xuyên raw/Bronze/Silver/mart/backup;
- access approval/audit;
- data owner/steward và change-management policy.

#### A19. Mở rộng CDC và Registry theo domain

- review SLA/capacity/ownership trước khi thêm table vào publication;
- versioned schema, compatibility, reconcile và single-writer cutover;
- cân nhắc Registry/outbox cho order-event nếu trở thành public contract.

#### A20. Concurrency fencing và transactional outbox

- batch/run identity và compare-and-set/lease khi promote progress;
- concurrent/reordered/crash test cho nhiều scheduler/writer;
- dùng outbox hoặc source CDC khi cần atomicity với application mutation.

### P3 — Hạ tầng production và environment promotion

#### A17. IaC, environment promotion và Kubernetes/cloud

- Terraform cho network, database, object store, Kafka và IAM;
- dev/ci/prod tách account/project và secret;
- immutable image/artifact promotion;
- Spark Operator/managed jobs, Airflow deployment và GitOps rollback;
- chỉ thực hiện sau khi contract, DR và SLO đã rõ.

## 17. Thứ tự triển khai được khuyến nghị

```text
Schema Registry/contracts ✅
    ↓
CDC Bronze → Silver → reconciliation → cutover ✅
    ↓
External alerting/SLO + backup/restore + security
    ↓
Lineage/catalog + centralized logs
    ↓
HA và capacity/failure tests
    ↓
Event-time/anomaly/serving/governance
    ↓
IaC/Kubernetes hoặc managed production deployment
```

Thứ tự này không tuyệt đối, nhưng tránh hai lỗi phổ biến: triển khai Kubernetes
trước khi biết data contract là gì, hoặc xây HA trước khi có backup/restore đã
được kiểm thử.

## 18. Tài liệu liên quan

- [Bản đồ nhập môn](learning/00-beginner-map.md): state/retry/delete cho người mới.
- [Trạng thái P0](current-state.md): capability thật đang tồn tại và bằng chứng.
- [Backlog](backlog.md): công việc chưa hoàn thành và Definition of Done.
- [Kiến trúc](architecture.md): topology, node, network, storage và failure domain.
- [Production trade-offs](learning/11-verification-and-tradeoffs.md): readiness matrix.
- [Runbook](runbook.md): command và recovery action.
- [Roadmap](../ROADMAP.txt): trạng thái level hiện tại.
