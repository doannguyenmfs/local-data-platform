# Backlog sau P0

Tài liệu này chỉ chứa capability **chưa hoàn thành** ở baseline P0. Mỗi mục mô
tả nó là gì, tại sao cần, thiết kế dự kiến và điều kiện để được đổi trạng thái
sang Done. Một thư mục/config thử nghiệm hoặc container chạy được chưa đủ để
đánh dấu hoàn thành.

## 1. Nguyên tắc ưu tiên

```text
P0 data correctness/contracts ✅
        ↓
P1 recoverability + security + operability
        ↓
P2 event semantics + scale + data product + governance
        ↓
P3 hạ tầng production/cloud nếu thật sự cần
```

Thứ tự này tránh hai lỗi phổ biến:

- dựng cluster lớn trước khi biết retry/delete/schema contract có đúng không;
- thêm dashboard/tool nhưng không có owner, recovery action và executable proof.

## 2. Bảng ưu tiên

| ID | Priority | Capability chưa có | Kết quả mong muốn | Phụ thuộc chính |
| --- | --- | --- | --- | --- |
| A6 | P1 | External alerting + SLO | alert tới đúng owner, reliability đo được | metrics/rules P0 |
| A7 | P1 | Backup, PITR, DR drill | phục hồi được và biết RPO/RTO | state inventory |
| A8 | P1 | Security hardening | giảm blast radius, bảo vệ credential/traffic | owner/identity map |
| A9 | P1 | HA + failure domains | chịu process/node/AZ loss có kiểm chứng | A7, A8 |
| A10 | P1 | OpenLineage + catalog | biết source, downstream, owner, impact | dbt manifest/contracts |
| A11 | P1 | Centralized logs + traces | điều tra một run xuyên component | correlation standard |
| A18 | P1 | Batch late row + hard-delete correctness | polling không bỏ record/xóa âm thầm | source contract/reconcile |
| A12 | P2 | Event-time + late streaming | window đúng với out-of-order data | streaming contract |
| A13 | P2 | Data observability | phát hiện drift/anomaly ngoài rule tĩnh | A6, A10 |
| A14 | P2 | Capacity + cost engineering | sizing bằng workload evidence | stable SLO/workload |
| A15 | P2 | Semantic/serving layer | metric contract dùng chung cho consumer | stable marts |
| A16 | P2 | Governance + privacy | PII, retention, deletion, audit | A8, A10 |
| A19 | P2 | Mở CDC + Registry theo domain | giảm polling debt có capacity proof | A7, A8, A14 |
| A20 | P2 | Concurrency fencing + outbox | nhiều run/writer vẫn có một progress order | stable batch/event identity |
| A17 | P3 | IaC/cloud/Kubernetes | deployment/promotion tái lập | A7–A16 decisions |

## 3. P1 — Reliability, security và operability

### A6. External alerting, SLO và error budget

**Là gì?**  Alertmanager local hiện chỉ group/display alert. A6 đưa notification
ra Slack/email/PagerDuty hoặc provider-neutral webhook, đồng thời định nghĩa SLI,
SLO và error budget cho availability/freshness/correctness.

**Vì sao cần?**  Alert không tới người chịu trách nhiệm thì chỉ là dashboard.
Một threshold đơn lẻ cũng không cho biết reliability trong cửa sổ dài hay release
đang tiêu error budget nhanh đến mức nào.

**Thiết kế dự kiến:**

- mỗi alert có `owner`, `severity`, `runbook_url`, dedupe/group key;
- secret provider được inject runtime, không commit vào Git;
- recording rules tính success ratio/bad minutes theo cửa sổ;
- relay/audit tách provider khỏi rule và redacts payload;
- synthetic firing/resolved test chạy định kỳ.

**Done khi:** một test tạo đúng một incident firing và resolved ở external
receiver; SLO/error budget query có fixture xác minh; runbook chỉ rõ escalation
và action. “Alert rule load thành công” chưa đủ.

### A7. Backup, PITR và disaster-recovery drill

**Là gì?**  Backup là bản sao state; PITR cho PostgreSQL quay về timestamp; DR
drill chứng minh bản sao có thể boot/query và đo RPO/RTO.

**Vì sao cần?**  Named volume chỉ survive container recreate, không chống
`down -v`, disk corruption hoặc host loss. Iceberg còn tách catalog và object
tree nên backup lệch thời điểm có thể không dùng được.

**Thiết kế dự kiến:**

- PostgreSQL base backup + WAL archive cho ecommerce và Airflow metadata;
- object-store versioning/mirror phối hợp catalog restore point;
- export connector config, topic policy và consumer offsets/strategy;
- encrypted immutable/off-host copy;
- automated isolated restore, data assertions và RPO/RTO evidence.

**Done khi:** restore vào environment cô lập pass schema/count/invariant, không
ghi đè live state; bằng chứng ghi thời điểm backup, restore target, RPO và RTO;
drill được lặp theo lịch.

### A8. Security hardening

**Là gì?**  Xác thực, mã hóa, phân quyền và quản trị secret theo least privilege.

**Vì sao cần?**  P0 có replication role riêng và non-root ở một số image, nhưng
đa số service vẫn dùng credential local, traffic plaintext và quyền database
rộng. Một compromised process có blast radius lớn.

**Thiết kế dự kiến:**

- tách owner/migration role khỏi steady-state roles cho Airflow, materializer,
  dbt và monitor;
- negative authorization tests: thao tác không thuộc nhiệm vụ phải fail;
- TLS cho PostgreSQL/HTTP/object store, Kafka SASL/ACL;
- external secret manager + rotation, không để production secret trong `.env`;
- private network/ingress allow-list, image/SBOM/vulnerability scan, audit log.

**Done khi:** allow/deny matrix có automated proof; secret rotate không downtime
ngoài budget; traffic nhạy cảm được mã hóa/xác thực; CI chặn secret và privilege
regression.

### A9. High availability và failure-domain separation

**Là gì?**  Nhiều replica có quorum/failover và nằm trên failure domain khác
nhau; không chỉ nhân container trên cùng laptop.

**Vì sao cần?**  P0 có single PostgreSQL, Kafka, Connect, Spark master/worker,
MinIO và monitoring node. Một host/disk/network failure dừng toàn platform.

**Thiết kế dự kiến:**

- managed PostgreSQL HA hoặc primary/standby với tested promotion;
- Kafka ≥3 broker/controller, RF/min ISR phù hợp;
- distributed Kafka Connect workers và nhiều Airflow scheduler/worker;
- object store/monitoring external hoặc replicated;
- idempotent task + queue visibility timeout được kiểm chứng;
- chaos/failover drills sau khi A7 recovery đã vững.

**Done khi:** kill active process/node không vượt SLO, không mất acknowledged
data và reconciliation vẫn bằng 0. Nhiều replica cùng host chỉ chứng minh process
failover, không được ghi là node/AZ HA.

### A10. OpenLineage, catalog và ownership

**Là gì?**  Metadata graph mô tả dataset nào sinh từ dataset nào, job/run nào
tạo nó, ai sở hữu, SLA/classification/runbook là gì và thay đổi ảnh hưởng ai.

**Vì sao cần?**  dbt `ref/source` cho graph trong dbt project nhưng chưa nối
Airflow, Spark, Kafka, Iceberg và operational ownership trong một view.

**Thiết kế dự kiến:**

- emit OpenLineage run/job/dataset events từ dbt manifest và orchestration;
- catalog logical/physical dataset với owner, tier, SLA, classification;
- graph/impact API, model-level trước, column-level cho mart critical sau;
- liên kết alert → dataset → owner → runbook;
- auth/RBAC/audit/retention cho metadata production.

**Done khi:** từ một source truy được transitive downstream và đúng owner;
re-run không nhân graph sai; metadata outage policy (required/best-effort) được
quyết định rõ.

### A11. Centralized logging và distributed tracing

**Là gì?**  Log aggregation cho search chung và trace parent/child nối một run
qua Airflow → gateway → Spark/consumer boundaries.

**Vì sao cần?**  P0 phải mở log từng container. Timestamp gần nhau không đủ
chứng minh các dòng thuộc cùng DAG run/Kafka batch.

**Thiết kế dự kiến:**

- structured operational events, không chứa row payload/PII;
- correlation theo Airflow run, Spark application và Kafka coordinate range;
- log agent/shipper có durable position, at-least-once push, redaction;
- bounded labels; business/run ID nằm trong body để tránh cardinality explosion;
- OpenTelemetry propagation và Collector cho batch/retry/sampling;
- Loki/Tempo hoặc backend tương đương có retention, auth và object storage.

**Done khi:** một integration test lần theo cùng correlation từ Airflow log tới
Spark child span; restart shipper không mất dòng; redaction negative test pass;
telemetry failure không retry một data commit đã thành công.

### A18. Batch late-arrival và hard-delete correctness

**Là gì?**  Bốn bảng batch hiện được chọn bằng `updated_at`. Một row mới nhưng
mang timestamp cũ hơn committed watermark, hoặc một row bị hard-delete, không
thể tự xuất hiện trong polling result. Backfill chỉ sửa được khi operator đã
biết chính xác khoảng thiếu.

**Vì sao cần?**  Đây là correctness debt chứ không chỉ là tối ưu. Pipeline có
thể xanh nhưng staging vẫn thiếu row hoặc giữ ghost row. Index `updated_at`
cũng chưa được chứng minh bằng query-plan/capacity test ở dataset lớn.

**Thiết kế dự kiến:** chọn theo contract từng source: platform-owned ingestion
timestamp; lookback window + stable-key dedupe; periodic full-key/control-total
reconciliation; soft-delete/tombstone; hoặc CDC. Thêm late/delete metrics, index
và query-plan regression khi volume yêu cầu. Không dùng một giải pháp mặc định
cho mọi bảng nếu SLA/volume/delete semantics khác nhau.

**Done khi:** fixture có late insert, old-timestamp update và hard delete đều
hội tụ source/target qua retry; không duplicate; reconcile phát hiện hoặc tự sửa
mọi difference; query vẫn nằm trong batch SLA ở baseline volume.

## 4. P2 — Streaming semantics, scale và data product

### A12. Event-time, late data và stateful streaming

**Là gì?**  Aggregate theo thời gian xảy ra sự kiện thay vì processing time,
với watermark, window, allowed lateness và late-event side output.

**Vì sao cần?**  Kafka message có thể đến trễ/out-of-order. Current P0 xử lý
version/current state nhưng chưa cung cấp window KPI có semantics late data.

**Thiết kế dự kiến:** event-time contract có timezone; deterministic window key;
watermark/allowed lateness theo SLA; state-store sizing; late path có replay và
reconciliation; checkpoint migration/versioning.

**Done khi:** fixture out-of-order/duplicate/very-late tạo kết quả window dự
kiến qua restart; state growth và late rate có metric/alert.

### A13. Data observability nâng cao

**Là gì?**  Theo dõi behavior của dữ liệu, không chỉ process: control total,
distribution, null/cardinality, volume và freshness anomaly theo seasonality.

**Vì sao cần?**  Rule tĩnh bắt invariant đã biết; nó không phát hiện “doanh thu
giảm 70% bất thường nhưng vẫn non-null và non-negative”.

**Thiết kế dự kiến:** source-to-target totals, baseline theo ngày/giờ, bounded
dimensions, incident history, lineage impact và feedback/tuning để giảm noise.

**Done khi:** synthetic shift được phát hiện với false-positive budget được đo;
incident chỉ đúng owner/impacted datasets và có query evidence.

### A14. Performance, capacity và cost engineering

**Là gì?**  Benchmark/sizing dựa trên throughput, latency, backlog, state,
shuffle, file size và chi phí thay vì đoán resource.

**Vì sao cần?**  Resource limits local chỉ chống laptop quá tải, không trả lời
bao nhiêu data/ngày hoặc bao lâu sẽ vi phạm SLA.

**Thiết kế dự kiến:** reproducible load generator; baseline volumes; query plan;
Spark skew/shuffle; Iceberg file metrics; Kafka partition/lag model; soak,
backpressure và failure injection; cost per dataset/pipeline ở cloud.

**Done khi:** capacity envelope và bottleneck được chứng minh, regression gate
so với baseline tồn tại, scaling decision gắn với SLO/cost.

### A15. Semantic và serving layer

**Là gì?**  Metric definition/serving contract ổn định cho BI, API hoặc reverse
ETL, thay vì mỗi consumer tự viết lại doanh thu/khách hàng active.

**Vì sao cần?**  dbt mart chuẩn hóa tables nhưng chưa tự tạo metric governance,
query API, caching hay row/column access policy.

**Thiết kế dự kiến:** semantic metrics có owner/version/test; access policy;
freshness contract; query acceleration/caching chỉ sau benchmark; consumer
compatibility và deprecation policy.

**Done khi:** hai consumer nhận cùng metric result qua một contract versioned;
breaking change có migration/deprecation test.

### A16. Governance và privacy

**Là gì?**  Phân loại PII, masking/tokenization, retention, legal hold,
right-to-delete và access approval/audit xuyên mọi layer và backup.

**Vì sao cần?**  CDC/Bronze giữ lịch sử rất tốt cho replay nhưng cũng làm delete
và retention khó hơn. “Silver không còn row” không đồng nghĩa dữ liệu biến mất
khỏi Kafka, Bronze, snapshot, Iceberg và backup.

**Thiết kế dự kiến:** classification trong catalog; field policy; delete lineage;
retention theo layer; cryptographic erase/tokenization khi phù hợp; audit và
owner/steward workflow.

**Done khi:** một test subject được truy vết và xử lý theo policy trên source,
Kafka, Bronze/Silver, mart/lake và backup; bằng chứng audit không chứa PII thừa.

### A19. Mở rộng CDC và schema contract theo domain

**Là gì?**  P0 chỉ CDC customer và enforce Avro contract cho customer topic.
Products/orders/order_items/payments vẫn polling; order-event demo vẫn là JSON
được consumer validate chứ chưa qua Registry compatibility gate.

**Vì sao cần?**  Mở CDC cho mọi table quá sớm làm tăng WAL retention, topic,
schema và ownership surface. Nhưng giữ polling mãi sẽ duy trì hard-delete/late
row debt và batch latency. Quyết định phải dựa trên SLA, capacity và consumer.

**Thiết kế dự kiến:** đánh giá từng domain; publication allow-list; versioned
topic/subject; contract owner và compatibility policy; Bronze/Silver hoặc
Iceberg raw retention; reconcile/cutover/rollback; WAL/topic/storage sizing.
Order-event chỉ chuyển Registry/outbox khi được coi là public contract.

**Done khi:** mỗi domain được chọn có create/update/delete/replay/schema-evolution
proof, single writer sau cutover, reconciliation bằng 0 và capacity/DR evidence.

### A20. Concurrency fencing và transactional outbox

**Là gì?**  P0 dùng `max_active_runs=1` và deterministic event ID để đơn giản
hóa ordering. P0 chưa hỗ trợ nhiều DAG run/writer cùng tranh một watermark; order
events là batch-derived publish chứ không cùng transaction với source mutation.

**Vì sao cần?**  Khi scale scheduler/writer, hai run có thể promote progress sai
thứ tự hoặc cùng phát hành một mutation theo các semantics khác nhau. Stable ID
giúp dedupe nhưng không tự tạo source transaction ordering.

**Thiết kế dự kiến:** batch/run identity, compare-and-set hoặc lease/fencing
token khi promote watermark, unique sink constraints và explicit state machine.
Nếu yêu cầu “không bỏ lỡ mutation” ở application boundary, dùng transactional
outbox hoặc source CDC thay cho dual-write database + broker.

**Done khi:** concurrent/reordered/crash fixtures không skip window, không có
hai owner hợp lệ và không nhân business event; recovery/lease expiry có test.

## 5. P3 — Hạ tầng production và environment promotion

### A17. IaC, environment promotion và cloud/Kubernetes

**Là gì?**  Provision network/database/object store/Kafka/IAM bằng code; build
immutable artifacts và promote dev → ci → prod với migration/rollback.

**Vì sao để cuối?**  Kubernetes không sửa contract, idempotency, DR hay SLO.
Đóng gói một thiết kế chưa rõ chỉ làm failure phân tán khó debug hơn.

**Thiết kế dự kiến:** Terraform/module boundaries; separate account/project;
managed service hoặc operator có lý do; artifact registry/signing/SBOM; GitOps;
Spark Operator/managed jobs; Airflow deployment; secret/identity integration.

**Done khi:** environment mới tạo từ code, artifact giống nhau được promote,
migration/rollback và DR drill pass, không dùng click-ops bắt buộc.

## 6. Quy tắc cập nhật backlog

Một mục chỉ chuyển sang Done khi đồng thời có:

1. code/config trong repository;
2. wiring runtime thực tế;
3. executable positive và failure-path test;
4. runbook recovery/rollback;
5. architecture/capability/learning docs cập nhật;
6. giới hạn local và production delta được ghi trung thực.

Nếu mới có PoC hoặc design, giữ trạng thái `In progress`/`Planned`; không đổi
thành Done vì một happy-path command đã chạy một lần.
