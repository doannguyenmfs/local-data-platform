# Level 11 — System verification và production trade-offs

## Vì sao cần một bước verification riêng?

Unit test chỉ chứng minh một hàm đúng với input nhỏ. Container `running` chỉ
chứng minh process chưa thoát. Một data platform chỉ được coi là hoàn tất khi
các ranh giới hệ thống cùng hoạt động: credentials, network, dependency order,
retry, checkpoint, catalog, object storage và business grain.

Project dùng bốn lớp bằng chứng:

1. Static: Python compile, Compose render, Prometheus/Alertmanager config.
2. Component: dbt tests và Spark transformation/parser unit tests.
3. Data invariants: row count bằng distinct business key, no-op khi replay.
4. End-to-end: một DAG thật hoàn tất và watermark chỉ commit ở task cuối.

Mỗi lớp bắt loại lỗi khác nhau. Python compile không phát hiện credential sai;
unit test không phát hiện Docker DNS; end-to-end happy path không chứng minh
replay idempotent. Vì vậy release gate phải kết hợp chứ không chọn một loại test
duy nhất.

## Test pyramid áp dụng cho data platform

| Lớp | Chi phí | Ví dụ | Lỗi bắt được |
| --- | ---: | --- | --- |
| Static | thấp | compile, Compose/promtool config | syntax, wiring invalid |
| Unit | thấp-vừa | Spark transform/parser | business logic cô lập |
| Component | vừa | dbt build, Iceberg job | engine/materialization contract |
| Invariant/replay | vừa-cao | build/publish hai lần | duplicate, stale aggregate |
| End-to-end | cao | Airflow DAG thật | network, credential, dependency, retry barrier |

CI ưu tiên fixture nhỏ và deterministic để feedback nhanh. Dataset 1,5 triệu
row dùng cho local integration/capacity baseline, không nên bắt mọi pull request
chờ full-scale test.

## Kết quả baseline đã xác minh ngày 2026-09-17

| Kiểm tra | Kết quả |
| --- | --- |
| dbt build | 89/89 PASS, chạy liên tiếp hai lần |
| dbt incremental replay | `fact_sales` và `daily_sales` đều `MERGE 0` |
| Spark unit tests | batch transform PASS, event parser PASS |
| Iceberg initial load | 1.499.586 fact rows, grain hợp lệ |
| Iceberg replay | `operation=no-op`, snapshot không đổi |
| Kafka replay | publish 10 event hai lần, `order_events` vẫn 10 key |
| Current state | 10 rows = 10 distinct `order_id` |
| DLQ | 0 record trong happy path |
| Iceberg maintenance | compact 11 data files và 10 position-delete files |
| Airflow | manual DAG success; 5 candidate watermarks trở về `NULL` |
| Monitoring | 6 semantic component checks; 8 Prometheus rules; Alertmanager/Grafana health PASS |
| CDC | connector/task RUNNING; bootstrap chạy lại an toàn; smoke test thấy `c,u,d,tombstone` |

Các số row là baseline của seed hiện tại, không phải constant để hard-code vào
alert. Invariant quan trọng là grain, retry behavior và quan hệ giữa source với
sink.

## Vì sao row count bằng distinct key là invariant hữu ích nhưng chưa đủ?

Nó chứng minh không duplicate/null key ở declared grain, nhưng không chứng minh
mọi measure đúng hoặc source row không bị thiếu. Vì vậy project còn có dbt
reconciliation daily-to-fact, relationship tests, accepted values, non-negative
measure và SCD2 range/current-key assertions.

Production nên bổ sung source-to-target control totals, distribution anomaly,
schema contract và freshness theo từng dataset critical.

## Những lỗi integration đã phát hiện và bài học

### 1. Một executable không phải một chuỗi command

`subprocess.run()` phải nhận list argument. Truyền cả command thành một string
trong phần tử đầu khiến OS tìm một file có tên chứa toàn bộ flags. Airflow hiện
dùng list rõ ràng cho dbt và producer.

### 2. Python runtime phải sở hữu dependency của task

Airflow task runner và `/opt/dbt-venv` là hai interpreter khác nhau. Kafka
producer được gọi bằng Python của venv nơi `psycopg` và `confluent-kafka` đã
được cài; không dựa vào `sys.executable` một cách ngẫu nhiên.

### 3. Container port và published host port là hai khái niệm

Producer kết nối `postgres:5432` qua Docker network; dbt chạy từ host có thể dùng
published port khác. Compose truyền cả `ECOMMERCE_POSTGRES_PORT` và
`ECOMMERCE_POSTGRES_HOST_PORT` để hai execution context không ghi đè ý nghĩa
của nhau.

### 4. First load không nên giả vờ là incremental merge

MERGE vào bảng Iceberg rỗng đúng về logic nhưng tạo row-level plan tốn memory.
Job fact dùng append cho snapshot đầu, sau đó mới MERGE; nếu không có delta thì
không tạo snapshot mới.

### 5. Streaming current state có write pattern khác event history

`current_orders` bị upsert liên tục nên dùng merge-on-read; `order_events` là
lịch sử logic và dedupe bằng event id. Đổi lại, merge-on-read tạo delete files,
vì vậy weekly maintenance compact cả data files lẫn position-delete files.

### 6. Retry phải được kiểm tra bằng failure thật

Trong lần verification, task dbt từng vào `up_for_retry` vì thiếu env var. Sau
khi sửa runtime config, Airflow tự retry thành công rồi mới chạy
`advance_watermarks`. Đây là bằng chứng dependency/watermark barrier hoạt động,
không chỉ là sơ đồ đúng trên giấy.

### 7. Docker Compose profile cũng là runtime behavior

One-shot service thuộc `tools` nhưng dependency của nó thuộc `spark`,
`lakehouse`, `streaming`. Compose validate active profile graph trước cả khi có
`--no-deps`. Lệnh tài liệu dùng `--profile '*'` để render đủ graph và
`--no-deps` để chỉ tạo job container trên stack đang chạy. Runbook command cũng
phải được test như code.

## Production-shaped không đồng nghĩa production-ready

Project thể hiện pattern đúng nhưng cố ý giữ footprint local. Để chạy dữ liệu
thật cần ít nhất multi-node HA, TLS/SASL/RBAC, secret manager, remote backup,
external alert receiver, capacity test, SLO và deployment strategy. Spark
gateway allow-list là ranh giới an toàn cho lab; production thường thay bằng
Spark Operator, Livy hoặc managed job API.

### Đánh giá theo từng năng lực production

| Năng lực | Project hiện có | Phần còn thiếu để production |
| --- | --- | --- |
| Data correctness | grain, relationship, accepted-value, reconciliation, SCD2 invariant, replay test, mart contracts | contracts cho order-event/remaining external boundaries, anomaly/control totals |
| Incremental/retry | candidate watermark, MERGE, deterministic event ID, checkpoint, no-op replay | concurrency fencing cho nhiều DAG run/writer và recovery drill định kỳ |
| CDC | WAL, Avro Registry, least-privilege role, durable slot/offset, Bronze/Silver, delete apply, reconciliation/cutover | mở rộng table scope có capacity review, retention/archive và DR drill |
| Availability | restart policy, healthcheck, durable local volume | PostgreSQL/Kafka/MinIO/Airflow/Connect HA trên failure domain khác nhau |
| Security | non-root image khi phù hợp, separate replication role, env-based config | TLS, Kafka SASL/ACL, database RBAC chi tiết, network policy, secret manager/rotation |
| Observability | exporter, Prometheus, 8 alert rules, Grafana, Alertmanager local, runbook | external receiver, on-call/escalation, centralized logs/traces, SLO/error budget |
| Backup/DR | state/volume và recovery boundary được ghi tài liệu | off-host backup, PostgreSQL PITR, Kafka/object-store strategy, automated restore test, RPO/RTO |
| Delivery | GitHub Actions kiểm tra dbt/Python/Compose/images/config | CD, immutable artifact registry/signing, IaC, migration/rollback và environment promotion |
| Scale/cost | resource limits, partitioning, incremental/no-op, maintenance | load/soak/chaos test, capacity model, autoscaling, cost dashboard và tuning bằng workload thật |
| Governance | docs, lineage qua `ref/source`, raw audit coordinates | catalog/OpenLineage, ownership, PII classification/masking, retention/deletion policy, audit access |

Kết luận chính xác là: project đã **production-shaped** vì đã có boundary,
state, retry, quality gate và failure mode thật. Nó chưa **production-ready** vì
vẫn là topology một máy, một node, credential local và chưa có quy trình tổ chức
như on-call, DR, security governance và deployment promotion.

### Ưu tiên nâng cấp, không phải danh sách mua thêm công nghệ

1. Thêm external alert receiver, owner/runbook URL và SLO cho pipeline critical.
2. Backup off-host + restore drill; định nghĩa RPO/RTO trước khi nói HA.
3. TLS/SASL/RBAC, secret manager và network segmentation.
4. Thêm lineage/catalog và centralized logs cho điều tra xuyên component.
5. Load/soak/failure test rồi mới quyết định scale node/partition/resource.

Thứ tự này ưu tiên tính đúng và khả năng phục hồi trước việc làm topology trông
“to” hơn.

## Release checklist của project

1. `git diff --check` và Python compile.
2. `docker compose --profile '*' config --quiet`.
3. dbt parse/build; chạy incremental lần hai.
4. Spark unit tests trong image thật.
5. promtool/amtool config validation.
6. Iceberg batch replay và platform grain validation.
7. Kafka event replay, current-state và DLQ check.
8. Airflow DAG end-to-end; candidate watermark phải về `NULL`.
9. Working tree chỉ chứa thay đổi có chủ đích; commit nhưng không tự push.

## Khi nào project được gọi là production-ready hơn?

Không phải khi thêm nhiều logo. Cần evidence cho availability, security,
recoverability và operability: multi-node failure test, TLS/RBAC, secret
rotation, off-host backup restore, capacity benchmark, SLO/error budget,
schema/data contract ownership và deploy/rollback procedure.

Không có một checkbox biến project thành production-ready. Mỗi workload cần SLA,
RPO/RTO, data sensitivity và lưu lượng cụ thể; readiness phải được chứng minh
bằng test/failure drill tương ứng, không chỉ bằng việc file cấu hình tồn tại.

Các lệnh verification chuẩn và failure recovery nằm trong
[`docs/runbook.md`](../runbook.md). Không xóa volume/checkpoint để làm test xanh;
hãy giữ state và chứng minh cơ chế retry/idempotency xử lý được nó.
