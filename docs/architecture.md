# Kiến trúc Local Data Platform — baseline P0

Tài liệu này mô tả kiến trúc **đang có trong code**, không mô tả kiến trúc P1
dự kiến. Xem bản tóm tắt capability và bằng chứng tại
[`current-state.md`](current-state.md); xem các thành phần chưa triển khai tại
[`backlog.md`](backlog.md).

## 1. Phạm vi và nguyên tắc

Đây là kiến trúc một máy, chạy bằng Docker Compose, nhưng giữ các boundary quan
trọng của production: control plane tách data plane, metadata tách business
data, compute tách storage, batch tách streaming, state bền tách runtime và
monitoring quan sát semantic health thay vì chỉ nhìn process.

Mục tiêu là học cơ chế và failure mode, không mô phỏng giả một cluster lớn trên
cùng laptop. Vì vậy mỗi distributed system chỉ có số node tối thiểu.

Quyết định thiết kế trung tâm là giữ đúng ranh giới ownership/state trước khi
tăng số node: Airflow sở hữu orchestration, PostgreSQL/Kafka/Iceberg sở hữu dữ
liệu bền, dbt/Spark sở hữu transformation và monitoring chỉ quan sát. Nhờ vậy
retry không tạo thêm một “nguồn sự thật” trong scheduler hay dashboard.

## 2. Logical architecture

```text
                         CONTROL PLANE
┌──────────────────────────────────────────────────────────────────────┐
│ Airflow API / Scheduler / DAG Processor / Worker / Triggerer        │
│              │                         │                             │
│              │ metadata               │ submit/execute              │
│              ▼                         ▼                             │
│       Airflow PostgreSQL       dbt / Spark Gateway / Kafka Producer │
└──────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
                            DATA PLANE
┌──────────────────────────────────────────────────────────────────────┐
│ Ecommerce PostgreSQL                                                │
│ public source -> staging/CDC Silver -> dbt dev/ci/prod schemas      │
│      │ JDBC                 │                                       │
│      ├──────────────► Spark batch ──► Iceberg catalog + MinIO       │
│      ├──────────────► Kafka ──► Spark stream ──► Iceberg            │
│      └─ WAL ─► Debezium ─► Avro/Registry ─► Bronze ─► Silver       │
└──────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
                         OBSERVABILITY PLANE
┌──────────────────────────────────────────────────────────────────────┐
│ Platform Exporter -> Prometheus -> Grafana                          │
│                                └-> Alertmanager                     │
└──────────────────────────────────────────────────────────────────────┘
```

Control plane quyết định công việc nào chạy. Data plane lưu và xử lý dữ liệu.
Observability plane đo cả hai. Tách khái niệm này giúp tránh dùng Airflow làm
compute engine hoặc dùng monitoring database làm source of truth.

## 3. Physical topology: container và node

Compose định nghĩa 35 services: 21 process chạy dài hạn, 4 init job chạy rồi
thoát và 10 one-shot tool chỉ được tạo khi gọi `docker compose run`.

### 3.1 Long-running containers: 21

| Nhóm | Container/service | Node count | Trách nhiệm |
| --- | --- | ---: | --- |
| Data | `postgres` | 1 PostgreSQL node | source, staging, dbt schemas, watermark, Iceberg JDBC catalog |
| Airflow state | `airflow-postgres` | 1 PostgreSQL node | metadata/result backend riêng của Airflow |
| Airflow queue | `redis` | 1 Redis node | Celery broker |
| Airflow | `airflow-api-server` | 1 | UI và REST API |
| Airflow | `airflow-scheduler` | 1 | schedule task instance |
| Airflow | `airflow-dag-processor` | 1 | parse DAG files |
| Airflow | `airflow-worker` | 1 worker, concurrency 4 | thực thi batch task |
| Airflow | `airflow-triggerer` | 1 | xử lý deferred/async wait |
| Spark | `spark-master` | 1 master | cấp tài nguyên cho application |
| Spark | `spark-worker` | 1 worker, 2 cores/3 GiB | chạy executor |
| Spark | `spark-gateway` | 1 driver gateway | allow-list và submit finite Spark jobs |
| Streaming | `kafka` | 1 combined broker/controller | KRaft event log |
| CDC | `debezium-connect` | 1 Connect worker, 1 PostgreSQL source task | WAL -> table topics |
| Contract | `schema-registry` | 1 Apicurio node | Avro subjects, versions và compatibility |
| CDC apply | `customer-cdc-materializer` | 1 consumer process | Avro topic -> PostgreSQL Bronze/Silver |
| Streaming | `order-stream` | 1 long-running Spark driver | đọc Kafka micro-batch và ghi Iceberg |
| Lake storage | `minio` | 1 object-storage node | bucket `warehouse` chứa Iceberg files |
| Monitoring | `platform-exporter` | 1 | chuyển health/data state thành Prometheus metrics |
| Monitoring | `prometheus` | 1 | scrape, TSDB, evaluate rules |
| Monitoring | `alertmanager` | 1 | group/deduplicate alert local |
| Monitoring | `grafana` | 1 | dashboard được provision từ Git |

Một Spark application còn tạo executor process bên trong `spark-worker`; nó
không phải container service riêng trong file Compose. `order-stream` là driver
liên tục, còn executor được master cấp trên worker.

### 3.2 Init jobs: 4

| Service | Chạy khi nào | Vì sao phải kết thúc? |
| --- | --- | --- |
| `airflow-init` | trước Airflow daemons | migrate metadata schema một lần |
| `minio-init` | sau MinIO healthy | tạo bucket idempotently |
| `kafka-init` | sau Kafka healthy | tạo topic/partition/config idempotently |
| `cdc-bootstrap` | sau Connect healthy | tạo role/publication và upsert connector config |

Trạng thái `Exited (0)` của init container là thành công, không phải component
down. Giữ init logic riêng giúp startup repeatable và không nhét migration vào
mọi daemon.

### 3.3 One-shot tools: 10

| Service | Công việc |
| --- | --- |
| `spark-submit` | batch PostgreSQL -> partitioned Parquet demo |
| `iceberg-submit` | incremental PostgreSQL -> Iceberg fact |
| `iceberg-maintenance` | compact files và expire snapshots |
| `order-producer` | publish deterministic order events |
| `order-stream-once` | xử lý Kafka backlog rồi dừng |
| `cdc-smoke-test` | chứng minh customer `c/u/d` và delete tombstone |
| `schema-contract-test` | chứng minh policy và breaking schema bị từ chối |
| `customer-cdc-once` | drain CDC backlog rồi thoát |
| `cdc-materialization-test` | chứng minh Bronze/Silver create/update/delete |
| `cdc-reconcile` | đối soát source với Silver current state |

Các service này thuộc profile `tools` và không chạy khi `up`. Chúng dùng cùng
image/config với runtime thật để tránh “test trên môi trường khác production”.

## 4. Vì sao node count như vậy?

| Hệ thống | Local | Production thường cần | Trade-off local |
| --- | ---: | ---: | --- |
| PostgreSQL | 1 node/database role | primary + replica/PITR | không HA, mất node là downtime |
| Kafka | 1 broker/controller, RF=1 | >=3 broker/controller, RF=3 | không chịu được broker loss |
| Kafka Connect | 1 worker/1 source task | >=2 workers; connector-specific HA | restart ngắn gây lag, slot giữ WAL |
| Spark | 1 master + 1 worker | HA master + nhiều worker/autoscale | không parallel theo nhiều máy |
| MinIO | 1 node | distributed/managed object store | không erasure coding/HA |
| Airflow scheduler | 1 | >=2 scheduler, nhiều worker | control plane single point |
| Prometheus | 1 | replicated/remote write | monitoring cùng failure domain |

Chạy ba “node” Kafka hay nhiều Spark worker trên cùng laptop không tạo failure
domain thật; nó chỉ tốn RAM. Project ưu tiên một node nhưng ghi rõ production
delta để không nhầm local topology với thiết kế triển khai thật.

## 5. Network topology và port

Compose tạo một bridge network mặc định. Mọi service trong project resolve nhau
qua service name. Không publish mọi internal port ra host; chỉ publish giao diện
hoặc API cần người dùng truy cập.

| Host port | Container endpoint | Mục đích |
| ---: | --- | --- |
| 5432 | `postgres:5432` | SQL client/dbt từ host |
| 8080 | `airflow-api-server:8080` | Airflow UI/API |
| 7077 | `spark-master:7077` | Spark master endpoint |
| 8081 | `spark-master:8080` | Spark master UI |
| 8082 | `spark-worker:8080` | Spark worker UI |
| 8083 | `debezium-connect:8083` | Kafka Connect REST quản trị connector |
| 8084 | `schema-registry:8080` | Registry API; internal health ở port 9000 |
| 8090 | `spark-gateway:8090` | local job API/health |
| 9000 | `minio:9000` | S3-compatible API |
| 9001 | `minio:9001` | MinIO console |
| 9092 | `kafka:9092` | Kafka listener cho host |
| 9100 | `platform-exporter:9100` | custom metrics |
| 9090 | `prometheus:9090` | Prometheus UI/API |
| 9093 | `alertmanager:9093` | Alertmanager UI/API |
| 3000 | `grafana:3000` | Grafana UI |

Kafka có hai advertised listener: `kafka:19092` cho container và
`localhost:9092` cho host. Một advertised address sai làm client bootstrap được
nhưng thất bại khi broker trả metadata address khác.

Project chưa chia network public/private để giữ Compose dễ học. Production nên
chỉ expose ingress cần thiết và tách database/broker/object store vào private
subnet/network policy.

## 6. Storage topology

### 6.1 Named volumes

| Volume | Owner | Dữ liệu |
| --- | --- | --- |
| `ecommerce-postgres-data` | PostgreSQL ecommerce | source, staging, marts, catalog, watermark |
| `airflow-postgres-data` | Airflow PostgreSQL | scheduler metadata |
| `redis-data` | Redis | Celery broker state |
| `lakehouse-data` | Spark services | Parquet output và streaming checkpoint |
| `minio-data` | MinIO | Iceberg metadata/manifests/data files |
| `kafka-data` | Kafka | topic log và broker metadata |
| `prometheus-data` | Prometheus | time series 15 ngày |
| `alertmanager-data` | Alertmanager | alert state/silence |
| `grafana-data` | Grafana | local Grafana state |

Iceberg table state trải qua hai storage system: catalog pointer trong
PostgreSQL và immutable object tree trong MinIO. Backup chỉ một phía không đủ.

`lakehouse-data` không phải Iceberg warehouse; nó giữ Parquet demo và Spark
checkpoint. Iceberg warehouse thật nằm trong MinIO `s3://warehouse/`.

CDC state không có volume Connect riêng. Connector config/status/offset và
Registry journal nằm trong Kafka; replication slot/LSN và Bronze/Silver nằm
trong PostgreSQL. Các phía phải được phục hồi nhất quán.

### 6.2 Bind mounts

- `airflow/dags` được mount để DAG processor đọc code.
- dbt project files được mount read-only; `.venv`, `target`, `logs` của host
  không lọt vào Linux container.
- monitoring config/dashboard được mount read-only để Git là source of truth.
- PostgreSQL init SQL chỉ chạy khi volume database được tạo lần đầu.

## 7. Data flow theo từng path

### 7.1 Daily batch path

```text
public products/orders/order_items/payments --updated_at window--> staging.*
    ├── dbt build --> dbt_dev/dbt_ci/analytics
    ├── Spark JDBC --> Iceberg fact_sales --> MinIO
    └── producer --> Kafka durable log
all success --> metadata.etl_watermark commit
```

Watermark barrier không chờ streaming consumer. Kafka acknowledgement là durable
handoff; stream có thể catch up bằng checkpoint sau.

### 7.2 Streaming path

```text
Kafka topic (3 partitions)
   -> Spark readStream offset range
   -> parse + validate
   -> foreachBatch
      ├── event history MERGE by event_id
      ├── current state MERGE by order_id/version
      └── invalid payload MERGE by Kafka coordinate hash
   -> checkpoint offset after sink commit
```

Nếu driver chết sau Iceberg commit nhưng trước checkpoint, batch được replay.
Stable keys làm final state idempotent.

### 7.3 CDC path

```text
public.customers transaction commit
  -> PostgreSQL WAL / pgoutput
  -> ecommerce_cdc_avro_slot retains unread WAL
  -> Debezium source task + Apicurio Avro schema ID
  -> ecommerce_cdc_avro.public.customers (r/c/u/d + tombstone)
  -> cdc_bronze.customer_changes (append-only Kafka coordinates)
  -> cdc_silver.customers (latest state + delete tombstone)
  -> customers_current -> stg_customers -> snapshot -> dim_customer
```

Consumer commit PostgreSQL trước Kafka offset. Nếu crash ở giữa, record được
replay nhưng Bronze primary key biến nó thành no-op. Airflow không còn extract
customer, vì vậy Silver là single owner và không có dual writer.

### 7.4 Observability path

Exporter chủ động query PostgreSQL/Kafka/HTTP endpoint mỗi 15 giây. Prometheus
scrape exporter và evaluate rules. Grafana chỉ đọc Prometheus; Alertmanager nhận
firing alert. Exporter không crash khi dependency lỗi để vẫn xuất metric
`platform_component_up=0`.
Với CDC, exporter còn kiểm tra connector/task state, slot active và WAL bytes
đang bị slot giữ lại; ping port Connect không đủ chứng minh capture đang chạy.

## 8. Compute và resource allocation

- Airflow worker concurrency là 4, đủ chạy bốn extract và ba
  downstream branch nhưng vẫn giới hạn laptop load.
- Spark worker có 2 cores/3 GiB.
- Long-running stream dùng tối đa 1 core/1 GiB executor để chừa tài nguyên.
- Gateway finite job dùng tối đa 1 core/2 GiB executor; lock serializes local
  submissions để tránh hai driver tranh worker nhỏ.
- PostgreSQL ecommerce giới hạn 2 GiB trong lab.
- Kafka Connect giới hạn 1 GiB container, Java heap tối đa 768 MiB.
- Schema Registry giới hạn 1 GiB; customer materializer là Python process nhẹ.

Đây là capacity guardrail, không phải sizing formula. Production phải benchmark
dựa trên input volume, shuffle, state size, file size và SLA.

## 9. Security boundary

Hiện tại:

- credential lấy từ `.env`;
- Debezium dùng replication role riêng thay vì PostgreSQL superuser;
- Kafka/internal HTTP dùng plaintext;
- UI publish trên localhost;
- Spark gateway chỉ cho phép job name định trước, không nhận arbitrary command;
- image runtime chạy non-root khi khả thi.

Allow-list gateway quan trọng vì mount Docker socket vào Airflow gần như trao
quyền root host. Production cần TLS, SASL, RBAC, network policy, external secret
manager và audit log.

## 10. Failure domain và recovery

| Failure | State bền còn ở đâu? | Recovery owner |
| --- | --- | --- |
| Airflow worker restart | Airflow DB, staging/sinks | Celery/Airflow retry |
| Spark stream restart | Kafka log + checkpoint + Iceberg | service restart + replay |
| Spark batch failure | committed Iceberg snapshot cũ | Airflow retry |
| Kafka broker restart | `kafka-data` volume | broker restart |
| Debezium restart | Connect offset topic + PostgreSQL slot | Connect restart/catch-up |
| Customer materializer restart | Kafka consumer offset + Bronze PK | replay idempotent/catch-up |
| Registry restart | KafkaSQL journal topic | rebuild Registry state từ Kafka |
| Debezium dừng quá lâu | slot giữ WAL tới safety cap | khôi phục connector hoặc controlled resnapshot |
| MinIO restart | `minio-data` volume | service restart |
| Candidate watermark treo | PostgreSQL metadata | operator theo runbook |

Volume bảo vệ trước container recreation, không bảo vệ trước disk/host loss.
Off-host backup và restore drill vẫn là khoảng trống production.

## 11. Vị trí implementation

- Orchestration: `airflow/dags/`
- Relational initialization: `postgres/init/`
- Analytics engineering: `dbt/`
- Spark batch/lakehouse/stream: `spark/jobs/`
- Event producer: `kafka/producer/`
- CDC bootstrap/materialization/reconciliation: `cdc/`
- Registry contract verification: `schema_registry/`
- Monitoring: `monitoring/`
- CI: `.github/workflows/`
- Operations: `docs/runbook.md`
- File-by-file guide: `docs/code-map.md`
- Current implemented baseline: `docs/current-state.md`
- Unfinished production work: `docs/backlog.md`
