# Bản đồ code: file nào làm gì và đọc theo thứ tự nào

Tài liệu này trả lời câu hỏi “tôi đang mở file gì?” trước khi đi vào syntax.
Code comment giải thích local decision; các bài `docs/learning` giải thích khái
niệm và trade-off rộng hơn.

Nếu các khái niệm từ dbt incremental trở đi còn mới, đọc
[`docs/learning/00-beginner-map.md`](learning/00-beginner-map.md) trước. Bản đồ
đó nối state/retry/delete xuyên suốt các tool; tài liệu này trả lời mỗi phần
implementation nằm ở đâu.

## 1. Điểm vào nên đọc trước

| Thứ tự | File | Câu hỏi file trả lời |
| ---: | --- | --- |
| 1 | `docker-compose.yml` | process nào tồn tại, nối với nhau và giữ state ở đâu? |
| 2 | `.env.example` | runtime contract cần biến cấu hình nào? |
| 3 | `airflow/dags/ecommerce_pipeline.py` | daily batch đi theo dependency nào? |
| 4 | `postgres/init/003_create_etl_metadata.sql` | pipeline nhớ progress bằng gì? |
| 5 | `dbt/models/` | staging được biến thành model phân tích thế nào? |
| 6 | `spark/jobs/iceberg_sales.py` | cùng business transform được ghi lakehouse ra sao? |
| 7 | `kafka/producer/publish_orders.py` | event identity/window được tạo thế nào? |
| 8 | `spark/jobs/order_stream.py` | replay/checkpoint/DLQ hoạt động thế nào? |
| 9 | `monitoring/platform_exporter.py` | health và freshness được đo thế nào? |
| 10 | `cdc/bootstrap.py` | WAL capture được cấp quyền và deploy idempotently thế nào? |

## 2. Root và runtime definition

| File | Vai trò | Lưu ý |
| --- | --- | --- |
| `README.md` | quickstart và scope | không thay runbook/architecture deep dive |
| `ROADMAP.txt` | trạng thái level và accepted risks | không phải task scheduler |
| `docs/platform-capabilities.md` | chức năng hiện có + advanced backlog | góc nhìn capability, không thay code map |
| `docker-compose.yml` | topology local executable | anchors tránh lặp Airflow/Spark config |
| `.env.example` | contract cấu hình không bí mật | copy thành `.env`, không commit secret |
| `requirements.txt` | dependency cho data generator/host tools | runtime container có requirements riêng |

## 3. PostgreSQL và dữ liệu mẫu

| File | Vai trò |
| --- | --- |
| `postgres/init/001_schema.sql` | tạo OLTP source tables, constraints, indexes |
| `postgres/init/002_staging_analytics_schema.sql` | tạo landing/current-state staging và legacy analytics schema |
| `postgres/init/003_create_etl_metadata.sql` | tạo/seed committed và candidate watermark |
| `postgres/sql/001_correction.sql` | câu lệnh sửa dữ liệu phục vụ bài thực hành |
| `postgres/sql/002_validation.sql` | query kiểm tra dữ liệu thủ công |
| `postgres/ci/*.sql` | seed nhỏ, mutation và assertion cho dbt CI |
| `data-generator/*.py` | sinh dataset OLTP lớn cho local benchmark |

Các script trong `/docker-entrypoint-initdb.d` chỉ chạy lúc PostgreSQL volume
rỗng. Sửa init SQL không tự migrate database đã tồn tại.

## 4. Airflow

| File | Vai trò |
| --- | --- |
| `airflow/Dockerfile` | Airflow runtime + dbt/Kafka producer venv biệt lập |
| `airflow/dags/ecommerce_pipeline.py` | daily incremental/backfill DAG và commit barrier |
| `airflow/dags/lakehouse_maintenance.py` | weekly Iceberg housekeeping DAG |
| `airflow/dags/ecommerce_reset_other_schema.py` | destructive local reset, manual only |

`ecommerce_pipeline.py` là control flow, không chứa Spark transformation. Nó gọi
database, dbt executable, Spark gateway và producer theo dependency.

## 5. dbt

| Path/file | Vai trò |
| --- | --- |
| `dbt/dbt_project.yml` | project metadata và source paths |
| `dbt/profiles.yml` | targets dev/ci/prod, connection qua env vars |
| `dbt/models/staging/_sources.yml` | khai báo physical source relation |
| `dbt/models/staging/stg_*.sql` | rename/cast/standardize 1:1 từ staging tables |
| `dbt/models/staging/_staging_models.yml` | docs + generic tests của staging contract |
| `dbt/models/intermediate/int_*.sql` | reusable business joins/aggregations |
| `dbt/models/intermediate/_intermediate_models.yml` | grain/docs/tests intermediate |
| `dbt/snapshots/_snapshots.yml` | SCD2 customer snapshot definition + tests |
| `dbt/models/marts/dim_*.sql` | consumer-facing dimensions |
| `dbt/models/marts/fact_sales.sql` | incremental fact ở order-item grain |
| `dbt/models/marts/daily_sales.sql` | incremental daily aggregate |
| `dbt/models/marts/_marts_models.yml` | mart contract và generic tests |
| `dbt/tests/**/*.sql` | singular tests trả về failure rows |

Naming convention:

- `stg_`: source-conformed view, không business aggregation.
- `int_`: intermediate building block, không hứa ổn định cho BI.
- `dim_`: descriptive analytical entity.
- `fact_`: measurable event/process ở grain đã khai báo.
- `snap_`: dbt-managed historical source versions.
- `assert_`: singular data invariant; query đúng phải trả zero rows.

## 6. Spark và Iceberg

| File | Vai trò |
| --- | --- |
| `spark/Dockerfile` | Spark runtime với JDBC, Iceberg, AWS và Kafka jars ghim version |
| `spark/jobs/sales_batch.py` | JDBC -> transform -> validate -> Parquet overwrite demo |
| `spark/jobs/iceberg_common.py` | cấu hình SparkSession dùng chung cho JDBC catalog + MinIO |
| `spark/jobs/iceberg_sales.py` | incremental/merge fact vào Iceberg |
| `spark/jobs/order_stream.py` | Kafka Structured Streaming -> history/current/DLQ Iceberg |
| `spark/jobs/maintain_iceberg.py` | compact data/delete files và expire snapshots |
| `spark/jobs/validate_platform.py` | read-only row/grain verification |
| `spark/gateway/job_gateway.py` | HTTP allow-list để Airflow submit finite job |
| `spark/sql/iceberg_demo.sql` | query snapshot/time travel thực hành |
| `spark/tests/*.py` | transformation/parser unit tests bằng local Spark |
| `spark/kafka-pom.xml` | Maven dependency set cho Spark Kafka connector |

`sales_batch.py` cố ý ghi plain Parquet để cho thấy giới hạn file format;
`iceberg_sales.py` là phiên bản có table transaction/snapshot. Hai file giống
business transform nhưng khác storage semantics.

## 7. Kafka

| File | Vai trò |
| --- | --- |
| `kafka/producer/publish_orders.py` | đọc đúng watermark window, tạo event UUID5, publish/flush |
| `kafka/producer/Dockerfile` | one-shot producer image non-root |
| `kafka/producer/requirements.txt` | psycopg + confluent-kafka versions |

Kafka broker/topic được cấu hình trong `docker-compose.yml`; repository không có
custom broker code.

## 8. Monitoring

| File | Vai trò |
| --- | --- |
| `monitoring/platform_exporter.py` | collect semantic metrics từ PostgreSQL/Kafka/HTTP |
| `monitoring/prometheus/prometheus.yml` | scrape target, rule file, Alertmanager endpoint |
| `monitoring/prometheus/alerts.yml` | alert expressions và `for` duration |
| `monitoring/alertmanager/alertmanager.yml` | route/group receiver local |
| `monitoring/grafana/provisioning/` | datasource/dashboard provisioning |
| `monitoring/grafana/dashboards/platform-overview.json` | dashboard source-controlled |

## 8.1 CDC

| File | Vai trò |
| --- | --- |
| `cdc/bootstrap.py` | tạo/rotate replication role, publication và PUT connector config |
| `cdc/verify_cdc.py` | kiểm tra end-to-end `c/u/d` + delete tombstone |

Debezium không nằm trong Airflow DAG. Nó là long-running Kafka Connect source
connector đọc PostgreSQL WAL; Airflow batch extractor vẫn là owner của staging
cho tới bước Bronze/Silver cutover.

## 9. CI

| File | Vai trò |
| --- | --- |
| `.github/workflows/dbt-ci.yml` | dựng PostgreSQL nhỏ, build hai lần, mutate, assert incremental |
| `.github/workflows/platform-ci.yml` | compile Python, validate config, unit test Spark, build images |

CI dùng fixture nhỏ để kiểm tra logic nhanh; end-to-end baseline lớn vẫn được
chạy local và ghi trong Level 11.

## 10. File generated hoặc local-only không nên đọc như source

- `dbt/target/`: compiled SQL, manifest và run artifacts.
- `dbt/logs/`, `airflow/logs/`: runtime logs.
- `dbt/.venv/`: local Python environment.
- named volumes: runtime data, không nằm trong Git.
- `command.txt`, `dbt/command.txt`: scratch commands, không phải contract.

Nếu compiled SQL khác source model, source/Jinja là nơi sửa; `target` sẽ được dbt
tạo lại.
