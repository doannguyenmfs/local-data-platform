# Trung tâm tài liệu Local Data Platform

Đây là điểm bắt đầu duy nhất khi chưa biết cần đọc file nào.

- Baseline code: P0 tại `33d76fc`.
- Trạng thái hiện tại: P0 hoàn thành; P1/P2/P3 vẫn là backlog.
- Ngày rà soát toàn bộ tài liệu: 2026-09-18.
- Project scope: production-shaped local lab, không phải production deployment.

## 1. Nếu chỉ có một phút

| Câu hỏi | Đọc tài liệu |
| --- | --- |
| Project hiện đã làm được gì? | [`current-state.md`](current-state.md) |
| Hệ thống gồm những container/node/state nào? | [`architecture.md`](architecture.md) |
| File code nào chịu trách nhiệm việc gì? | [`code-map.md`](code-map.md) |
| Chạy, kiểm tra và xử lý lỗi thế nào? | [`runbook.md`](runbook.md) |
| Chức năng mang lại cho người dùng/operator là gì? | [`platform-capabilities.md`](platform-capabilities.md) |
| Việc nào chưa làm? | [`backlog.md`](backlog.md) |
| Học project theo thứ tự nào? | [`learning/README.md`](learning/README.md) |
| Tự tìm hiểu Airflow/Kafka bằng keyword nào? | [`learning/16-self-study-keywords.md`](learning/16-self-study-keywords.md) |
| Thực hành gì để tiến tới Mid/Senior? | [`learning/17-senior-data-engineer-roadmap.md`](learning/17-senior-data-engineer-roadmap.md) |

## 2. Source of truth và thứ tự ưu tiên

Các tài liệu nhìn cùng một hệ thống từ các góc khác nhau. Khi thấy thông tin có
vẻ mâu thuẫn, dùng thứ tự sau:

1. **Executable source:** `docker-compose.yml`, DAG, SQL, Python, dbt và workflow.
2. **Implemented status:** [`current-state.md`](current-state.md).
3. **Physical/logical design:** [`architecture.md`](architecture.md).
4. **Operation commands:** [`runbook.md`](runbook.md).
5. **File ownership:** [`code-map.md`](code-map.md).
6. **Conceptual explanation:** `learning/*.md`.
7. **Future design:** [`backlog.md`](backlog.md).

`backlog.md` và lab trong Level 17 không phải bằng chứng code đã tồn tại.
`platform-capabilities.md` mô tả P0 khá chi tiết nhưng phần A6–A20 chỉ là bản tóm
tắt roadmap; backlog là tài liệu có quyền quyết định Definition of Done.

Các con số runtime như row count, duration và snapshot ID là bằng chứng của một
lần chạy được ghi ngày cụ thể, không phải constant để code hoặc alert phụ thuộc.

## 3. Lộ trình đọc theo mục tiêu

### 3.1 Người mới mở repository lần đầu

```text
Root README
  → current-state
  → architecture
  → learning/00-beginner-map
  → code-map
  → level đang quan tâm
```

Kết quả mong muốn: phân biệt control plane, data plane, observability plane và
biết state/progress của mỗi đường dữ liệu nằm ở đâu.

### 3.2 Muốn chạy project ngay

```text
Root README quickstart
  → architecture: port/volume/profile
  → runbook: start/verify/recovery/shutdown
```

Không bắt đầu bằng script reset. Đặc biệt `down -v` xóa named volume và chỉ dùng
sau khi đã xác nhận đúng target, backup và nhu cầu reset.

### 3.3 Muốn hiểu batch incremental

1. [`learning/02-airflow-orchestration.md`](learning/02-airflow-orchestration.md)
2. [`learning/04-incremental-backfill.md`](learning/04-incremental-backfill.md)
3. [`learning/05-dbt-incremental.md`](learning/05-dbt-incremental.md)
4. [`learning/09-production-integration.md`](learning/09-production-integration.md)
5. `airflow/dags/ecommerce_pipeline.py` theo [`code-map.md`](code-map.md)

Tập trung vào committed/candidate watermark, half-open backfill, idempotent sink
và commit barrier. Customer không còn thuộc batch polling sau CDC cutover.

### 3.4 Muốn hiểu Kafka và streaming

1. [`learning/16-self-study-keywords.md`](learning/16-self-study-keywords.md), phần Kafka.
2. [`learning/08-kafka.md`](learning/08-kafka.md).
3. [`learning/07-iceberg.md`](learning/07-iceberg.md), phần MERGE/checkpoint state.
4. [`learning/09-production-integration.md`](learning/09-production-integration.md).
5. `kafka/producer/` và `spark/jobs/order_stream.py`.

Tập trung vào partition/key/offset, delivery acknowledgement, consumer progress,
checkpoint, replay, deterministic event ID và DLQ.

### 3.5 Muốn hiểu CDC và delete

1. [`learning/12-cdc-debezium.md`](learning/12-cdc-debezium.md).
2. [`learning/13-schema-registry-avro.md`](learning/13-schema-registry-avro.md).
3. [`learning/14-cdc-bronze-silver-cutover.md`](learning/14-cdc-bronze-silver-cutover.md).
4. `cdc/bootstrap.py`, `cdc/materialize_customers.py` và reconciliation tools.

Luôn phân biệt bốn bước: capture → store → apply → cutover. Customer delete đã
được apply vào Silver và đóng current SCD2 version; bốn table batch vẫn còn
hard-delete/late-row debt được ghi ở backlog A18/A19.

### 3.6 Muốn chuẩn bị phỏng vấn

- Tổng quan: [`current-state.md`](current-state.md).
- Warehouse/dbt: Level 3, 5 và 15.
- Batch DE: Level 2, 4, 6, 7 và 9.
- Streaming/CDC: Level 8, 12, 13 và 14.
- Production trade-off: Level 10 và 11.
- Keyword để tự học: Level 16.
- Lab tạo evidence Mid/Senior: Level 17.

Đừng học thuộc danh sách công nghệ. Với mỗi component phải trả lời: input nào,
state ở đâu, retry ra sao, success nghĩa là gì và owner nào phục hồi.

### 3.7 Muốn tiếp tục phát triển project

```text
current-state
  → backlog priority + Definition of Done
  → learning/17 lab tương ứng
  → ADR/design
  → code + positive/failure test
  → runbook + docs + evidence
```

Không chuyển backlog item sang Done nếu mới có config hoặc happy path.

## 4. Danh mục tài liệu cốt lõi

| File | Đối tượng | Nội dung | Không dùng để làm gì |
| --- | --- | --- | --- |
| [`../README.md`](../README.md) | người mới/operator | giới thiệu, quickstart, command phổ biến | không thay deep dive |
| [`../ROADMAP.txt`](../ROADMAP.txt) | maintainer | level P0, accepted risks, hướng tiếp theo | không phải task tracker chi tiết |
| [`current-state.md`](current-state.md) | mọi người | implemented baseline và evidence | không mô tả implementation từng dòng |
| [`architecture.md`](architecture.md) | DE/platform engineer | logical/physical topology, network, storage, failure domain | không thay runbook |
| [`code-map.md`](code-map.md) | người đọc code | trách nhiệm từng path/file và thứ tự đọc | không giải thích toàn bộ khái niệm |
| [`platform-capabilities.md`](platform-capabilities.md) | interviewer/stakeholder | project làm được gì và giới hạn | không phải trạng thái backlog authoritative |
| [`runbook.md`](runbook.md) | operator | command, verify, recovery, shutdown | không thay design rationale |
| [`backlog.md`](backlog.md) | maintainer/learner | A6–A20, lý do, design dự kiến, Done criteria | không được đọc như capability đã triển khai |
| [`learning/README.md`](learning/README.md) | người học | Level 1–15 và phụ lục 00/16/17 theo mục tiêu | không phải runtime source |

## 5. Danh mục learning đầy đủ

| Level/file | Đọc khi cần hiểu | File/code đối chiếu chính |
| --- | --- | --- |
| [`00-beginner-map.md`](learning/00-beginner-map.md) | state/retry/delete xuyên toàn platform | current state + architecture |
| [`01-platform-foundations.md`](learning/01-platform-foundations.md) | container, volume, network, config | Compose, Dockerfiles, `.env.example` |
| [`02-airflow-orchestration.md`](learning/02-airflow-orchestration.md) | DAG/task/schedule/retry/dependency | `airflow/dags/` |
| [`03-warehouse-modeling.md`](learning/03-warehouse-modeling.md) | grain, fact/dimension, SCD2 | `dbt/models/marts`, snapshots |
| [`04-incremental-backfill.md`](learning/04-incremental-backfill.md) | watermark/candidate/backfill/late data | DAG + metadata SQL |
| [`05-dbt-incremental.md`](learning/05-dbt-incremental.md) | dbt layers, incremental fact/daily | `dbt/models/` |
| [`06-spark.md`](learning/06-spark.md) | driver/executor/partition/shuffle | `spark/jobs/sales_batch.py` |
| [`07-iceberg.md`](learning/07-iceberg.md) | catalog/snapshot/MERGE/maintenance | Iceberg Spark jobs |
| [`08-kafka.md`](learning/08-kafka.md) | topic/partition/offset/replay/DLQ | producer + order stream |
| [`09-production-integration.md`](learning/09-production-integration.md) | multi-sink orchestration/retry | DAG + Spark gateway |
| [`10-observability.md`](learning/10-observability.md) | metrics/alerts/freshness | `monitoring/` |
| [`11-verification-and-tradeoffs.md`](learning/11-verification-and-tradeoffs.md) | test pyramid/readiness gaps | CI + runbook |
| [`12-cdc-debezium.md`](learning/12-cdc-debezium.md) | WAL/LSN/slot/publication/envelope | CDC bootstrap/verification |
| [`13-schema-registry-avro.md`](learning/13-schema-registry-avro.md) | schema ID/version/compatibility | Registry config/test |
| [`14-cdc-bronze-silver-cutover.md`](learning/14-cdc-bronze-silver-cutover.md) | apply delete, reconcile, ownership | CDC materializer + SQL |
| [`15-dbt-contracts-slim-ci.md`](learning/15-dbt-contracts-slim-ci.md) | contracts/state/defer/CI | dbt YAML + GitHub Actions |
| [`16-self-study-keywords.md`](learning/16-self-study-keywords.md) | keyword và search query bên ngoài | toàn project |
| [`17-senior-data-engineer-roadmap.md`](learning/17-senior-data-engineer-roadmap.md) | lab SLO/DR/security/capacity/cloud | backlog A6–A20 |

`00`, `16` và `17` là phụ lục/bản đồ, không phải capability level mới đã hoàn
thành. Level 1–15 tương ứng với baseline P0.

## 6. Tìm tài liệu theo component

| Component | Khái niệm | Kiến trúc/code | Vận hành |
| --- | --- | --- | --- |
| Docker/PostgreSQL | Level 1, 4 | architecture, code-map | runbook 1, 4, 7 |
| Airflow | Level 2, 4, 9, 16 | architecture + DAG | runbook 2, 4 |
| dbt | Level 3, 5, 15 | code-map phần dbt | runbook 3 |
| Spark | Level 6, 9 | architecture + Spark jobs | runbook 2–4 |
| Iceberg/MinIO | Level 7 | architecture storage | runbook 3, 4, 6, 7 |
| Kafka | Level 8, 16 | architecture streaming | runbook 3, 4 |
| CDC/Debezium | Level 12–14 | architecture CDC | runbook 3, 4 |
| Registry/Avro | Level 13 | code-map CDC/Registry | runbook verification |
| Monitoring | Level 10–11 | architecture observability | runbook 8 |
| CI/delivery | Level 11, 15 | `.github/workflows/` | release checklist |
| Production gaps | Level 17 | backlog | lab-specific runbook/evidence |

## 7. Quy ước trạng thái trong tài liệu

- **Implemented/P0/Hoàn thành:** có code, wiring và executable proof.
- **Local-ready:** chạy đủ trong lab nhưng thiếu HA/security/process production.
- **Accepted risk/Deferred:** biết rõ giới hạn và có recovery tạm, chưa sửa gốc.
- **Planned/Backlog/P1–P3:** chưa được phép mô tả như capability hiện có.
- **Evidence dated:** kết quả một lần chạy tại ngày/commit cụ thể.

Nếu một lab Level 17 được hoàn thành, phải cập nhật tối thiểu:

1. `current-state.md`;
2. `architecture.md` và `code-map.md` nếu topology/path đổi;
3. `runbook.md`;
4. `platform-capabilities.md`;
5. `backlog.md`;
6. bài learning tương ứng;
7. root `README.md` và `ROADMAP.txt` nếu milestone đổi.

## 8. Quy tắc giữ tài liệu mới nhất

1. Không ghi secret, token, payload PII hoặc database dump vào tài liệu.
2. Command phải chạy từ repository root trừ khi nói rõ.
3. Ghi rõ host context hay container context khi port/path khác nhau.
4. Con số topology phải được đối chiếu với `docker compose --profile '*' config`.
5. File/path được nhắc tới phải tồn tại hoặc được đánh dấu “dự kiến”.
6. Kết quả benchmark/test phải ghi ngày, commit, workload và environment.
7. Không sửa generated `dbt/target`, log hoặc virtualenv như source documentation.
8. Khi code và tài liệu khác nhau, sửa tài liệu hoặc code trong cùng change.

### Kiểm tra tài liệu tối thiểu trước commit

```bash
git diff --check
docker compose --profile '*' config --quiet

# Kiểm tra thủ công các link/path vừa thêm và trạng thái implemented/planned.
git diff -- README.md ROADMAP.txt docs/
```

Local-link audit của lần rà soát này được chạy thủ công và ghi kết quả ở mục 10;
repository chưa tuyên bố có một Markdown link/lint gate tự động trong CI.

## 9. Runtime version matrix của baseline

Bảng này phản ánh version được pin trong repository tại ngày rà soát, không
tuyên bố đó là version mới nhất của upstream.

| Thành phần | Version trong project | Nguồn cấu hình |
| --- | --- | --- |
| Airflow | 3.3.0, Python 3.12 | `airflow/Dockerfile` |
| dbt Core / PostgreSQL adapter | 1.12.3 / 1.11.0 | `dbt/requirements.txt` |
| PostgreSQL | 16 | `docker-compose.yml` |
| Redis | 7 Alpine | `docker-compose.yml` |
| Spark | 3.5.9, Scala 2.12, Java 17 | `spark/Dockerfile` |
| Kafka | 4.3.1, KRaft | `docker-compose.yml` |
| Debezium Connect | 3.6.2.Final | `docker-compose.yml` |
| Apicurio Registry | 3.2.5 | `docker-compose.yml` |
| MinIO | RELEASE.2025-09-07T16-13-09Z | `docker-compose.yml` |
| Prometheus | 3.13.3 | `docker-compose.yml` |
| Alertmanager | 0.34.0 | `docker-compose.yml` |
| Grafana | 13.2.1 | `docker-compose.yml` |

Khi nâng version, đọc migration/release notes, build lại image, chạy full
baseline proof và cập nhật bảng trong cùng commit. “Image pull/build thành công”
không chứng minh schema, plugin, connector hoặc persisted state tương thích.

## 10. Kết quả audit tài liệu 2026-09-18

- 28 file tài liệu được đưa vào local-link audit, không có link local hỏng.
- Compose render hợp lệ với 35 service definitions và 9 named volumes.
- Physical inventory vẫn là 21 long-running, 4 init và 10 one-shot services.
- Batch path hiện có bốn candidate watermark active; customer polling đã retire.
- Customer CDC capture/store/apply/delete/cutover được mô tả nhất quán.
- P0 implemented và A6–A20 planned được tách rõ.
- Port host/container được giữ đúng context, đặc biệt Kafka `9092`/`19092`.
- `git diff --check`, Python compile và Compose config là release checks bắt buộc.

Audit này xác nhận tài liệu khớp repository tại thời điểm ghi. Một thay đổi code,
version, topology hoặc ownership sau đó phải cập nhật tài liệu theo mục 7–8.
