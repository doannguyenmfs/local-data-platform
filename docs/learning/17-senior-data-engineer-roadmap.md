# Từ project P0 đến năng lực Senior Data Engineer

Tài liệu này trả lời hai câu hỏi:

1. Project hiện tại còn thiếu bằng chứng nào để thể hiện năng lực Senior DE?
2. Phải thực hành thế nào để biến một khái niệm thành kinh nghiệm có thể giải
   thích và bảo vệ trong phỏng vấn?

Đây là **lộ trình thực hành**, không phải danh sách capability đã có. Baseline
đang chạy vẫn là P0 trong [`../current-state.md`](../current-state.md); các mục
chưa hoàn thành được quản lý trong [`../backlog.md`](../backlog.md).

## 1. Seniority không phải số lượng công nghệ

| Cấp độ | Dấu hiệu chính | Câu hỏi thường trả lời |
| --- | --- | --- |
| Junior | Hoàn thành task có phạm vi rõ | “Code task này thế nào?” |
| Mid | Sở hữu một pipeline hoặc data domain | “Thiết kế, deploy và vận hành luồng này thế nào?” |
| Senior | Sở hữu outcome xuyên nhiều hệ thống/team | “Reliability, migration, chi phí và blast radius được kiểm soát thế nào?” |

Senior không nhất thiết tự viết mọi dòng code. Senior phải làm rõ requirement,
chọn trade-off, đặt success/failure boundary, tạo bằng chứng, chuẩn bị rollback
và giúp nhiều người vận hành hệ thống an toàn.

Project P0 đã có nền tảng kỹ thuật tốt: watermark/candidate, idempotent sinks,
dbt contracts, Kafka replay, CDC delete, Iceberg state và semantic monitoring.
Khoảng trống lớn nhất là production evidence: workload/SLO thật, restore drill,
security proof, failure-domain test, chi phí và ownership nhiều team.

## 2. Cách thực hành đúng

Mỗi lab trong tài liệu phải tạo đủ sáu artifact:

1. **Requirement:** workload, SLA/SLO, RPO/RTO hoặc security objective.
2. **Design:** sơ đồ state/owner/failure boundary và trade-off.
3. **Implementation:** code/config nhỏ nhất giải quyết requirement.
4. **Executable proof:** happy path và failure path tái chạy được.
5. **Runbook/rollback:** operator làm gì khi thất bại.
6. **Result:** metric trước/sau, giới hạn và điều chưa chứng minh.

Nếu chỉ thêm container hoặc chụp ảnh UI, lab chưa hoàn thành.

### Cấu trúc thư mục bằng chứng đề xuất

Khi bắt đầu làm các lab, tạo cấu trúc sau bằng commit riêng:

```text
docs/
  adr/                         # quyết định kiến trúc và trade-off
  incidents/                   # timeline + postmortem của failure drill
  evidence/
    slo/
    restore/
    capacity/
    security/
    migration/
```

Không commit secret, database dump thật, WAL, customer payload hoặc file dữ liệu
lớn. Chỉ commit Markdown, script tái tạo, metric tổng hợp và dữ liệu giả.

## 3. Lab 0 — Bảo vệ baseline P0 trước khi nâng cấp

### Mục tiêu

Tạo release evidence để biết nâng cấp sau này có phá correctness hiện tại hay
không. Không có baseline thì mọi con số “nhanh hơn” hoặc “ổn định hơn” đều thiếu
điểm so sánh.

### Thực hành

1. Dựng stack P0 theo [`../runbook.md`](../runbook.md).
2. Chạy static validation, dbt build, Spark unit test và Compose validation.
3. Trigger `ecommerce_pipeline` hai lần với source không đổi.
4. Chạy CDC create/update/delete/materialization/reconcile tests.
5. Chạy Iceberg validation và ghi snapshot ID trước/sau no-op run.
6. Ghi timestamp, Git commit, máy/RAM/CPU và dataset size.

### Bằng chứng pass

- dbt và Spark tests đều pass;
- lần chạy thứ hai không nhân business grain;
- bốn candidate watermark active trở về `NULL`;
- CDC reconciliation có `differences=0`, `duplicate_offsets=0`;
- no-op Iceberg run không tạo state ngoài dự kiến;
- working tree sạch và commit được ghi trong evidence.

### Câu hỏi phỏng vấn tự trả lời

- Tại sao container healthy chưa đủ chứng minh pipeline đúng?
- Vì sao phải chạy replay test thay vì chỉ happy path một lần?
- State nào thuộc Airflow, PostgreSQL, Kafka, Spark checkpoint và Iceberg?

## 4. Lab 1 — SLI, SLO, external alert và incident

Liên quan backlog A6.

### Mục tiêu

Chuyển monitoring từ “có dashboard” thành reliability contract có owner và
notification được kiểm thử.

### Requirement mẫu

```text
Critical dataset: analytics.daily_sales
Freshness SLI: now - committed order watermark
Freshness SLO: 99% số phút trong 30 ngày có lag < 30 phút
Correctness SLI: số lần reconciliation pass / tổng số lần chạy
Correctness SLO: 99.9%
Owner: data-platform
Notification: webhook test receiver
```

### Thực hành

1. Chọn một dataset critical; không đặt SLO chung cho mọi table.
2. Viết định nghĩa numerator, denominator, cửa sổ và trường hợp maintenance.
3. Thêm recording rule để tính bad minutes hoặc success ratio.
4. Gắn `owner`, `severity` và `runbook_url` vào alert labels.
5. Dùng webhook receiver local/test; secret phải inject runtime.
6. Dừng một dependency có chủ đích, ví dụ Kafka hoặc exporter.
7. Đo thời gian pending → firing → notification → resolved.
8. Viết incident timeline và postmortem.

### Failure scenario

```text
Kafka dừng
  → semantic component check = 0
  → alert chờ đủ `for` duration
  → một firing notification
  → operator khôi phục Kafka
  → một resolved notification
```

### Bằng chứng pass

- notification firing/resolved tới đúng receiver đúng một lần;
- alert có owner và runbook;
- recording rule cho ra kết quả kiểm tra được bằng fixture;
- monitoring outage không mutate data hay advance watermark;
- postmortem phân biệt symptom, root cause, mitigation và permanent action.

### Mẹo Senior

Đừng đặt mọi alert là critical. Page cho hành động cần người xử lý ngay; ticket
cho xu hướng; dashboard cho điều tra. Alert không có owner/action là noise.

## 5. Lab 2 — Backup, PITR và restore drill

Liên quan backlog A7. Đây là lab production-value cao nhất.

### Mục tiêu

Chứng minh dữ liệu phục hồi được, không chỉ chứng minh file backup tồn tại.

### Safety boundary bắt buộc

- Không restore đè database/volume đang chạy.
- Dùng Compose project name, port và volume riêng.
- Gắn nhãn rõ `restore-drill`.
- Trước mọi lệnh xóa, in và xác nhận exact project/volume target.

### State inventory phải backup

| State | Thành phần | Rủi ro nếu thiếu |
| --- | --- | --- |
| Source/staging/marts/watermark/CDC | ecommerce PostgreSQL | mất business/progress state |
| Airflow metadata | Airflow PostgreSQL | mất run/task history và scheduling state |
| Catalog pointer | PostgreSQL Iceberg catalog | object files không còn logical table pointer |
| Data/metadata objects | MinIO warehouse | catalog trỏ tới object không tồn tại |
| Kafka records/config | Kafka | mất replay source/topic policy |
| CDC position | replication slot + Connect offsets | gap hoặc duplicate/resnapshot |
| Streaming progress | checkpoint | consumer replay từ vị trí khác |

### Thực hành

1. Chọn restore point và ghi source counts/watermarks/Iceberg snapshot IDs.
2. Tạo PostgreSQL logical dump cho lab đầu tiên.
3. Mirror MinIO warehouse sang thư mục/bucket ngoài Compose volume.
4. Export connector config, topic config và version information.
5. Dựng isolated restore stack với project name và port khác.
6. Restore PostgreSQL trước, rồi object tree tương ứng.
7. Chạy dbt tests, CDC reconciliation và Iceberg validation read-only.
8. Đo:

```text
RPO = restore target time - newest recovered durable data time
RTO = service recovery end time - incident/drill start time
```

9. Phá thử một thành phần backup, ví dụ thiếu Iceberg metadata object, và ghi
   lại validator phát hiện lỗi như thế nào.

### Nâng từ dump lên PITR

Sau khi restore dump ổn định, nghiên cứu:

- PostgreSQL base backup;
- continuous WAL archive;
- restore command;
- recovery target time;
- timeline khi failover;
- phối hợp database restore point với MinIO/catalog state.

### Bằng chứng pass

- restore diễn ra trong environment cô lập;
- schema/count/grain/reconciliation pass;
- Iceberg catalog và object tree cùng nhất quán;
- RPO/RTO được đo, không chỉ ước lượng;
- runbook ghi rõ thứ tự và rollback khi restore thất bại.

## 6. Lab 3 — Capacity, performance và cost envelope

Liên quan backlog A14.

### Mục tiêu

Trả lời bằng số liệu: một cấu hình xử lý được bao nhiêu dữ liệu trước khi vi
phạm SLO, bottleneck nằm ở đâu và scaling option nào đáng tiền.

### Workload matrix mẫu

| Scenario | Data size | Change rate | Mục tiêu |
| --- | ---: | ---: | --- |
| Small | 100 nghìn rows | 1% | CI/local feedback |
| Medium | 1–5 triệu rows | 5% | baseline integration |
| Large | 10–50 triệu rows | 10% | tìm bottleneck |
| Skew | như Medium | một key/category chiếm 40% | kiểm tra shuffle skew |

Không cần bắt đầu bằng 50 triệu row nếu laptop không đủ. Quan trọng là workload
reproducible và so sánh cùng điều kiện.

### Thực hành

1. Ghim Git commit, image versions và resource limits.
2. Tạo data bằng seed cố định; ghi row count và distribution.
3. Đo riêng:
   - PostgreSQL incremental query time và `EXPLAIN (ANALYZE, BUFFERS)`;
   - Airflow task duration/queue time;
   - dbt model duration;
   - Spark input rows, shuffle read/write, spill và skew;
   - Iceberg data/delete file count và average file size;
   - Kafka publish throughput, consumer lag và catch-up time.
4. Thay đúng một biến mỗi thí nghiệm: index, partition count, fetch size,
   worker cores hoặc file target size.
5. Chạy warm-up và ít nhất ba lần đo; dùng median/P95 phù hợp.
6. Ghi trade-off, ví dụ tăng partition cải thiện throughput nhưng tăng file và
   scheduling overhead.

### Bằng chứng pass

- có baseline và regression threshold;
- bottleneck được chứng minh bằng metric/query plan, không đoán;
- xác định capacity envelope trước khi vi phạm SLO;
- đề xuất scaling có expected gain, cost và rollback;
- raw data lớn không bị commit vào Git.

### Câu hỏi phỏng vấn tự trả lời

- Tại sao tăng Spark executor memory chưa chắc làm job nhanh hơn?
- Khi nào Kafka cần thêm partition và hậu quả là gì?
- Small files ảnh hưởng Iceberg read/metadata thế nào?
- Index `updated_at` cải thiện query nhưng làm source write tốn gì?

## 7. Lab 4 — Least privilege, secret rotation và negative test

Liên quan backlog A8.

### Mục tiêu

Giảm blast radius và tạo executable proof rằng mỗi service không làm được việc
ngoài nhiệm vụ.

### Role matrix mẫu

| Principal | Cần được phép | Phải bị từ chối |
| --- | --- | --- |
| Airflow batch | đọc source batch, upsert staging/watermark | tạo superuser, đọc secret table |
| dbt runtime | đọc source/staging, ghi target schema | sửa OLTP source |
| CDC role | logical replication trên allow-listed publication | đọc table ngoài publication tùy tiện |
| CDC materializer | insert Bronze, upsert Silver | sửa source OLTP |
| monitoring | `SELECT` metric state | update watermark hoặc business data |

### Thực hành

1. Inventory credential đang được từng container dùng.
2. Tách owner/migration role khỏi steady-state runtime role.
3. Grant đúng schema/table/function cần thiết.
4. Viết positive test cho thao tác hợp lệ.
5. Viết negative test yêu cầu thao tác ngoài quyền phải fail.
6. Rotate một credential trong lab và đo downtime/cách rollout.
7. Quét Git history/current tree để ngăn secret commit.
8. Sau database RBAC mới mở rộng TLS, Kafka SASL/ACL và network policy.

### Bằng chứng pass

- allow/deny matrix tự động kiểm tra được;
- exporter không thể update watermark;
- materializer không thể mutate OLTP source;
- secret mới hoạt động, secret cũ bị revoke;
- log/test output không in credential.

### Mẹo Senior

“Có password” không đồng nghĩa least privilege. Hãy luôn hỏi credential bị lộ
thì attacker đọc/ghi/xóa được dataset nào và trong bao lâu.

## 8. Lab 5 — CDC outage, WAL growth và controlled recovery

Liên quan A6, A7, A13 và A19.

### Mục tiêu

Thực hành incident mà CDC dừng nhưng PostgreSQL vẫn nhận write. Hiểu vì sao
replication slot vừa bảo vệ dữ liệu vừa có thể làm đầy disk.

### Thực hành

1. Ghi baseline connector state, source/Silver count, slot LSN và retained bytes.
2. Dừng Debezium Connect trong một khoảng có giới hạn.
3. Tạo controlled customer insert/update/delete bằng dữ liệu giả.
4. Quan sát WAL retained bytes và alert state.
5. Khởi động Connect; đo catch-up duration và consumer lag.
6. Chạy CDC materialization test và full reconciliation.
7. Lặp lại tình huống trong đó offset/slot không còn khớp trên environment thử
   nghiệm; thiết kế controlled resnapshot thay vì xóa slot tùy tiện.

### Bằng chứng pass

- connector catch-up không mất create/update/delete;
- Bronze coordinates không duplicate;
- Silver/source reconciliation bằng 0;
- alert phát hiện retained WAL tăng;
- runbook có decision tree: restart, scale catch-up hay controlled resnapshot.

### Tuyệt đối tránh

Không drop replication slot trên environment có dữ liệu cần giữ chỉ để giải
phóng disk. Slot mất có thể làm mất khả năng đọc đoạn WAL chưa apply.

## 9. Lab 6 — Schema migration không phá consumer

Liên quan A10, A15 và A19.

### Mục tiêu

Thực hành thay schema qua producer, Registry, Bronze/Silver, dbt mart và
consumer mà không dùng “đổi tất cả cùng lúc”.

### Scenario mẫu

Thêm field nullable `customer_segment`:

1. Viết contract mới tương thích backward.
2. Chạy compatibility check trước registration/deploy.
3. Consumer phải đọc được record cũ không có field.
4. Producer bắt đầu phát field mới.
5. Silver thêm column nullable bằng migration.
6. dbt staging/mart thêm column và contract/test.
7. Consumer chuyển sang dùng field sau khi coverage đủ.
8. Chỉ deprecate fallback sau cửa sổ đã công bố.

Sau đó tạo negative exercise: đổi một field từ string sang incompatible type và
chứng minh CI/Registry chặn trước deploy.

### Artifact bắt buộc

- ADR giải thích compatibility policy;
- producer/consumer version matrix;
- migration order và rollback;
- impact analysis: dataset/model/consumer bị ảnh hưởng;
- test đọc cả old và new records.

### Bằng chứng pass

- consumer mới đọc được dữ liệu cũ;
- consumer cũ không hỏng trong compatibility promise;
- breaking change bị từ chối;
- rollback không cần xóa topic/history;
- dbt public contract và docs phản ánh schema mới.

## 10. Lab 7 — Concurrent runs, fencing và progress ordering

Liên quan backlog A20.

### Mục tiêu

Hiểu vì sao `max_active_runs=1` là guardrail hợp lệ ở P0 nhưng chưa phải thiết kế
cho nhiều scheduler/writer.

### Thực hành thiết kế trước khi code

Giả lập hai run:

```text
Run A: window 10:00 → 11:00, xử lý chậm
Run B: window 11:00 → 12:00, xử lý nhanh
```

Nếu B promote watermark `12:00` trước khi A hoàn thành, phần A có thể bị đánh dấu
an toàn sai. Hãy thiết kế một trong các hướng:

- batch/run identity + ordered state machine;
- compare-and-set trên previous committed version;
- database advisory lock;
- lease/fencing token có expiry và ownership;
- partition progress độc lập nếu workload thật sự tách được.

### Test matrix

| Case | Kỳ vọng |
| --- | --- |
| A và B cùng start | Chỉ run hợp lệ sở hữu progress transition |
| A chết trước sink commit | Candidate có thể retry, committed không nhảy |
| A commit nhưng response timeout | Retry nhận diện state đã commit |
| Lease hết hạn, owner cũ quay lại | Fencing token cũ bị từ chối |
| B hoàn thành trước A | Không promote vượt window chưa an toàn |

### Bằng chứng pass

- không skip window;
- không có hai owner cùng hợp lệ;
- sink grain không duplicate;
- retry sau ambiguous timeout hội tụ;
- runbook giải thích lock/lease recovery, không yêu cầu sửa DB bằng tay mơ hồ.

## 11. Lab 8 — Cloud/IaC vertical slice

Liên quan backlog A17. Chỉ làm sau khi đã hiểu DR và security boundary.

### Mục tiêu

Đưa một vertical slice nhỏ lên một cloud bằng code, không bê nguyên 35 Compose
services lên Kubernetes.

### Chọn một cloud, không học ba cloud cùng lúc

Ví dụ AWS:

```text
Terraform
  ├─ VPC/private subnet/security groups
  ├─ RDS PostgreSQL
  ├─ S3 warehouse
  ├─ secret manager/IAM roles
  ├─ container registry/runtime
  └─ monitoring/log destination
```

Kafka/Spark/Airflow có thể dùng managed service hoặc giữ một phần local tùy mục
tiêu lab. Mỗi lựa chọn phải có lý do về vận hành, chi phí và lock-in.

### Thực hành

1. Viết requirement và monthly cost ceiling.
2. Tách dev environment/account/project.
3. Terraform plan/apply/destroy phải tái chạy được.
4. Dùng IAM identity/role, không hard-code access key.
5. Build một immutable image và promote cùng digest.
6. Chạy một pipeline nhỏ end-to-end.
7. Test migration/rollback và secret rotation.
8. Chạy restore hoặc recreate vào environment thứ hai.
9. Ghi cost thực tế và teardown để không phát sinh hóa đơn ngoài ý muốn.

### Bằng chứng pass

- environment tạo lại được từ code;
- network/identity/secret không cần click bắt buộc;
- cùng artifact được promote;
- destroy không xóa nhầm shared state;
- cost và production delta được ghi trung thực.

## 12. Lab 9 — Design review, ADR và mentoring

Đây là phần repository kỹ thuật thường bỏ quên nhưng phân biệt Mid với Senior.

### Thực hành

Chọn một quyết định, ví dụ “mở CDC cho orders hay tiếp tục polling”, rồi viết ADR:

```text
Title
Status
Context và business requirement
Constraints
Options considered
Decision
Consequences tích cực/tiêu cực
Migration và rollback
Metrics để review lại quyết định
```

Nhờ một người khác review. Không chỉ hỏi “code đúng không”; yêu cầu họ chỉ ra
assumption, failure mode, operational burden và option bị bỏ sót. Sau đó sửa ADR
và ghi rõ phản hồi nào được nhận hoặc từ chối, vì sao.

Tiếp theo, hướng dẫn người khác chạy một failure drill mà không tự cầm bàn phím.
Nếu runbook khiến họ mắc kẹt, đó là lỗi operability cần sửa.

### Bằng chứng pass

- quyết định có ít nhất hai option thật sự;
- trade-off gắn với requirement/SLO;
- reviewer có thể phản biện từ tài liệu;
- một operator khác hoàn thành runbook;
- decision có review date thay vì trở thành chân lý vĩnh viễn.

## 13. Thứ tự triển khai khuyến nghị

Không cần làm tất cả trước khi ứng tuyển. Thứ tự có tỷ lệ học/chi phí tốt:

```text
Lab 0: baseline proof
  ↓
Lab 1: SLO + incident
  ↓
Lab 2: restore drill
  ↓
Lab 3: capacity benchmark
  ↓
Lab 4: least privilege
  ↓
Lab 5 hoặc 6: CDC incident / schema migration
  ↓
Lab 7: concurrency design
  ↓
Lab 8: cloud/IaC
  ↓
Lab 9: ADR/review/mentoring
```

Ba lab đầu đã tạo câu chuyện Mid/Senior tốt hơn việc thêm một engine mới.

## 14. Cách kể trong phỏng vấn

Dùng cấu trúc ngắn:

```text
Context      hệ thống và constraint là gì?
Problem      failure/business risk cụ thể là gì?
Decision     chọn thiết kế nào, bỏ lựa chọn nào?
Execution    triển khai/migrate/rollback ra sao?
Evidence     metric/test nào chứng minh?
Learning     giới hạn và lần sau thay đổi gì?
```

Ví dụ không nên nói:

> Tôi dùng Kafka, Spark, Airflow, dbt, Iceberg, Debezium và Grafana.

Nên nói:

> Pipeline dùng at-least-once delivery. Tôi đặt stable identity ở từng sink và
> giữ committed/candidate watermark tách biệt, nên failure sau staging commit
> nhưng trước Iceberg/Kafka completion có thể retry mà không skip window hoặc
> nhân business grain. Tôi kiểm chứng bằng replay và failure-path test.

## 15. Senior readiness checklist

Bạn đang tiến gần Senior khi có thể trả lời bằng evidence, không chỉ lý thuyết:

- [ ] Một SLO cụ thể và error-budget calculation.
- [ ] Một firing/resolved external notification test.
- [ ] Một isolated restore drill có RPO/RTO.
- [ ] Một capacity envelope và bottleneck được đo.
- [ ] Một allow/deny security matrix có negative tests.
- [ ] Một CDC outage/resnapshot decision tree.
- [ ] Một backward-compatible schema rollout và breaking negative test.
- [ ] Một concurrency/fencing design có crash matrix.
- [ ] Một cloud/IaC vertical slice có cost/teardown.
- [ ] Một ADR được người khác review.
- [ ] Một runbook được người khác sử dụng thành công.
- [ ] Một postmortem thể hiện root cause và corrective actions.

Checklist này không thay thế kinh nghiệm production thật. Nó giúp project tạo
evidence gần với công việc Senior hơn và giúp bạn nhận ra phần nào mình chỉ mới
“đọc qua” so với phần đã thực hành, đo lường và vận hành.

## 16. Tài liệu liên quan

- [`16-self-study-keywords.md`](16-self-study-keywords.md): keyword cần tự học.
- [`../current-state.md`](../current-state.md): baseline P0 thực sự đã có.
- [`../backlog.md`](../backlog.md): A6–A20 và Definition of Done.
- [`../architecture.md`](../architecture.md): state owner và failure domain.
- [`../runbook.md`](../runbook.md): command vận hành hiện tại.
- [`11-verification-and-tradeoffs.md`](11-verification-and-tradeoffs.md): test pyramid và production delta.
- [`14-cdc-bronze-silver-cutover.md`](14-cdc-bronze-silver-cutover.md): migration/cutover case study.
