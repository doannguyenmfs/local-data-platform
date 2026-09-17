# Level 10 — Monitoring và alerting

## Monitoring khác logging thế nào?

Log mô tả từng event chi tiết; metric là số đo theo thời gian để nhìn xu hướng
và tạo alert. Một task “success” không chứng minh dữ liệu còn tươi: DAG có thể
không được schedule, watermark đứng yên nhưng không có exception mới.

Project theo dõi ba lớp:

- Infrastructure reachability: PostgreSQL, Kafka, Spark gateway, MinIO, Airflow.
- Data/pipeline semantics: watermark lag, candidate bị treo, staging row counts,
  required Kafka topic/partition.
- CDC safety: connector và task có thực sự `RUNNING`, replication slot có active,
  và slot đang giữ lại bao nhiêu WAL.

## Kiến trúc

```text
platform-exporter ──► Prometheus ──► Grafana
       │                    │
       │                    └──────► Alertmanager
       ├── PostgreSQL metadata/staging
       ├── Kafka Admin API
       └── HTTP health endpoints
```

Exporter giữ process sống khi dependency lỗi và xuất `component_up=0`; nếu nó
crash theo dependency, Prometheus chỉ biết scrape target mất mà không còn context
subsystem nào hỏng.

Exporter là adapter project-specific vì generic container metrics không biết
watermark/candidate hay required Kafka topic. Nó thu thập theo chu kỳ 15 giây ở
background thread; HTTP `/metrics` chỉ expose giá trị đã collect, tránh mỗi
Prometheus scrape đồng thời tạo query/network storm tới mọi dependency.

Mỗi collector tự bắt exception và đặt metric subsystem tương ứng về 0. Một Kafka
timeout không được làm mất PostgreSQL freshness metrics, và process không được
crash chỉ vì dependency nó đang quan sát bị down.

## Metrics

| Metric | Ý nghĩa |
| --- | --- |
| `platform_component_up` | Semantic health theo component |
| `pipeline_watermark_lag_seconds` | Độ cũ của committed source watermark |
| `pipeline_candidate_pending` | Batch đã extract nhưng chưa commit hết downstream |
| `staging_table_rows` | Guardrail volume cơ bản |
| `kafka_topic_partitions` | Required topic tồn tại và có partition |
| `cdc_connector_up` | Connector và toàn bộ Debezium task đều `RUNNING` |
| `cdc_replication_slot_active` | PostgreSQL logical slot đang được connector sử dụng |
| `cdc_replication_slot_retained_bytes` | WAL bytes PostgreSQL chưa được phép dọn vì slot |
| `platform_exporter_collection_errors` | Exporter không thu được subsystem metric |

High-cardinality label như `order_id`, `customer_id`, `event_id` tuyệt đối không
đưa vào metric. Chúng làm số time series tăng không giới hạn; dùng log/table để
điều tra từng record.

`pipeline_watermark_lag_seconds` dùng committed watermark, không dùng candidate.
Candidate chỉ là batch chưa hoàn tất; dùng nó làm freshness sẽ báo dữ liệu mới
trước khi consumers thực sự có thể tin batch đã commit.

`staging_table_rows` là smoke signal, không phải expected count assertion. Row
count tăng/giảm theo business; chỉ trạng thái rỗng kéo dài được cảnh báo trong
lab. Data correctness chi tiết thuộc dbt/Spark test.

## Alerts

- Component down quá 2 phút: critical.
- Watermark cũ hơn 48 giờ: warning.
- Candidate giữ quá 1 giờ: critical; downstream có thể đang lỗi trước commit.
- Required Kafka topic không có partition: critical.
- Debezium connector/task không RUNNING 5 phút: critical.
- CDC slot inactive 10 phút: warning.
- CDC slot giữ trên 512 MiB WAL trong 10 phút: warning, trước safety cap 1 GiB.
- Staging table rỗng 10 phút: warning.

`for` duration chống alert flapping do restart ngắn. Threshold phải dựa trên SLA;
48 giờ phù hợp DAG daily demo nhưng không phù hợp pipeline realtime.

Alert lifecycle:

```text
Prometheus expression true
    -> pending trong khoảng `for`
    -> firing
    -> Alertmanager group/deduplicate/route
    -> receiver hoặc UI
```

Nếu expression trở lại false trước `for`, alert không firing. Đây là debounce,
không phải retry của pipeline.

Alertmanager local chỉ group và hiển thị alert trên UI, không gửi ra bên ngoài vì
repository không chứa Slack/email credential. Production thêm receiver bằng
secret manager; không commit webhook token.

## Vậy project đã có monitoring/alert chưa?

**Có**, nhưng cần phân biệt bốn tầng:

1. Exporter đã đọc state thật và xuất metric.
2. Prometheus đã scrape metric và có tám alert rules.
3. Grafana đã có dashboard được provision từ Git.
4. Alertmanager đã nhận/group alert, nhưng receiver hiện chỉ là `local-ui`.

Tầng 1–3 và logic tầng 4 đã có. Phần chưa production là notification tới
Slack/email/PagerDuty, on-call ownership, escalation policy, SLO/error budget và
monitoring nằm ngoài cùng failure domain với workload.

## Chạy

Thêm profile monitoring khi khởi động full stack:

```bash
docker compose \
  --profile spark \
  --profile lakehouse \
  --profile streaming \
  --profile monitoring \
  up -d --build
```

- Exporter metrics: <http://localhost:9100/metrics>
- Prometheus: <http://localhost:9090>
- Alertmanager: <http://localhost:9093>
- Grafana: <http://localhost:3000>

Grafana tự provision Prometheus datasource và dashboard **Local Data Platform
Overview**. Credentials lấy từ `.env`.

Provisioning từ Git tránh dashboard chỉ tồn tại trong Grafana volume và không ai
biết ai đã click sửa gì. Grafana volume vẫn giữ local state/user, nhưng datasource
và dashboard contract có thể tái tạo từ repository.

## Lưu ý production

- Monitoring phải ở failure domain khác platform; local Compose chỉ minh họa.
- Alert phải có owner, severity, runbook link và hành động cụ thể.
- Volume metric không thay data quality test; dbt tests vẫn là assertion cấp row.
- “Up” không đồng nghĩa “correct”; vì vậy component health và semantic data
  metrics phải đi cùng nhau.
- Theo dõi alert noise. Alert không dẫn tới hành động nên bị xóa hoặc đổi thành
  dashboard signal.

## Vì sao monitoring stack cũng chỉ một node?

Mục tiêu local là học metric/rule/dashboard wiring. Prometheus, Alertmanager và
Grafana cùng Docker host nên nếu host chết, monitoring cũng biến mất—một failure
domain không chấp nhận được ở production. Triển khai thật cần replica/remote
write và monitoring nằm ngoài workload được quan sát.

## File cần đọc

- `monitoring/platform_exporter.py`: collector và metric semantics.
- `monitoring/prometheus/prometheus.yml`: scrape/rule/Alertmanager wiring.
- `monitoring/prometheus/alerts.yml`: expression, threshold, duration, severity.
- `monitoring/alertmanager/alertmanager.yml`: grouping/routing local.
- `monitoring/grafana/provisioning/`: declarative datasource/dashboard loading.
- `docs/runbook.md`: hành động operator khi từng alert xảy ra.

## Điều kiện hoàn thành

- Prometheus target `local-data-platform` là UP.
- Dashboard tự xuất hiện, không cấu hình tay.
- Dừng Kafka tạo `PlatformComponentDown` sau 2 phút.
- Candidate watermark giả lập treo tạo alert sau 1 giờ.
- Alert biến mất sau khi dependency phục hồi.

## Tài liệu chính thức

- [Prometheus releases](https://prometheus.io/download/)
- [Prometheus Alertmanager](https://prometheus.io/docs/alerting/latest/alertmanager/)
- [Grafana Docker installation](https://grafana.com/docs/grafana/latest/setup-grafana/installation/docker/)
