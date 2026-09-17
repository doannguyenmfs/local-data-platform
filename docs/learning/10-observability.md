# Level 10 — Monitoring và alerting

## Monitoring khác logging thế nào?

Log mô tả từng event chi tiết; metric là số đo theo thời gian để nhìn xu hướng
và tạo alert. Một task “success” không chứng minh dữ liệu còn tươi: DAG có thể
không được schedule, watermark đứng yên nhưng không có exception mới.

Project theo dõi hai lớp:

- Infrastructure reachability: PostgreSQL, Kafka, Spark gateway, MinIO, Airflow.
- Data/pipeline semantics: watermark lag, candidate bị treo, staging row counts,
  required Kafka topic/partition.

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

## Metrics

| Metric | Ý nghĩa |
| --- | --- |
| `platform_component_up` | Semantic health theo component |
| `pipeline_watermark_lag_seconds` | Độ cũ của committed source watermark |
| `pipeline_candidate_pending` | Batch đã extract nhưng chưa commit hết downstream |
| `staging_table_rows` | Guardrail volume cơ bản |
| `kafka_topic_partitions` | Required topic tồn tại và có partition |
| `platform_exporter_collection_errors` | Exporter không thu được subsystem metric |

High-cardinality label như `order_id`, `customer_id`, `event_id` tuyệt đối không
đưa vào metric. Chúng làm số time series tăng không giới hạn; dùng log/table để
điều tra từng record.

## Alerts

- Component down quá 2 phút: critical.
- Watermark cũ hơn 48 giờ: warning.
- Candidate giữ quá 1 giờ: critical; downstream có thể đang lỗi trước commit.
- Required Kafka topic không có partition: critical.
- Staging table rỗng 10 phút: warning.

`for` duration chống alert flapping do restart ngắn. Threshold phải dựa trên SLA;
48 giờ phù hợp DAG daily demo nhưng không phù hợp pipeline realtime.

Alertmanager local chỉ group và hiển thị alert trên UI, không gửi ra bên ngoài vì
repository không chứa Slack/email credential. Production thêm receiver bằng
secret manager; không commit webhook token.

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

## Lưu ý production

- Monitoring phải ở failure domain khác platform; local Compose chỉ minh họa.
- Alert phải có owner, severity, runbook link và hành động cụ thể.
- Volume metric không thay data quality test; dbt tests vẫn là assertion cấp row.
- “Up” không đồng nghĩa “correct”; vì vậy component health và semantic data
  metrics phải đi cùng nhau.
- Theo dõi alert noise. Alert không dẫn tới hành động nên bị xóa hoặc đổi thành
  dashboard signal.

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
