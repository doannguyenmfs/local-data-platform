# Bản đồ nhập môn — từ dbt incremental đến CDC

Tài liệu này dành cho người đã hiểu SQL/Airflow cơ bản nhưng chưa từng vận hành
dbt incremental, Iceberg, Kafka, monitoring hoặc CDC. Đừng bắt đầu bằng việc
nhớ option của từng tool. Hãy luôn trả lời bốn câu cho mỗi bước:

1. **Input nào được chọn?** Toàn bộ dữ liệu hay chỉ phần thay đổi?
2. **State nằm ở đâu?** Watermark, table, snapshot, offset hay checkpoint?
3. **Nếu chạy lại thì sao?** Append thêm, merge vào key cũ hay no-op?
4. **Success nghĩa là gì?** Process chạy, dữ liệu đã durable hay consumer đã apply?

Sơ đồ dưới đây chỉ mô tả baseline P0 hiện hữu. Đối chiếu
[`../current-state.md`](../current-state.md) khi cần kiểm tra trạng thái; những
nâng cấp production chưa có được tách sang [`../backlog.md`](../backlog.md).

## 1. Bức tranh tổng thể bằng ngôn ngữ đơn giản

```text
OLTP public.*
    │
    ├─ Airflow đọc cửa sổ updated_at của 4 bảng batch
    │      └─ UPSERT vào staging.*
    │
    ├─ dbt đọc staging batch + CDC Silver customer
    │      ├─ view chuẩn hóa
    │      ├─ snapshot lưu lịch sử customer
    │      └─ MERGE fact/daily mart
    │
    ├─ Spark đọc staging
    │      └─ MERGE fact vào Iceberg/MinIO
    │
    ├─ producer đọc candidate batch
    │      └─ Kafka ─► Spark stream ─► Iceberg history/current/DLQ
    │
    └─ PostgreSQL WAL
           └─ Debezium/Avro ─► Bronze ─► Silver customer current/delete

mọi component ─► platform exporter ─► Prometheus ─► Grafana/Alertmanager
```

Hai đường cuối dễ bị nhầm:

- pipeline Kafka `order-events` là event do project chủ động tạo từ staging;
- topic `ecommerce_cdc_avro.public.customers` là thay đổi database do Debezium đọc
  trực tiếp từ WAL.

## 2. `capture`, `store` và `apply` không phải một việc

Đây là phân biệt quan trọng nhất khi đọc phần CDC.

| Giai đoạn | Nghĩa | Customer delete hiện đã tới đâu? |
| --- | --- | --- |
| Capture | Nhìn thấy thay đổi ở nguồn | Có: Debezium đọc delete từ WAL |
| Store | Ghi thay đổi bền vững để có thể replay | Có: event `d` và tombstone ở Kafka |
| Apply/materialize | Dùng event sửa/xóa state ở đích | Có: Bronze/Silver consumer apply theo LSN/offset |
| Cut over | Chuyển ownership từ pipeline cũ sang pipeline mới | Có: dbt đọc Silver; Airflow customer poller đã retire |

Vì vậy câu đúng hiện tại là: **source delete được capture, lưu ở Bronze, apply
thành Silver tombstone và đóng current `dim_customer` ở lần dbt build kế tiếp.**

Không cho Debezium ghi thẳng vào staging là quyết định có chủ đích. Nếu Airflow
backfill và CDC cùng là writer, backfill có thể insert lại row vừa bị CDC xóa.
Project đã tạo namespace Bronze/Silver riêng, đối soát rồi cutover ownership;
legacy `staging.customers` được giữ làm rollback evidence nhưng không còn writer.

## 3. Sáu loại state đang tồn tại

| State | Nằm ở đâu | Trả lời câu hỏi gì? | Ai cập nhật? |
| --- | --- | --- | --- |
| Watermark | `metadata.etl_watermark` | source batch đã commit tới thời gian nào? | Airflow task cuối |
| dbt target table | PostgreSQL schema `dbt_*` | mart đã materialize tới `source_loaded_at` nào? | dbt MERGE |
| dbt snapshot | `dbt_*_snapshots` | customer có những version lịch sử nào? | dbt snapshot |
| Iceberg snapshot | catalog + MinIO | tập data files nào tạo thành table ở một commit? | Spark/Iceberg |
| Kafka offset/checkpoint | Kafka + Spark checkpoint | consumer đã xử lý tới record nào? | Kafka Connect/Spark |
| CDC replication slot | PostgreSQL | WAL nào chưa được connector xác nhận? | PostgreSQL + Debezium |

Chúng không thay thế nhau. Ví dụ dbt snapshot là lịch sử business dimension,
còn Iceberg snapshot là commit metadata của table; cùng chữ “snapshot” nhưng
giải quyết hai vấn đề khác nhau.

## 4. Một update đi qua hệ thống như thế nào?

Giả sử payment của một order cũ được cập nhật hôm nay:

1. Source đổi `payments.updated_at`.
2. Airflow chụp upper bound và đọc cửa sổ đã đóng.
3. Staging UPSERT payment theo primary key; chạy lại không nhân đôi row.
4. Candidate watermark được ghi nhưng committed watermark chưa đổi.
5. dbt tạo `source_loaded_at` mới cho order item liên quan.
6. `fact_sales` MERGE theo `order_item_id`.
7. `daily_sales` tìm ngày bị ảnh hưởng và tính lại trọn ngày đó.
8. Spark/Iceberg MERGE theo cùng business grain.
9. Kafka producer tạo event ID ổn định; retry vẫn có cùng identity.
10. Chỉ khi mọi nhánh success, Airflow mới commit watermark.

Nếu lỗi ở bước 8, dbt có thể đã commit. Lần retry không rollback dbt; nó chạy
lại và hội tụ nhờ MERGE/idempotency. Đây là **retry-forward**, không phải một
distributed transaction bao trùm PostgreSQL, Kafka và object storage.

## 5. Một delete đi qua hệ thống như thế nào?

Với `public.customers`, đường owner hiện tại là:

```text
DELETE source customer
    └─ WAL/Debezium: d + tombstone ─► Bronze ─► Silver is_deleted=true
                                             └─ dbt snapshot invalidate
```

- Event `d` mang nghĩa nghiệp vụ: row đã bị xóa.
- Tombstone có cùng Kafka key và value `null`; nó phục vụ log compaction.
- Event `d`, không phải Kafka tombstone, đặt Silver `is_deleted=true`.
- `customers_current` ẩn key; dbt snapshot `hard_deletes: invalidate` đóng
  version mở mà không xóa lịch sử.

## 6. `incremental` có ba nghĩa khác nhau trong project

| Nơi | Candidate được chọn bằng | Ghi đích bằng |
| --- | --- | --- |
| Airflow extract | source `updated_at` và committed watermark | PostgreSQL UPSERT |
| dbt mart | `source_loaded_at` lớn hơn high-water của target | PostgreSQL MERGE |
| Spark Iceberg fact | `source_loaded_at` lớn hơn max đã commit | Iceberg MERGE |

Điểm chung là chỉ xử lý delta. Điểm khác là mỗi layer có state và transaction
boundary riêng. Không nên gọi tất cả là “watermark” rồi giả định chúng commit
cùng lúc.

## 7. Monitoring đang chứng minh điều gì?

```text
collector đọc state thật
    └─ metric số hóa state
         └─ Prometheus lưu và đánh giá rule
              └─ pending qua thời gian `for`
                   └─ firing gửi tới Alertmanager
```

- Health: PostgreSQL/Kafka/Airflow/MinIO/Spark gateway/Debezium có hoạt động đúng
  semantic check không?
- Freshness: committed watermark cũ bao lâu?
- Stuck state: candidate đã chờ downstream quá lâu chưa?
- CDC safety: connector/task có RUNNING, slot có active và giữ quá nhiều WAL?

Grafana chỉ hiển thị. Alertmanager local chỉ group/hiển thị alert, chưa gửi
Slack/email/PagerDuty. Do đó project **đã có monitor và alert rule**, nhưng chưa
có production notification/on-call integration.

## 8. Thứ tự đọc code ít gây ngợp nhất

1. `dbt/models/marts/fact_sales.sql`: đọc grain, candidate filter và MERGE key.
2. `dbt/models/marts/daily_sales.sql`: đọc `affected_dates` trước, rồi mới đọc
   aggregate.
3. `airflow/dags/ecommerce_pipeline.py`: tìm candidate watermark và task cuối
   `advance_watermarks`.
4. `spark/jobs/iceberg_sales.py`: so sánh append đầu tiên, MERGE và no-op.
5. `kafka/producer/publish_orders.py`: tìm deterministic `event_id` và broker
   acknowledgement.
6. `spark/jobs/order_stream.py`: phân biệt event history, current state và DLQ.
7. `monitoring/platform_exporter.py`: mỗi collector biến state nào thành metric?
8. `cdc/bootstrap.py`, rồi `cdc/verify_cdc.py`: publication/slot trước, smoke
   test sau.

Sau mỗi file, tự trả lời lại bốn câu ở đầu tài liệu. Nếu chưa trả lời được, chưa
cần học thêm option; hãy quay lại xác định state và retry boundary.

## 9. Từ điển cực ngắn

| Thuật ngữ | Hiểu đơn giản |
| --- | --- |
| Grain | một row đại diện chính xác cho đối tượng gì |
| Idempotent | chạy lại cùng input vẫn hội tụ về cùng kết quả |
| Watermark | mốc source đã được batch hoàn tất an toàn |
| Candidate watermark | upper bound đang xử lý, chưa được quyền commit |
| High-water mark | giá trị lớn nhất đã materialize ở một sink cụ thể |
| Snapshot SCD2 | lịch sử các version business của dimension |
| Iceberg snapshot | commit metadata trỏ tới tập file của table |
| Offset | vị trí record trong Kafka partition |
| Checkpoint | state giúp stream tiếp tục sau restart |
| LSN | vị trí trong PostgreSQL WAL |
| Replication slot | lời hứa giữ WAL chưa được subscriber xử lý |
| Tombstone | Kafka record value `null` để compaction quên key cũ |
| DLQ | nơi giữ record lỗi thay vì làm chết toàn stream |
