# Lộ trình đọc tài liệu học

Thư mục này được viết để có thể học lại project mà không cần nhớ lịch sử hội
thoại. Mỗi bài đi theo cùng một trình tự:

1. khái niệm là gì;
2. tại sao cần nó;
3. cơ chế hoạt động;
4. project áp dụng ở file nào và vì sao thiết kế như vậy;
5. cách kiểm tra kết quả;
6. trade-off, lỗi thường gặp và câu hỏi phỏng vấn.

Nếu bắt đầu mất dấu từ phần dbt incremental, hãy đọc
[Bản đồ nhập môn từ dbt đến CDC](00-beginner-map.md) trước. Bài này nối các tool
bằng bốn câu hỏi về input, state, retry và ý nghĩa của success; đồng thời phân
biệt rõ delete mới được **capture** hay đã được **apply**.

Nếu cần từ khóa để tự tìm khóa học/video/tài liệu bên ngoài, dùng
[Từ khóa tự học Data Engineering theo project](16-self-study-keywords.md). Tài
liệu đó ưu tiên Airflow và Kafka, cung cấp cụm tìm kiếm tiếng Anh và thứ tự học.

Nếu mục tiêu là tiến từ portfolio P0 lên năng lực Mid/Senior, đọc
[Lộ trình Senior Data Engineer và các lab thực hành](17-senior-data-engineer-roadmap.md).
Tài liệu này hướng dẫn tạo SLO/incident, restore drill, capacity benchmark,
security proof, CDC recovery, schema migration, concurrency và cloud/IaC evidence.

Các bài Level 1–15 giải thích baseline P0 đã có trong repository. Đọc
[Trạng thái P0](../current-state.md) trước nếu cần đối chiếu “đã làm”, và đọc
[Backlog](../backlog.md) cho A6–A20 “chưa làm”. Việc một khái niệm xuất hiện
trong backlog không có nghĩa container/config tương ứng đang tồn tại.

## Đường học đầy đủ

| Level | Bài | Sau bài này cần trả lời được |
| ---: | --- | --- |
| 1 | [Nền tảng](01-platform-foundations.md) | image/container/volume và host/container network khác gì? |
| 2 | [Airflow](02-airflow-orchestration.md) | task dependency, retry owner và commit barrier nằm đâu? |
| 3 | [Warehouse/SCD2](03-warehouse-modeling.md) | grain, fact/dimension và temporal join hoạt động thế nào? |
| 4 | [Incremental](04-incremental-backfill.md) | watermark/candidate/idempotency/backfill khác nhau ra sao? |
| 5 | [dbt](05-dbt-incremental.md) | layer, materialization, test và incremental mart vì sao được chọn? |
| 6 | [Spark](06-spark.md) | driver/executor/shuffle/partition và JDBC batch vận hành ra sao? |
| 7 | [Iceberg](07-iceberg.md) | table format, snapshot, catalog, object store và maintenance là gì? |
| 8 | [Kafka](08-kafka.md) | partition/offset/key/checkpoint/replay/DLQ phối hợp ra sao? |
| 9 | [Integration](09-production-integration.md) | nhiều sink commit/retry mà không có distributed transaction thế nào? |
| 10 | [Observability](10-observability.md) | health, freshness, metric, alert và data test khác nhau thế nào? |
| 11 | [Verification](11-verification-and-tradeoffs.md) | bằng chứng nào đủ để tin một platform end-to-end? |
| 12 | [CDC/Debezium](12-cdc-debezium.md) | WAL, LSN, publication, slot, snapshot và delete event phối hợp thế nào? |
| 13 | [Schema Registry/Avro](13-schema-registry-avro.md) | schema ID, subject, reference và compatibility gate bảo vệ consumer ra sao? |
| 14 | [CDC Bronze/Silver/cutover](14-cdc-bronze-silver-cutover.md) | capture thành current state/delete và đổi owner không dual-writer thế nào? |
| 15 | [dbt contracts/slim CI](15-dbt-contracts-slim-ci.md) | contract khác test và manifest state/defer rút ngắn PR ra sao? |

[Từ khóa tự học](16-self-study-keywords.md) là phụ lục tra cứu, không phải Level
16 bắt buộc và không đại diện cho capability mới đã triển khai.
[Lộ trình Senior](17-senior-data-engineer-roadmap.md) cũng là phụ lục thực hành;
các lab trong đó vẫn là backlog cho tới khi có code và executable proof.

`00-beginner-map.md` không phải một level mới. Nó là bản đồ để tra cứu trong lúc
đọc Level 5–15.

Đọc [Kiến trúc hệ thống](../architecture.md) sau Level 4 để gắn khái niệm vào
container/node/network/volume. Giữ [Bản đồ code](../code-map.md) mở bên cạnh IDE
khi đọc implementation.

## Đường học nhanh theo mục tiêu

### Chuẩn bị phỏng vấn Analytics Engineer

Đọc Level 3 → 5 → 11 → 15. Tập trung grain, SCD2, dbt layering,
generic/singular tests, contracts, incremental MERGE và idempotency.

### Chuẩn bị phỏng vấn Data Engineer batch

Đọc Level 1 → 2 → 4 → 6 → 7 → 9. Tập trung orchestration boundary, watermark,
Spark execution, Iceberg metadata/transaction và failure recovery.

### Chuẩn bị phỏng vấn Streaming

Đọc Level 4 → 7 → 8 → 9 → 10 → 12 → 13 → 14. Tập trung stable event
identity, Kafka ordering, schema compatibility, WAL/LSN, replay và delete apply.

### Muốn vận hành project ngay

Đọc [architecture](../architecture.md), sau đó làm theo
[runbook](../runbook.md). Tài liệu learning giải thích lý do; runbook ưu tiên
lệnh và recovery action.

## Cách học hiệu quả với repository

Cho mỗi level:

1. Đọc tài liệu mà chưa mở code, tự vẽ lại flow.
2. Mở các file trong mục “File cần đọc” hoặc `code-map.md`.
3. Dự đoán kết quả command trước khi chạy.
4. Chạy happy path, sau đó cố ý tạo một failure có thể phục hồi.
5. Chạy lại để kiểm tra idempotency thay vì chỉ nhìn lần đầu success.
6. Viết lại trade-off bằng lời của mình; tránh học thuộc tên tool.

## Bài thực hành gợi ý

- Level 2: làm một extract task fail tạm thời và quan sát fan-in/barrier.
- Level 3: đổi email customer, snapshot lại và temporal-query hai version.
- Level 4: backfill hai cửa sổ liền nhau, chứng minh không trùng biên.
- Level 5: chuyển một order sang ngày khác, kiểm tra ngày cũ bị xóa aggregate.
- Level 6: xem Spark UI và tìm stage có shuffle.
- Level 7: chạy no-op, chứng minh snapshot id không đổi; sau đó time travel.
- Level 8: publish cùng event hai lần, kiểm tra history/current grain.
- Level 9: làm một downstream branch fail, chứng minh watermark chưa advance.
- Level 10: dừng Kafka, quan sát pending → firing → resolved alert.
- Level 11: chạy release checklist mà không xóa volume/checkpoint.
- Level 12: chạy CDC smoke test, đọc đủ `c/u/d/tombstone`, rồi restart Connect
  và chứng minh slot/offset tiếp tục thay vì snapshot lại.
- Level 13: gửi proposed breaking schema vào compatibility endpoint và chứng
  minh bị từ chối mà không tạo version rác.
- Level 14: chạy materialization test và reconcile source/Silver sau restart.
- Level 15: so manifest baseline/current và dự đoán node `state:modified+`.

Không thực hiện destructive exercise trên dữ liệu cần giữ. `down -v`, schema
reset và xóa checkpoint chỉ dùng sau khi đã hiểu state nào sẽ mất.
