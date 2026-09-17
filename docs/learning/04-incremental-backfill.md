# Level 4 — Incremental load, idempotency, backfill và late data

## Vì sao không full load mãi?

Full load đơn giản và thường là lựa chọn đúng khi dữ liệu nhỏ. Khi volume tăng,
đọc và ghi lại toàn bộ bảng mỗi ngày làm tăng thời gian, I/O, lock và chi phí.
Incremental load chỉ xử lý vùng thay đổi kể từ lần commit thành công gần nhất.

Đổi lại, incremental tạo state. Từ đây pipeline phải trả lời chính xác: đã xử lý
tới đâu, retry có an toàn không, update cũ xử lý thế nào và backfill có làm lệch
cursor bình thường không.

## Watermark là gì?

Mỗi source table có một row trong `metadata.etl_watermark`:

| Cột | Ý nghĩa |
| --- | --- |
| `pipeline_name` | định danh pipeline/table |
| `watermark_value` | upper bound đã commit hoàn chỉnh |
| `candidate_value` | upper bound đã extract nhưng chưa qua mọi downstream |
| `updated_at` | lúc committed watermark thay đổi |

Source `updated_at` là cursor. Scheduled window dùng:

```text
(watermark_value, max_source_updated_at]
```

Lower bound mở tránh đọc lại version đã commit; upper bound đóng giữ record đúng
snapshot max của batch. Upper bound được chụp trước query, vì source có thể tiếp
tục nhận update trong lúc pipeline chạy.

## Vì sao cần candidate watermark?

Nếu extract lập tức ghi đè committed watermark rồi dbt hoặc Kafka thất bại, lần
retry sẽ bỏ qua batch chưa hoàn tất. Candidate tạo two-phase protocol ở mức
application:

1. Extract/upsert staging và ghi candidate.
2. dbt, Iceberg, Kafka xử lý batch.
3. Chỉ khi cả ba thành công mới promote candidate thành committed watermark.

Đây không phải distributed transaction. Nó là commit barrier kết hợp các sink
idempotent để đạt at-least-once nhưng không nhân đôi business grain.

## Idempotency là gì?

Một operation idempotent cho cùng input có thể chạy nhiều lần mà final state
không thay đổi sau lần đầu thành công. Project áp dụng theo từng layer:

| Layer | Stable identity | Cơ chế retry-safe |
| --- | --- | --- |
| Staging | source primary key | PostgreSQL upsert |
| dbt fact | `order_item_id` | incremental merge |
| Daily mart | `sales_date` | merge + repair affected dates |
| Iceberg fact | `order_item_id` | append đầu tiên, merge về sau |
| Kafka event | UUID5 từ order/version | producer retry + sink merge |
| Current order | `order_id` | version-aware merge |

Idempotency không có nghĩa job không làm gì khi chạy lại; nó có thể đọc/so sánh,
nhưng final business state không bị nhân đôi.

## Vì sao staging dùng upsert?

Staging là current-state landing table, không phải raw immutable archive. Cùng
primary key từ source có thể đến lại sau retry hoặc có `updated_at` mới. Upsert
insert key mới và update key cũ, đồng thời đổi `loaded_at` để downstream biết
platform vừa nhận version nào.

Nếu yêu cầu audit mọi source version, cần raw append-only/CDC layer riêng. Không
nên ép current-state staging đóng cả hai vai trò.

## Backfill khác scheduled incremental run

Backfill nhận explicit half-open window `[start, end)` và không advance committed
watermark. Lý do: backfill có thể nằm trước hoặc cắt ngang cursor hiện tại; cho nó
di chuyển cursor sẽ khiến scheduled flow bỏ sót khoảng khác.

Staging upsert và downstream merge khiến chạy lại cùng cửa sổ an toàn. Airflow
UI có backfill theo logical dates, nhưng project dùng parameter timestamp để hỗ
trợ cả giờ/phút/giây và một DAG contract duy nhất.

## Hard delete

Current-state staging không tự phát hiện row bị source xóa vì watermark query chỉ
thấy row còn tồn tại. dbt snapshot có thể invalidate hard delete khi snapshot
source hiện tại, nhưng extract deletion cần tombstone/CDC hoặc periodic full-key
reconciliation.

Level 12 đã capture hard delete mới của `public.customers` vào raw Kafka CDC
topic. Staging chưa consume topic để giữ single-writer ownership; bước
Bronze/Silver sẽ materialize rồi cut over. Các table khác vẫn là deferred risk,
được ghi rõ thay vì giả vờ incremental timestamp giải quyết.

## Late-arriving data

Late data nguy hiểm nhất khi một record mới xuất hiện nhưng `updated_at` nhỏ hơn
committed watermark; scheduled filter sẽ không thấy nó. Backfill sửa được khi ta
biết khoảng bị thiếu. Production có thể dùng:

- ingestion timestamp do platform kiểm soát;
- CDC log position;
- lookback window + dedupe;
- periodic reconciliation;
- source SLA/metric về event lateness.

Project đánh dấu tình huống này là accepted risk do xác suất thấp trong dữ liệu
demo và dùng explicit backfill khi phát hiện.

## Failure walkthrough

Giả sử candidate là `10:00`:

1. Extract customers/orders xong, Spark thất bại.
2. Committed watermark vẫn là `09:00`; candidate giữ `10:00`.
3. Retry đọc lại `(09:00, 10:00]` hoặc source max mới hơn.
4. Staging upsert không nhân row; sinks merge/dedupe.
5. Khi mọi nhánh xanh, watermark thành max candidate và candidate về `NULL`.

Candidate treo là tín hiệu operational quan trọng, vì nó chỉ ra batch đã bắt đầu
nhưng chưa commit hoàn chỉnh.

## Tối ưu và trade-off

- Index `updated_at` ở source khi volume lớn; hiện schema demo chưa thêm để giữ
  bài học tập trung, nhưng production scan cần đo query plan.
- Không dùng `NOW()` làm upper bound nếu source writer có clock/transaction lệch;
  max source timestamp cho cửa sổ ổn định hơn trong phạm vi thiết kế này.
- Một watermark/table cho phép các bảng tiến độc lập khi extract, nhưng barrier
  cuối yêu cầu đủ năm candidate để tránh batch nửa vời.
- `max_active_runs=1` đơn giản hóa concurrency. Muốn song song cần batch id và
  state machine rõ hơn.

## Điều kiện hoàn thành

- Lần chạy không có thay đổi không tăng row count.
- Lỗi downstream không advance watermark.
- Retry sau lỗi hoàn tất mà không duplicate.
- Backfill đúng `[start, end)` và không đổi cursor scheduled.
- Late data/hard delete được ghi nhận là risk hoặc có cơ chế riêng.
