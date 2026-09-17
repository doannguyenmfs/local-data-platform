# Level 5 — dbt incremental models

## Mục tiêu

`fact_sales` và `daily_sales` không rebuild toàn bộ bảng ở mỗi DAG run. Chúng
chỉ xử lý các record được Airflow nạp mới hoặc cập nhật vào staging, nhưng vẫn
giữ được tính idempotent khi chạy lại.

## Ba loại thời gian không được trộn lẫn

| Cột | Ý nghĩa | Dùng cho |
| --- | --- | --- |
| `order_date` | Thời gian nghiệp vụ của đơn hàng | Phân tích doanh thu theo ngày |
| `updated_at` | Thời gian source nói record đã thay đổi | Watermark extract của Airflow |
| `loaded_at` / `source_loaded_at` | Thời gian platform nạp record | Incremental transform của dbt |

Không dùng `order_date` để lọc incremental: một đơn hàng tháng trước vẫn có thể
được cập nhật trạng thái hoặc nhận payment hôm nay.

## Luồng dữ liệu

```text
source updated_at
      │ Airflow [watermark_start, watermark_end)
      ▼
staging.loaded_at
      │ greatest/max qua các bảng liên quan
      ▼
intermediate.source_loaded_at
      │ dbt incremental merge theo order_item_id
      ▼
fact_sales.source_loaded_at
      │ chỉ tính lại affected_dates
      ▼
daily_sales.max_source_loaded_at
```

## Cơ chế `fact_sales`

- Materialization: `incremental`.
- Strategy: PostgreSQL `merge`.
- Unique key: `order_item_id`, đúng với grain một dòng trên mỗi order item.
- Run đầu tạo toàn bộ bảng.
- Run sau chỉ đọc intermediate rows có `source_loaded_at` lớn hơn high-water
  mark hiện có.
- `merge` insert business key mới và update business key đã tồn tại.

`previous_order_date` lưu ngày cũ khi một order item bị chuyển từ ngày A sang
ngày B. Đây không phải measure nghiệp vụ; nó là metadata sửa aggregate. Nếu chỉ
tính lại ngày B, số liệu còn lại của ngày A sẽ bị stale.

## Cơ chế `daily_sales`

Model lấy hai tập ngày bị ảnh hưởng:

1. `order_date` hiện tại của fact vừa đổi.
2. `previous_order_date`, nếu business timestamp bị chuyển ngày.

Sau đó model aggregate lại toàn bộ fact của đúng các ngày đó và `merge` theo
`sales_date`. Vì vậy payment/status/quantity thay đổi sẽ sửa đúng aggregate mà
không ghi lại mọi ngày.

Post-hook xóa aggregate không còn fact tương ứng. Nó cần thiết khi record duy
nhất của ngày A được chuyển sang ngày B: phép `merge` có thể tạo ngày B nhưng
không thể tự suy ra rằng row ngày A phải bị xóa.

## Vì sao CI chạy `dbt build` hai lần?

Lần thứ nhất kiểm tra khả năng khởi tạo. Lần thứ hai đi qua nhánh
`is_incremental()` và kiểm tra SQL incremental compile được, merge không nhân
đôi dữ liệu và tests vẫn pass sau khi chạy lại. CI còn thay đổi payment và
chuyển ngày của một order, chạy lần thứ ba rồi dùng SQL assertion kiểm tra kết
quả thực tế.

## Chạy local

Lần chuyển đổi đầu tiên từ table cũ sang incremental có thêm cột, chạy:

```bash
dbt/.venv/bin/dbt build \
  --project-dir dbt \
  --profiles-dir dbt \
  --select '+fact_sales daily_sales' \
  --full-refresh
```

Kiểm tra nhánh incremental bằng cách chạy lại không có `--full-refresh`:

```bash
dbt/.venv/bin/dbt build \
  --project-dir dbt \
  --profiles-dir dbt \
  --select '+fact_sales daily_sales'
```

## Lưu ý production

- Incremental filter dùng toán tử `>` vì một dbt model build là transaction:
  hoặc cả batch merge thành công, hoặc rollback.
- `on_schema_change='fail'` cố tình làm pipeline dừng khi schema drift. Thay đổi
  schema phải được review và migrate rõ ràng.
- Incremental không tự giải quyết hard delete ở source. Project hiện coi source
  delete và late data cũ hơn Airflow watermark là accepted risk; backfill được
  dùng khi phát hiện.
- `--full-refresh` là thao tác có chủ đích, không dùng trong DAG hằng ngày.

## Câu hỏi phỏng vấn

**Tại sao unique key là `order_item_id` chứ không phải `order_id`?**  
Vì grain của fact là một dòng trên mỗi order item; một order có nhiều item.

**Tại sao không dùng `updated_at` trực tiếp trong dbt?**  
`updated_at` thuộc source và là watermark extract. `source_loaded_at` chứng minh
record đã thật sự đi vào staging của platform, nên phù hợp để nối ranh giới giữa
EL và T.

**Incremental có luôn nhanh hơn table không?**  
Không. Nó giảm lượng transform/write khi phần dữ liệu thay đổi nhỏ, nhưng đổi
lại cần unique key, high-water mark, xử lý update/delete và test idempotency.

## Điều kiện hoàn thành

- Full refresh pass.
- Hai incremental build liên tiếp pass.
- Grain tests và daily reconciliation pass.
- Row count không tăng khi không có dữ liệu mới.
