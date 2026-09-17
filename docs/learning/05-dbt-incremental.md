# Level 5 — dbt incremental models

## Mục tiêu

`fact_sales` và `daily_sales` không rebuild toàn bộ bảng ở mỗi DAG run. Chúng
chỉ xử lý các record được Airflow nạp mới hoặc cập nhật vào staging, nhưng vẫn
giữ được tính idempotent khi chạy lại.

dbt ở đây không thay Airflow extract. Airflow chịu trách nhiệm đưa source vào
staging và giữ cross-system watermark; dbt chịu trách nhiệm dependency SQL,
materialization, test, documentation và lineage từ staging tới mart. Ranh giới
này giúp mỗi tool làm đúng sở trường.

## Vì sao có ba layer model?

| Layer | Trách nhiệm | Materialization trong project |
| --- | --- | --- |
| `stg_` | chuẩn hóa tên/type, giữ gần source | view |
| `int_` | join/aggregate reusable theo business grain | view |
| marts | contract cho consumer | view hoặc incremental table |

Staging view không tồn tại chỉ để đổi tên cho đẹp. Nó là dbt node đầu tiên nối
physical source bằng `source()` với lineage/test/docs. Tuy nhiên, staging không
phải “lớp chống mọi breaking change” một cách thần kỳ: nếu source đổi cột thì
staging SQL vẫn phải sửa. Giá trị thật là downstream chỉ phụ thuộc contract
`stg_*`, nên migration được tập trung tại một biên thay vì sửa mọi consumer.

Intermediate tránh lặp logic payment aggregation và order-item enrichment giữa
nhiều mart. Nó không phải public BI contract, vì vậy có thể refactor dễ hơn.

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

Trước MERGE, model resolve `customer_sk` bằng temporal join với SCD2. Nó giữ cả
`customer_id` để trace về source. Index trên `order_date`, `customer_sk`,
`product_id`, `source_loaded_at` phục vụ filter/join phổ biến; index không được
thêm tùy tiện vì mỗi index làm write/merge tốn thêm chi phí.

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

`daily_sales` không chỉ lấy các fact mới rồi cộng dồn. Cộng dồn sẽ sai khi một
fact cũ bị update quantity/payment hoặc chuyển ngày. Model xác định ngày bị ảnh
hưởng rồi tính lại toàn bộ ngày đó từ fact canonical; đây là recompute nhỏ có
tính quyết định, dễ reasoning hơn delta arithmetic.

## Jinja compile-time và SQL run-time

`is_incremental()` và vòng lặp `adapter.get_columns_in_relation(this)` chạy khi
dbt render model. SQL kết quả mới chạy ở PostgreSQL. Project kiểm tra target có
cột high-water hay chưa để nâng cấp relation cũ an toàn: lần migration đầu đọc
đủ dữ liệu, dbt đồng bộ cột; các lần sau mới bật filter incremental.

`ref()` không chỉ thay tên bảng. Nó tạo dependency trong DAG dbt, tự chọn đúng
schema target và cho phép dbt build upstream trước downstream. `source()` đánh
dấu relation do hệ thống ngoài dbt sở hữu.

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
- `on_schema_change='sync_all_columns'` cho phép hai mart nhận cột mới trong lần
  nâng cấp đã biết. Nó không thay data contract review: rename/drop/type change
  vẫn phải test và rollout có chủ đích. Nếu tổ chức muốn drift luôn dừng, đổi
  policy về `fail` sau khi migration hoàn tất.
- Incremental không tự giải quyết hard delete ở source. Debezium hiện đã
  **capture và lưu** customer delete trong raw Kafka topic, nhưng chưa có
  consumer **apply** delete đó vào staging/mart. Vì vậy dbt vẫn chưa thấy
  customer biến mất. Late data cũ hơn Airflow watermark vẫn cần explicit
  backfill hoặc một ingestion contract mới.
- `--full-refresh` là thao tác có chủ đích, không dùng trong DAG hằng ngày.

## Vì sao thiết kế này thay vì các lựa chọn khác?

| Lựa chọn | Không dùng làm mặc định vì sao? |
| --- | --- |
| Rebuild table mỗi ngày | đơn giản nhưng write/read toàn bộ và mất lợi ích incremental |
| Increment theo `order_date` | bỏ sót update cho order lịch sử |
| Cộng delta trực tiếp vào daily mart | khó đảo update/chuyển ngày và dễ double count khi retry |
| Dùng hash mọi cột cho fact | vẫn cần xác định candidate rows; `source_loaded_at` đã là ingestion boundary |
| Dùng dbt để extract source | dbt không sở hữu cross-system commit barrier/Kafka handoff |

## Bản đồ file của level

- `dbt/models/staging/`: source contract và generic tests.
- `dbt/models/intermediate/`: payment/order-item logic dùng lại.
- `dbt/models/marts/fact_sales.sql`: incremental row-grain model.
- `dbt/models/marts/daily_sales.sql`: affected-partition recompute.
- `dbt/tests/`: reconciliation/invariant không diễn đạt đủ bằng generic test.
- `.github/workflows/dbt-ci.yml`: initial build, no-op replay và mutation test.

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
