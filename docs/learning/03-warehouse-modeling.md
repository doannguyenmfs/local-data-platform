# Level 3 — Data warehouse, grain và SCD Type 2

## Vì sao cần mô hình phân tích riêng?

Source OLTP tối ưu cho ghi transaction và tính toàn vẹn nghiệp vụ. Câu hỏi phân
tích thường cần join nhiều bảng, giữ lịch sử và aggregate lặp lại. Nếu dashboard
query trực tiếp source:

- workload phân tích cạnh tranh với application;
- logic metric bị copy sang nhiều dashboard;
- thay đổi record source làm mất bối cảnh lịch sử;
- consumer phải hiểu schema vận hành quá chi tiết.

Warehouse tạo contract ổn định giữa dữ liệu vận hành và phân tích.

## Grain phải được nói trước cột

Grain là “một dòng đại diện điều gì?”. Trong project:

| Model | Grain | Khóa kiểm tra |
| --- | --- | --- |
| `dim_product` | một product hiện tại | `product_id` |
| `dim_customer` | một version của customer | `customer_sk` |
| `fact_sales` | một order item | `order_item_id` |
| `daily_sales` | một ngày bán hàng | `sales_date` |

Nếu không chốt grain trước, join dễ nhân dòng và metric sai nhưng SQL vẫn chạy.
Mỗi model quan trọng vì vậy có unique/not-null hoặc singular test bảo vệ grain.

## Dimension và fact

Dimension mô tả thực thể: ai, cái gì, thuộc tính nào. Fact mô tả sự kiện đo được:
đơn hàng nào, số lượng bao nhiêu, doanh thu bao nhiêu, xảy ra lúc nào.

`fact_sales` giữ `unit_price` của transaction thay vì luôn lấy giá hiện tại từ
`dim_product`. Nếu catalog price thay đổi, doanh thu lịch sử không được đổi theo.

## Vì sao customer dùng SCD2 còn product dùng current-state?

Customer cần trả lời “tại thời điểm đặt đơn, email/tên của customer là version
nào?”, nên cần SCD Type 2. Product trong phạm vi bài học chỉ cần catalog hiện
tại; giá giao dịch đã được giữ trong fact, nên chưa cần lịch sử product.

Đây là quyết định theo requirement, không phải mọi dimension đều mặc định SCD2.
SCD2 tăng row count, join complexity và retention cost; chỉ dùng khi historical
attribute có giá trị phân tích.

## Snapshot customer hoạt động thế nào?

Snapshot YAML định nghĩa:

- `unique_key: customer_id`: business entity nào đang được version hóa;
- `strategy: check`: so sánh các cột business được liệt kê;
- `check_cols`: `first_name`, `last_name`, `email` tạo version khi đổi;
- `updated_at: updated_at`: timestamp source dùng cho metadata/effective time.

dbt tự sinh các metadata column như `dbt_scd_id`, `dbt_updated_at`,
`dbt_valid_from` và `dbt_valid_to`. `dbt_scd_id` là identifier version do dbt
tạo; nó không “tự biết” là SK.
Model `dim_customer` chủ động alias nó thành `customer_sk`, từ đó project định
nghĩa semantics và test unique/not-null.

Snapshot hiện chưa thấy hard delete ở OLTP vì staging là current-state upsert
nhưng chưa có bước xóa/tombstone key đã biến mất ở source. Việc thêm mapping
delete source → staging đã được chủ động để lại cho bước nâng cấp sau; không nên
ghi tài liệu rằng snapshot đã xử lý một tín hiệu mà upstream chưa cung cấp.

## Valid time và load time

Với check strategy có cấu hình `updated_at`, dbt dùng timestamp source trong
SCD metadata thay vì chỉ lấy giờ job chạy. Điều này biểu diễn version có hiệu
lực theo timestamp source.
`loaded_at` lại trả lời khi platform nhìn thấy row. Hai thời gian phục vụ hai câu
hỏi khác nhau và không được dùng thay nhau.

## Temporal join trong `fact_sales`

Fact nối customer version theo điều kiện:

```text
customer_id giống nhau
AND order_date >= valid_from
AND (order_date < valid_to OR valid_to IS NULL)
```

Khoảng `[valid_from, valid_to)` tránh hai version cùng match đúng timestamp biên.
Với dữ liệu seed ban đầu, order có thể cũ hơn version snapshot đầu tiên; model có
fallback `earliest_available` để không làm mất fact. Cột
`customer_key_resolution` công khai việc fallback thay vì âm thầm che giấu.

## Tại sao `dim_customer` là view?

Snapshot đã là bảng vật lý chứa lịch sử. `dim_customer` chủ yếu đổi tên metadata
column và tạo `is_current`; materialize thêm table sẽ sao chép toàn bộ lịch sử
mỗi lần build. View tránh duplicate storage và luôn phản ánh snapshot mới nhất.

Trade-off: query dimension phụ thuộc snapshot và có thể chậm hơn table nếu logic
nặng. Ở đây projection rất nhẹ nên view là lựa chọn hợp lý. Nếu workload BI lớn,
có thể dùng incremental/table nhưng phải định nghĩa refresh strategy rõ ràng.

## Aggregate mart

`daily_sales` đưa metric dùng thường xuyên về grain một ngày:

- `total_orders`: distinct order;
- `total_items`: tổng quantity;
- `total_sales`: gross sales;
- `paid_sales`: sales thuộc order đã paid.

Aggregate giúp dashboard nhanh và thống nhất metric. Singular test full-outer
join aggregate với phép tính trực tiếp từ fact để phát hiện thiếu ngày, thừa
ngày và sai measure.

## Vì sao database constraint chưa đủ?

Constraint source bảo vệ transaction tại lúc ghi. Sau extract/join/aggregate,
pipeline có thể làm sai grain dù source đúng. dbt tests kiểm tra contract ở mỗi
layer: source relationship, intermediate row preservation, dimension invariant,
fact grain và aggregate reconciliation.

## Kết quả áp dụng

Consumer có model theo business meaning thay vì query năm bảng vận hành. Lịch sử
customer được giữ, fact resolve đúng version theo thời gian và daily metric được
kiểm tra ngược về source-of-truth fact.

## Mẹo thiết kế

- Viết grain vào description trước khi viết SQL.
- Giữ business key trong fact để trace, dù đã có surrogate key.
- Đừng dùng current dimension join cho historical fact nếu thuộc tính cần “as of”.
- `is_current` tiện cho query, nhưng validity range mới là contract lịch sử.
- Test reconciliation thường bắt lỗi tốt hơn chỉ test not-null.

## Điều kiện hoàn thành

- Mỗi model có grain rõ và test bảo vệ grain.
- Một customer đổi thuộc tính tạo version mới, version cũ được đóng.
- Mỗi customer hiện diện trong staging có đúng một current version.
- Fact không bị nhân dòng sau temporal join.
- Daily aggregate khớp fact.
