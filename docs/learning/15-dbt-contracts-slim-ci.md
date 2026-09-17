# Level 15 — dbt model contracts và state-aware slim CI

## 1. Contract khác data test

Data test chạy query trên data đã build:

- key có null/duplicate không;
- relationship có orphan không;
- metric có âm không.

Model contract kiểm tra interface trước khi publish relation:

- tên cột;
- số/thứ tự cột;
- data type;
- constraint nếu adapter hỗ trợ/được khai báo.

Một model có đúng data hôm nay vẫn có thể phá dashboard nếu đổi `customer_id`
từ UUID sang text hoặc bỏ cột. Vì vậy public marts cần cả contract lẫn test.

## 2. Project áp dụng contract thế nào?

`dbt/models/marts/_marts_models.yml` bật:

```yaml
config:
  contract:
    enforced: true
```

cho `dim_product`, `dim_customer`, `fact_sales`, `daily_sales`. Mọi output column
có `data_type` rõ: UUID, text, timestamptz, numeric precision/scale, bigint...

`fact_sales` và `daily_sales` là incremental. Trước đây
`on_schema_change=sync_all_columns` cho dbt tự đổi table theo query. Điều đó
mâu thuẫn contract, nên chuyển thành `fail`:

- SQL/YAML lệch nhau => build fail;
- developer phải cập nhật contract/migration có review;
- consumer không nhận schema change âm thầm.

Contract không version API thay ta. Breaking change vẫn cần quy trình rename,
deprecation window hoặc model `v2`; contract chỉ biến drift thành failure sớm.

## 3. State là gì?

Sau `dbt parse/build`, `manifest.json` mô tả node, config, compiled dependency và
checksum. dbt có thể so manifest code hiện tại với manifest baseline:

```text
baseline manifest (main) + current project (PR)
                   │
                   └─ state:modified+ = node đổi và mọi downstream
```

Dấu `+` phía sau rất quan trọng. Chỉ build model sửa trực tiếp có thể bỏ qua
consumer downstream bị ảnh hưởng.

## 4. `--defer` hoạt động thế nào?

Slim schema không có toàn bộ relation. Với `--defer`, một `ref()` tới node không
được chọn sẽ resolve sang relation từ baseline state thay vì báo thiếu:

```text
modified model ──ref unchanged parent──► baseline schema relation
       │
       └── downstream selected models──► PR schema
```

`--favor-state` ưu tiên baseline relation cho node chưa chọn ngay cả nếu relation
cùng tên tình cờ tồn tại trong CI schema. Điều này làm dependency source rõ và
tránh state rác từ run trước.

Defer chỉ an toàn khi manifest và baseline relations cùng một deployment state.
Chỉ upload manifest mà đã drop schema baseline thì ref vẫn hỏng.

## 5. Workflow hai tầng

### Pull request: feedback nhanh

1. checkout đầy đủ history (`fetch-depth: 0`);
2. tạo detached worktree tại base SHA;
3. build base project vào schema `dbt_ci_base_<run>` và lưu manifest riêng;
4. parse current project;
5. chạy current với:

```bash
dbt build \
  --select 'state:modified+' \
  --state /tmp/dbt-base-target \
  --defer \
  --favor-state
```

Fixture vẫn seed legacy `staging.customers` để base commit trước CDC cutover có
thể build; current code đọc Silver. Đây là compatibility của CI baseline, không
khôi phục dual writer runtime.

### Main/workflow dispatch: full stateful proof

Sau merge, workflow vẫn:

1. full build lần đầu;
2. full build lần hai để chứng minh no-change idempotency;
3. mutate payment/order/customer;
4. build incremental;
5. assert exact fact/daily/SCD2 result.

Slim CI tối ưu PR latency; full main build giữ khả năng phát hiện tương tác mà
state selection/checksum không dự đoán được. Không nên bỏ mọi full build.

## 6. Mutation test customer

CI seed ba customers:

- customer 1 có order và được update => phải có hai SCD2 versions;
- customer 2 có order và vẫn active;
- customer 3 không có order và bị hard-delete => giữ closed history, zero
  current version.

Thiết kế customer 3 tránh một fixture phi thực tế: OLTP foreign key không cho
xóa customer đang có orders. Test tốt phải sinh được từ source contract thật.

## 7. Cách kiểm tra local

Full project:

```bash
dbt/.venv/bin/dbt build \
  --project-dir dbt \
  --profiles-dir dbt \
  --target dev \
  --no-partial-parse
```

Quan sát selection trước khi build:

```bash
dbt/.venv/bin/dbt ls \
  --project-dir dbt \
  --profiles-dir dbt \
  --select 'state:modified+' \
  --state /path/to/baseline/target
```

Không dùng `dbt/target` hiện tại làm cả baseline và output của run hiện tại;
parse có thể overwrite manifest bạn định so. Giữ baseline target ở thư mục
riêng hoặc artifact CI bất biến.

## 8. Failure mode và mẹo

| Hiện tượng | Nguyên nhân | Cách xử lý |
| --- | --- | --- |
| contract compile error | thiếu `data_type`/cột lệch | sửa YAML và SQL cùng review |
| incremental contract từ chối sync | `sync_all_columns` tự đổi API | dùng `fail`, viết migration rõ |
| defer relation not found | có manifest nhưng baseline schema đã mất | giữ relation hoặc build baseline trước |
| không node nào selected | PR không đổi dbt graph | hợp lệ; static parse vẫn chạy |
| downstream không chạy | quên `+` sau modified | dùng `state:modified+` |
| state giả modified hàng loạt | dbt version/env/config drift | pin dbt, so đúng base SHA, kiểm tra manifest |

Slim CI giảm compute, không thay unit/data test. Contract giảm schema drift,
không thay semantic tests. Hai công cụ giải quyết hai failure class khác nhau.

## 9. File cần đọc

- `dbt/models/marts/_marts_models.yml`: enforced public contracts.
- `dbt/models/marts/fact_sales.sql`: incremental + schema-change fail policy.
- `dbt/models/marts/daily_sales.sql`: contracted aggregate.
- `.github/workflows/dbt-ci.yml`: base state, defer, full-after-merge path.
- `postgres/ci/*.sql`: deterministic seed/mutation/assertion.
