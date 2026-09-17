# Level 7 — Apache Iceberg lakehouse

## Khái niệm và lý do cần Iceberg

Parquet định nghĩa cách mã hóa một file. Nó không biết tập file nào tạo thành
một bảng hợp lệ, commit nào hoàn chỉnh hay schema đã tiến hóa ra sao. Iceberg là
open table format bổ sung metadata tree và atomic snapshot phía trên các data
file.

Một Iceberg table có ba lớp độc lập:

```text
Spark (compute)
   │
   ├── JDBC catalog trong PostgreSQL: namespace/table → metadata hiện tại
   │
   └── MinIO: metadata JSON, manifest list, manifest, Parquet data files
```

Catalog không chứa toàn bộ dữ liệu. Nó giữ con trỏ đến metadata hiện tại.
MinIO không hiểu table; nó chỉ giữ immutable objects. Iceberg commit tạo metadata
mới rồi atomically đổi pointer trong catalog.

## Cơ chế snapshot và ACID

Mỗi write tạo snapshot mới:

```text
catalog pointer
      │
      ▼
metadata.json → snapshot → manifest list → manifests → data files
```

Reader đã mở snapshot cũ tiếp tục đọc danh sách file cũ. Writer tạo file và
metadata mới rồi commit pointer; reader không nhìn thấy trạng thái nửa cũ nửa
mới. Đây là snapshot isolation và là khác biệt cốt lõi với overwrite một thư
mục Parquet.

## Áp dụng trong project

- Catalog: Iceberg JDBC catalog trên PostgreSQL hiện có.
- Object storage: MinIO bucket `warehouse`.
- Table: `local.ecommerce.fact_sales`.
- Table format version: 2, cần cho row-level update/delete hiệu quả hơn.
- Partition: hidden transform `days(order_date)`.
- Upsert: `MERGE INTO` theo grain `order_item_id`.

Hidden partitioning cho phép query filter `order_date`; người dùng không phải
tự tạo và nhớ cột thư mục `year/month/day`. Iceberg dùng metadata để prune file.

`source_loaded_at` vừa lọc các staging row mới hơn high-water mark của Iceberg,
vừa ngăn update cũ ghi đè bản mới. Retry sau một commit thành công có thể không
có update nào và vẫn kết thúc an toàn.

## Catalog, metadata và data file nằm ở đâu?

Một table name như `local.ecommerce.fact_sales` có ba phần:

- `local`: catalog name Spark cấu hình trong `iceberg_common.py`;
- `ecommerce`: Iceberg namespace;
- `fact_sales`: table.

JDBC catalog tables nằm trong PostgreSQL `public` schema và chỉ giữ namespace,
table registration cùng current metadata location. MinIO bucket `warehouse` giữ
`metadata.json`, manifest list, manifest và Parquet data/delete files. Spark là
compute engine đọc catalog rồi truy cập object store; dừng Spark không làm mất
table.

## First append, incremental merge và no-op

Job tạo table nếu chưa có rồi kiểm tra table rỗng:

1. Bảng rỗng: append toàn bộ initial dataset, tránh row-level MERGE plan vô ích.
2. Có delta: MERGE theo `order_item_id`, chỉ update khi source version mới hơn.
3. Không delta: không write, không tạo snapshot rỗng.

No-op quan trọng cho metadata hygiene. Một scheduler chạy hằng ngày nhưng source
không đổi không nên tạo hàng nghìn snapshots vô nghĩa.

## Copy-on-write và merge-on-read

`fact_sales` batch phù hợp copy-on-write: update ít thường xuyên, reader đơn giản
và batch có thời gian rewrite file. `current_orders` streaming dùng
merge-on-read: update ghi delete/data delta nhanh hơn nhưng reader phải merge và
maintenance phải compact position-delete files.

Không có mode luôn tốt hơn. Quyết định phụ thuộc write frequency, read latency,
file size và maintenance budget.

## Chạy

```bash
docker compose --profile spark --profile lakehouse up -d --build \
  postgres minio minio-init spark-master spark-worker

docker compose --profile '*' run --rm --no-deps iceberg-submit
```

MinIO API: <http://localhost:9000>  
MinIO console: <http://localhost:9001>

Chạy lại `iceberg-submit` để tạo snapshot merge tiếp theo. Xem lịch sử:

```bash
docker compose --profile '*' run --rm --no-deps \
  --entrypoint /opt/spark/bin/spark-sql \
  iceberg-submit \
  -f /opt/spark/sql/iceberg_demo.sql
```

## Time travel

Metadata table `fact_sales.snapshots` trả về `snapshot_id`. Thay id vào:

```sql
SELECT *
FROM local.ecommerce.fact_sales VERSION AS OF <snapshot_id>;
```

Time travel hữu ích để điều tra regression, so sánh trước/sau pipeline và khôi
phục logic. Nó không phải backup độc lập: nếu snapshot và file đã expire/xóa,
time travel không thể cứu dữ liệu.

## Maintenance

```bash
docker compose --profile '*' run --rm --no-deps iceberg-maintenance
```

Job có hai thao tác:

1. `rewrite_data_files`: compact file nhỏ về target khoảng 128 MiB.
2. `expire_snapshots`: xóa snapshot cũ hơn 7 ngày nhưng luôn giữ ít nhất 10 bản.

Với `current_orders`, job còn gọi `rewrite_position_delete_files`. Bốn table
được duyệt độc lập; table stream chưa được tạo sẽ được báo `not_created_yet`
thay vì làm cả maintenance run thất bại.

Không expire snapshot ngay sau mỗi ingest. Maintenance có workload, SLA và
chính sách retention riêng; tách job giúp retry ingest không vô tình xóa lịch
sử.

## Lưu ý production

- MinIO credentials mặc định chỉ dành cho local development; production phải
  dùng secret manager và rotate credentials.
- JDBC catalog bền hơn in-memory REST fixture. Khi nhiều engine/team cùng dùng,
  có thể đặt REST catalog/Polaris phía trước để tách client khỏi catalog backend.
- Không xóa object trực tiếp trong bucket. Iceberg metadata mới biết file nào
  đang được snapshot tham chiếu.
- Snapshot nhiều vô hạn làm tăng metadata và storage; expire quá sớm làm mất
  khả năng rollback. Retention phải gắn với SLA và backup.
- Small-file problem đến từ micro-batch ghi quá thường xuyên; compaction chữa
  hậu quả, còn batch sizing giải quyết nguyên nhân.

## Vì sao không partition theo mọi cột?

Partition là pruning structure, không phải index miễn phí. Partition theo UUID
hoặc high-cardinality key tạo quá nhiều partition/file nhỏ. `days(order_date)`
có cardinality vừa phải và phù hợp query doanh thu theo thời gian. Iceberg hidden
partitioning còn cho phép evolve transform mà consumer không đổi query column.

## File cần đọc

- `spark/jobs/iceberg_common.py`: catalog/S3/timezone configuration duy nhất.
- `spark/jobs/iceberg_sales.py`: create, delta filter, append/MERGE, validate.
- `spark/jobs/maintain_iceberg.py`: file/snapshot retention policy.
- `spark/jobs/validate_platform.py`: read-only grain check qua catalog.
- `spark/sql/iceberg_demo.sql`: metadata table và time-travel query.

## Câu hỏi phỏng vấn

**Iceberg khác Parquet thế nào?**  
Parquet là file format; Iceberg là table format quản lý tập Parquet files,
schema, partition, snapshot và transaction.

**Hidden partitioning giải quyết gì?**  
Query dùng business column, còn Iceberg tự áp partition transform và evolution;
consumer không phụ thuộc layout thư mục vật lý.

**Time travel có phải backup không?**  
Không. Snapshot history chia sẻ data files và chịu retention; backup phải độc
lập về failure domain và chính sách lưu giữ.

## Điều kiện hoàn thành

- Bucket `warehouse` tồn tại.
- `iceberg-submit` merge thành công và in `snapshot_id`.
- Chạy lại không nhân đôi `order_item_id`.
- Metadata table liệt kê nhiều snapshot.
- Time-travel query đọc được snapshot cũ.
- Maintenance chạy mà vẫn giữ tối thiểu 10 snapshot khi lịch sử đủ dài.

## Tài liệu chính thức

- [Iceberg Spark getting started](https://iceberg.apache.org/docs/latest/spark-getting-started/)
- [Iceberg Spark writes](https://iceberg.apache.org/docs/latest/spark-writes/)
- [Iceberg Spark queries](https://iceberg.apache.org/docs/latest/spark-queries/)
- [Iceberg maintenance procedures](https://iceberg.apache.org/docs/latest/spark-procedures/)
