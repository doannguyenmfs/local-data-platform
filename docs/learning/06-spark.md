# Level 6 — Spark batch processing

## Khái niệm

Apache Spark là distributed compute engine. Driver lập execution plan và chia
công việc thành task; executors chạy task trên các partition dữ liệu. Spark
không phải database và cũng không tự cung cấp transaction cho một thư mục
Parquet.

Trong project này, dbt vẫn là công cụ transform tốt nhất cho PostgreSQL
warehouse. Spark được thêm để xử lý dữ liệu dạng file ở quy mô lớn và tạo nền
cho lakehouse. Không chuyển SQL sang Spark chỉ để “có Spark trong CV”.

## Kiến trúc áp dụng

```text
PostgreSQL staging
       │ JDBC
       ▼
Spark driver ──────► Spark worker/executors
       │ transform + validate
       ▼
Parquet partitioned by sales_date
       │
       └── /opt/lakehouse/parquet/sales
```

Cluster standalone gồm:

- `spark-master`: nhận application và phân bổ tài nguyên.
- `spark-worker`: cung cấp CPU/RAM và chạy executor.
- `spark-submit`: container dùng để gửi batch job.

Project ghim Spark `3.5.9`, Java 17 và Scala 2.12. Nhánh Spark 3.5 vẫn được
Iceberg duy trì, đồng thời có runtime jar tương ứng. Không dùng tag `latest` vì
image có thể thay đổi mà code không đổi.

## Cơ chế job

`spark/jobs/sales_batch.py` thực hiện:

1. Đọc `staging.orders`, `staging.order_items`, `staging.payments` qua JDBC.
2. Aggregate payment về grain một dòng trên order.
3. Join order với item, tạo grain một dòng trên `order_item_id`.
4. Tính `sales_amount`, `payment_status`, `source_loaded_at`.
5. Kiểm tra unique grain, null business key và measure âm.
6. Ghi Parquet, partition vật lý theo `sales_date`.

`fetchsize=10000` giảm số network round-trip. Project chưa partition JDBC read
vì business key là UUID; tự đặt `numPartitions` khi không có partition column
phù hợp dễ tạo nhiều full-table scan. Khi dữ liệu lớn, nên bổ sung numeric
surrogate/range column hoặc các predicates không giao nhau.

## Driver, executor, stage và shuffle trong chính job này

Driver chạy `main()`, tạo logical/physical plan và gọi action. Join/aggregate có
thể tạo shuffle: cùng `order_id` phải được đưa về cùng partition để aggregate
payment. Executor trên worker chạy task cho từng partition. `persist()` sales
dataset vì validation và write là hai action; không cache thì Spark có thể đọc
JDBC và tính lại lineage hai lần.

`persist()` không miễn phí: nó dùng memory/disk và phải được cân nhắc với dataset
size. Job kết thúc process ngay sau write nên Spark giải phóng cache khi stop;
long-running application cần `unpersist()` rõ ràng.

## Vì sao transformation là pure function riêng?

`transform_sales(orders, order_items, payments)` không tự mở JDBC và không tự
write. Nhờ đó unit test tạo DataFrame nhỏ và kiểm tra business logic trong local
Spark mà không cần PostgreSQL/MinIO. I/O nằm ở rìa, transformation nằm giữa—đây
là cấu trúc testable hơn một `main()` dài.

`validate_sales()` trả metric và raise trước write. Output không bao giờ được coi
là thành công nếu grain/null/measure invariant đã sai.

## Chạy

```bash
docker compose --profile spark up -d --build spark-master spark-worker

docker compose --profile '*' run --rm --no-deps spark-submit
```

Spark master UI: <http://localhost:8081>  
Spark worker UI: <http://localhost:8082>

Chạy unit test trong đúng Spark runtime:

```bash
docker compose --profile '*' run --rm --no-deps \
  --entrypoint /opt/spark/bin/spark-submit \
  spark-submit \
  --master 'local[2]' \
  /opt/spark/tests/test_sales_batch.py
```

## Vì sao Parquet chưa đủ production?

Parquet chỉ là file format. `mode('overwrite')` không đem lại table-level ACID,
snapshot history hay concurrent commit an toàn. Nếu job chết giữa lúc thay thế
file, reader có thể thấy trạng thái không hoàn chỉnh. Đây chính là vấn đề Level
7 giải quyết bằng Iceberg.

`mode('overwrite')` ở level này là cố ý: nó tạo baseline dễ hiểu và làm nổi bật
vấn đề transaction/concurrent reader. Đây không phải sink được daily DAG dùng
để commit production-shaped data; Iceberg job mới là sink tích hợp chính.

## Lưu ý và mẹo

- Partition theo cột thường xuyên được filter và có cardinality vừa phải.
  Partition theo UUID sẽ sinh quá nhiều thư mục/file nhỏ.
- `repartition('sales_date')` gom dữ liệu theo partition trước lúc write, nhưng
  production vẫn phải theo dõi file size và tránh một file cho mỗi record.
- Spark transformations là lazy; validation action mới thực sự kích hoạt job.
- Không log password JDBC. Hàm config chỉ báo tên biến bị thiếu.
- Image chứa JDBC driver cố định thay vì tải package mỗi lần chạy.

## Vì sao Dockerfile có nhiều build stage?

- Stage Alpine tải JDBC và Iceberg jars theo version cố định.
- Stage Maven resolve toàn bộ transitive jars của Spark Kafka connector.
- Runtime stage chỉ nhận artifacts cần thiết trên Spark image chuẩn.

Tải jar trong mỗi `spark-submit --packages` làm startup phụ thuộc internet và có
thể resolve version khác nhau. Build-time resolution tạo image reproducible,
đổi lại image lớn hơn và phải rebuild khi upgrade dependency.

## Resource decision

Local cluster chỉ có một worker 2 cores/3 GiB. Gateway jobs bị giới hạn 1 core và
2 GiB; stream dùng 1 core/1 GiB. Mục tiêu là tránh một application chiếm hết
worker, không phải tuning tối ưu throughput. Production sizing phải dựa trên
shuffle size, spill, GC, input partitions và SLA.

## Câu hỏi phỏng vấn

**Driver và executor khác nhau thế nào?**  
Driver xây DAG, lập lịch và giữ SparkSession; executor chạy task và lưu/cache
partition dữ liệu.

**Tại sao Spark có thể chậm hơn PostgreSQL với bảng nhỏ?**  
Spark có chi phí khởi động JVM, lập plan, serialization và shuffle. Distributed
compute chỉ có lợi khi workload đủ lớn hoặc cần xử lý file/lake đa nguồn.

**`repartition` và `partitionBy` khác gì?**  
`repartition` thay đổi phân bố dữ liệu khi thực thi; `partitionBy` quyết định
cấu trúc thư mục lúc ghi.

## Điều kiện hoàn thành

- Spark master và worker đăng ký thành công.
- Unit test transformation pass.
- Batch job in `SPARK_JOB_METRICS` với row count hợp lệ.
- Thư mục output có partition `sales_date=...` và đọc lại được.

## Tài liệu chính thức

- [Apache Spark downloads](https://spark.apache.org/downloads/)
- [Apache Spark Docker images](https://hub.docker.com/r/apache/spark/tags)
- [Iceberg multi-engine support](https://iceberg.apache.org/multi-engine-support/)
