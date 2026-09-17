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

## Lưu ý và mẹo

- Partition theo cột thường xuyên được filter và có cardinality vừa phải.
  Partition theo UUID sẽ sinh quá nhiều thư mục/file nhỏ.
- `repartition('sales_date')` gom dữ liệu theo partition trước lúc write, nhưng
  production vẫn phải theo dõi file size và tránh một file cho mỗi record.
- Spark transformations là lazy; validation action mới thực sự kích hoạt job.
- Không log password JDBC. Hàm config chỉ báo tên biến bị thiếu.
- Image chứa JDBC driver cố định thay vì tải package mỗi lần chạy.

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
