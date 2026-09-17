# Level 1 — Nền tảng data platform local

## Mục tiêu của level

Level này không nhằm “chạy thật nhiều container”. Mục tiêu là tạo một môi trường
có ranh giới giống production để những bài sau có chỗ kiểm chứng: source database
khác metadata database, process điều phối khác process tính toán, dữ liệu bền
không nằm trong container filesystem và cấu hình không được hard-code vào code.

## Data platform là gì?

Một data pipeline chỉ là một đường xử lý dữ liệu. Data platform là tập hợp các
khả năng dùng chung để nhiều pipeline có thể chạy đáng tin cậy:

- lưu trữ source, staging, warehouse và lakehouse;
- lập lịch, retry và ghi nhận trạng thái;
- biến đổi, kiểm thử và công bố dữ liệu;
- truyền event và replay;
- đo health, freshness và data quality;
- quản lý cấu hình, runtime và state.

Project này gom các khả năng đó trên một máy bằng Docker Compose. Đây là
`production-shaped`: ranh giới và failure mode có thật, nhưng mỗi hệ thống chỉ
có ít node để tiết kiệm tài nguyên.

## Bốn loại state cần phân biệt

| Loại state | Ví dụ trong project | Nếu mất thì sao? |
| --- | --- | --- |
| Business data | PostgreSQL `public.*` | Mất dữ liệu nguồn |
| Pipeline state | watermark, Airflow metadata, Kafka offset | Không biết đã xử lý tới đâu |
| Analytical state | dbt marts, Iceberg snapshots | Consumer mất dữ liệu phục vụ phân tích |
| Runtime state | process/container đang chạy | Có thể tái tạo nếu state bền còn nguyên |

Container là runtime có thể thay thế. Named volume mới là nơi giữ state bền.
Vì vậy `docker compose down` chỉ dừng runtime, còn `down -v` là thao tác xóa dữ
liệu có tính phá hủy.

## Vì sao dùng hai PostgreSQL container?

`postgres` chứa ecommerce source, staging, dbt output và Iceberg JDBC catalog.
`airflow-postgres` chỉ chứa metadata nội bộ của Airflow.

Tách hai database server giúp:

1. Airflow migration không đụng schema nghiệp vụ.
2. Backup/restore metadata scheduler độc lập với business data.
3. Quyền truy cập và vòng đời khác nhau được thể hiện rõ.
4. Khi source database lỗi, ta vẫn phân biệt được scheduler state với source.

Trong production, chúng thường còn nằm trên hai cluster/managed service khác
nhau. Local lab dùng hai container là mức tách hợp lý giữa realism và chi phí.

## Docker image, container và volume

- **Image** là runtime template bất biến, ví dụ `postgres:16`.
- **Container** là một process instance tạo từ image.
- **Volume** là storage có vòng đời độc lập với container.
- **Bind mount** đưa source/config từ repository vào container.

Code dùng bind mount để sửa DAG/dbt model rồi container đọc ngay. Database,
Kafka log, MinIO objects và monitoring history dùng named volume để recreate
container mà không mất state.

## Service name và localhost

Trong Docker network, service gọi nhau bằng DNS name của Compose:

```text
airflow-worker -> postgres:5432
spark-gateway  -> minio:9000
order-stream   -> kafka:19092
```

Từ máy host, người dùng gọi published port:

```text
host -> localhost:5432
host -> localhost:8080
host -> localhost:9001
```

`localhost` bên trong container trỏ vào chính container đó, không phải máy Mac
và cũng không phải container khác. Đây là lý do project có cả internal port và
host-published port trong `.env`/Compose.

## Tại sao ghim version?

Project ghim PostgreSQL, Airflow, Spark, Kafka, Iceberg, Prometheus và Grafana.
Tag `latest` có thể thay đổi runtime dù Git commit không đổi, làm build hôm nay
khác build tuần sau. Ghim version tạo reproducibility; nâng version phải là một
thay đổi có review và test.

## Cấu hình và secret

`.env.example` là contract: tên biến nào cần có và giá trị mẫu ra sao. `.env`
là instance local, không được commit. Compose inject biến vào process; Python và
dbt đọc từ environment thay vì chứa password trong source.

Điều này chưa phải secret management production. Environment variable vẫn có
thể bị người có quyền Docker inspect nhìn thấy. Production cần Vault, cloud
secret manager hoặc Kubernetes Secret kết hợp RBAC và rotation.

## Healthcheck và dependency

`depends_on` chỉ mô tả thứ tự khởi động. Với dependency quan trọng, project dùng
`condition: service_healthy` hoặc `service_completed_successfully`:

- PostgreSQL phải trả lời `pg_isready` trước khi init/consumer kết nối.
- MinIO phải healthy trước khi `minio-init` tạo bucket.
- Kafka phải healthy trước khi `kafka-init` tạo topic.
- Airflow metadata migration phải xong trước các Airflow daemon.

Healthcheck không chứng minh dữ liệu đúng; nó chỉ chứng minh endpoint cơ bản có
thể phục vụ. Data quality được kiểm tra ở dbt/Spark và freshness ở monitoring.

## Kết quả áp dụng

Sau level này ta có thể recreate runtime từ Git + `.env`, giữ dữ liệu qua lần
restart và phân biệt rõ host address với container address. Đây là nền để học
orchestration; nếu runtime boundary còn mơ hồ thì mọi lỗi network về sau sẽ bị
nhầm thành lỗi SQL hoặc lỗi tool.

## Mẹo và lỗi thường gặp

- Dùng `docker compose config` để xem cấu hình sau khi nội suy biến.
- Dùng `docker compose ps` để phân biệt running, healthy và exited init job.
- Không dùng `down -v` như cách “sửa lỗi nhanh”. Nó xóa bằng chứng cần debug.
- Không mount `.venv` của macOS vào Linux container; binary wheel khác platform.
- Nếu một URL dùng trong container có `localhost`, hãy kiểm tra lại gần như ngay
  lập tức.

## Điều kiện hoàn thành

- Compose render được và các healthcheck cốt lõi xanh.
- Dữ liệu PostgreSQL còn sau khi recreate container.
- Người học giải thích được image/container/volume và internal/published port.
- `.env` không được Git track.
