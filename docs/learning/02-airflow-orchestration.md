# Level 2 — Airflow orchestration và dependency graph

## Airflow giải quyết vấn đề gì?

Airflow là workflow orchestrator, không phải compute engine và không phải nơi
lưu business data. Nó trả lời bốn câu hỏi:

1. Khi nào workflow nên chạy?
2. Task nào phụ thuộc task nào?
3. Task thất bại thì retry/timeout ra sao?
4. Run nào đã thành công, thất bại hoặc đang chờ?

SQL chạy ở PostgreSQL, dbt compile/execute model, Spark xử lý phân tán và Kafka
giữ event. Airflow chỉ điều phối các hệ thống đó.

## Vì sao Airflow có nhiều container?

Project dùng `CeleryExecutor`, vì vậy các trách nhiệm được tách thành process:

| Container | Trách nhiệm | Vì sao tách? |
| --- | --- | --- |
| `airflow-api-server` | UI và REST API | Web traffic không chặn scheduler |
| `airflow-scheduler` | tạo task instance đến hạn | Scheduler không trực tiếp chạy task |
| `airflow-dag-processor` | parse file DAG | Lỗi/chi phí parse tách khỏi scheduler loop |
| `airflow-worker` | thực thi task | Có thể scale worker độc lập |
| `airflow-triggerer` | chờ async/deferred event | Không giữ worker slot khi chờ I/O |
| `airflow-init` | migrate metadata DB rồi thoát | Init có điểm kết thúc rõ ràng |

Redis là Celery broker; `airflow-postgres` giữ DAG run, task instance, connection
và result backend. Một worker local là đủ để học nhưng không tạo HA.

## Đọc `ecommerce_pipeline.py` theo từng vùng

1. **Constants/config**: connection id, đường dẫn dbt, gateway URL.
2. **`extract_incremental`**: primitive dùng lại cho năm bảng.
3. **DAG metadata**: schedule daily, `catchup=False`, `max_active_runs=1`.
4. **Tasks**: validate config, extract, validate data, downstream sinks, commit.
5. **Dependency graph**: phần cuối file nối task object bằng `>>`.

Không đặt network/database call ở module top-level. DAG processor import file
thường xuyên; side effect lúc import sẽ chạy khi parse chứ không phải khi task
được schedule.

## Vì sao extract song song nhưng validate nối tiếp?

Các bảng batch không phụ thuộc nhau trong thao tác copy nên được fan-out để
giảm wall-clock time. Customer do CDC Silver sở hữu liên tục. Validation chỉ
chạy sau khi cả bốn extract hoàn tất;
`validate_relationships` chạy tiếp để chắc các foreign-key logical đã đầy đủ.

```text
validate config
      │
      ├── products  ─┐
      ├── orders    ─┼── count (gồm Silver customer) ── relationships
      ├── items     ─┤
      └── payments  ─┘
```

Nếu nối extract thành chuỗi, pipeline chậm không cần thiết. Nếu cho downstream
chạy ngay sau từng extract, dbt/Spark có thể đọc một batch nửa đầy.

## Retry, timeout và idempotency

Retry không tự động an toàn. Nó chỉ an toàn khi side effect có thể lặp lại:

- staging dùng `INSERT ... ON CONFLICT DO UPDATE`;
- dbt/Iceberg dùng stable unique key và merge;
- Kafka dùng deterministic event id;
- watermark commit chỉ ở barrier cuối.

`retries` xử lý lỗi tạm thời; `execution_timeout` ngăn task treo vô hạn. Business
logic sai sẽ thất bại lại sau retry, vì vậy retry không thay validation.

## Schedule, logical date và backfill

`schedule='@daily'` tạo cadence một lần mỗi ngày. `catchup=False` tránh Airflow
tự tạo hàng trăm historical run từ `start_date` trong local lab. Backfill được
thực hiện bằng parameter `run_mode`, `backfill_start`, `backfill_end` vì project
cần timestamp chính xác tới giây, không chỉ logical date theo ngày.

Hai cửa sổ luôn half-open `[start, end)`. Ví dụ `[00:00, 01:00)` và
`[01:00, 02:00)` không cùng nhận record đúng 01:00.

## Tại sao `max_active_runs=1`?

Metadata watermark hiện là một con trỏ trên mỗi pipeline. Hai DAG run đồng thời
có thể cùng đọc watermark cũ, ghi candidate cạnh tranh và làm commit boundary
khó reasoning. Giới hạn một active run là mutex đơn giản, phù hợp daily local
pipeline. Scale production có thể dùng batch/run table hoặc compare-and-swap để
cho phép concurrency có kiểm soát.

## TaskFlow API và XCom

Decorator `@task` biến Python function thành task definition. Gọi function ở
phần dựng DAG không thực thi business code; nó tạo node. Project không truyền
dataset lớn qua XCom vì XCom nằm trong metadata DB và dành cho metadata nhỏ.
Dữ liệu lớn đi qua PostgreSQL, Kafka hoặc object storage.

## Maintenance DAG riêng

`lakehouse_maintenance` có SLA, retry và failure impact khác ingest. Nếu compact
file được nhét vào cuối daily ingest, một lỗi housekeeping sẽ làm business batch
trông như thất bại và kéo dài critical path. Vì vậy maintenance chạy weekly ở
DAG riêng.

`ecommerce_reset_other_schema` là DAG quản trị destructive cho local lab, không
thuộc daily flow. Nó không được schedule tự động để tránh reset nhầm.

## Kết quả áp dụng

Graph cho thấy rõ dữ liệu chỉ được commit watermark sau ba downstream branch.
UI Airflow trở thành audit trail: biết task nào chạy, retry lần mấy, log gì và
barrier nào chưa qua.

## Mẹo đọc và debug

- Graph view để hiểu dependency; Grid view để hiểu lịch sử run.
- Xem log của task thất bại đầu tiên, không chỉ task downstream bị upstream_failed.
- Parse success chỉ chứng minh DAG hợp lệ, không chứng minh task runtime có đủ
  dependency/network.
- Clear task có side effect chỉ khi sink idempotent.
- Một long-running stream không nên là Airflow task; nó phải do service manager
  duy trì.

## Điều kiện hoàn thành

- DAG parse không side effect.
- Extract fan-out/fan-in đúng.
- Retry một task không nhân đôi grain.
- Backfill validation chặn cửa sổ thiếu hoặc ngược.
- Maintenance và destructive reset không nằm trên daily critical path.
