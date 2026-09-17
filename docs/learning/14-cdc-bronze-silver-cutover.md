# Level 14 — CDC Bronze, Silver, delete apply và ownership cutover

## 1. Capture khác apply

Debezium đưa delete vào Kafka mới chỉ là **capture**. Downstream chỉ phản ánh
delete sau khi consumer **apply** operation đó vào state mà dbt đọc.

```text
PostgreSQL WAL
   -> Debezium Avro topic                 capture
      -> cdc_bronze.customer_changes      durable evidence
         -> cdc_silver.customers          apply/fold latest state
            -> customers_current view     live keys only
               -> dbt snapshot/dim        analytical history
```

Bronze và Silver không trùng vai trò:

- Bronze trả lời “Kafka đã phát record nào, ở offset nào, payload gì?”;
- Silver trả lời “customer hiện còn sống hay đã xóa, giá trị mới nhất là gì?”;
- dbt snapshot trả lời “các business version có hiệu lực khi nào?”.

## 2. Vì sao không ghi thẳng vào staging cũ?

Trước cutover, `staging.customers` do Airflow polling sở hữu. Nếu CDC consumer
cùng ghi:

- backfill có thể hồi sinh row CDC vừa xóa;
- watermark và Kafka offset mô tả hai progress contract khác nhau;
- khó biết writer nào là source of truth;
- rollback/reconciliation không có ranh giới.

Project tạo namespace riêng, đối soát, chuyển dbt source, rồi tắt writer cũ.
Đây là ownership migration, không chỉ là sửa một câu SQL.

## 3. Bronze append-only

`cdc_bronze.customer_changes` có primary key:

```text
(kafka_topic, kafka_partition, kafka_offset)
```

Đây là identity vật lý duy nhất của Kafka record. Table giữ:

- customer key;
- operation `r/c/u/d/t` (`t` là tombstone);
- source LSN/timestamp/transaction ID;
- `before`, `after`, event key dưới JSONB;
- Kafka coordinates/timestamp và ingestion time.

`INSERT ... ON CONFLICT DO NOTHING` khiến đọc lại cùng offset không nhân đôi.
Không dùng `customer_id` làm Bronze key vì một customer hợp lệ có nhiều change.

Bronze nằm PostgreSQL trong local lab để query/reconcile đơn giản. Volume lớn
production thường chọn object-storage table format với retention/partition rõ;
semantics identity/replay không đổi.

## 4. Silver current state

`cdc_silver.customers` có một row mỗi `customer_id` và giữ metadata event đã áp:

- `r/c/u`: upsert attributes, `is_deleted=false`;
- `d`: giữ row audit nhưng đặt `is_deleted=true`;
- `t`: chỉ ghi Bronze, không apply lần hai vì event `d` đã đủ business meaning.

`customers_current` là view `WHERE NOT is_deleted`. dbt đọc view này; operator
vẫn nhìn được tombstone trong base table.

### Ordering guard

Silver chỉ update khi incoming tuple mới hơn:

```text
(incoming source_lsn, incoming kafka_offset)
    > (stored source_lsn, stored kafka_offset)
```

Customer key luôn hash vào cùng partition nên offset giữ order của key. LSN là
source ordering chính; offset phá hòa và giúp trace. Guard ngăn replay/record cũ
ghi đè state mới.

## 5. Transaction và crash window

Consumer đọc tối đa 500 messages rồi:

1. mở PostgreSQL transaction;
2. insert từng record vào Bronze;
3. chỉ nếu Bronze insert mới thì apply Silver;
4. commit PostgreSQL;
5. commit next Kafka offset của mọi partition trong batch.

Crash matrix:

| Điểm crash | Database | Kafka offset | Khi restart |
| --- | --- | --- | --- |
| trước DB commit | chưa có batch | cũ | đọc lại và apply |
| sau DB commit, trước offset commit | đã có batch | cũ | đọc lại; Bronze conflict => no-op; commit offset |
| sau cả hai commit | đã có batch | mới | tiếp tục record sau |

Đây là at-least-once + idempotent apply, không phải distributed exactly-once.
Thứ tự DB-before-offset rất quan trọng: commit offset trước có thể mất data nếu
DB commit thất bại.

## 6. Delete đi tới SCD2 như thế nào?

```text
source DELETE
  -> Debezium d + tombstone
  -> Silver is_deleted=true
  -> key biến mất khỏi customers_current
  -> dbt snapshot hard_deletes: invalidate
  -> dbt_valid_to của version mở được đóng
  -> dim_customer giữ lịch sử, không còn is_current=true
```

Không xóa lịch sử dimension. Một customer đã từng tồn tại vẫn cần để giải thích
fact cũ. Test `current_keys_match_cdc_silver` đảm bảo live keys có current
version và deleted keys không còn current; test `one_current_record` đảm bảo
không bao giờ có hơn một version mở.

Source có foreign key từ orders tới customers, nên hard-delete chỉ hợp lệ khi
không còn order tham chiếu. CI dùng customer thứ ba không có order để mô phỏng
đúng constraint thật, thay vì tạo fixture mà OLTP không thể sinh.

## 7. Cutover đã áp dụng

### Parallel/reconcile

Consumer drain initial snapshot 100.000 rows. `cdc/reconcile_customers.py` dùng
FULL OUTER JOIN và `IS DISTINCT FROM` để so:

- source/current count;
- missing/extra business key;
- mọi customer attribute;
- duplicate Kafka coordinate trong Bronze.

### Chuyển dbt

`stg_customers.sql` vẫn tồn tại như dbt interface nhưng source đổi từ
`staging.customers` sang `cdc_silver.customers_current`. Downstream `ref()`
không phải biết transport đã đổi.

### Retire polling writer

- Airflow bỏ `extract_customers`;
- commit barrier còn bốn batch candidates;
- relationship validation dùng Silver current;
- watermark row `staging_customers` được giữ nhưng `active=false` và candidate
  bị clear;
- exporter chỉ báo freshness cho watermark `active=true`.

Không drop bảng/watermark cũ ngay. Chúng là rollback evidence; quan trọng là
không còn task nào ghi, nên không còn dual writer.

Sau khi Avro path và reconciliation đã pass, runtime migration cũng retire
connector JSON `ecommerce-postgres-cdc` và drop replication slot
`ecommerce_cdc_slot`. Topic JSON cũ không bị xóa: giữ event evidence rẻ hơn
việc để slot cũ tiếp tục giữ WAL trên source. Fresh install chỉ tạo connector
Avro/slot mới nên không cần bước cleanup này.

## 8. Chạy và kiểm tra

```bash
# Khởi động consumer liên tục
docker compose --profile '*' up -d customer-cdc-materializer

# Chứng minh source và Silver giống nhau
docker compose --profile '*' run --rm cdc-reconcile

# Tạo/update/delete UUID riêng và kiểm tra Bronze + Silver
docker compose --profile '*' run --rm cdc-materialization-test

# Xử lý backlog rồi thoát (chỉ dùng khi daemon không chạy)
docker compose --profile '*' run --rm customer-cdc-once
```

Không chạy daemon và `customer-cdc-once` đồng thời với cùng group như hai tool
độc lập: Kafka sẽ rebalance partitions giữa chúng. Điều đó đúng về group
semantics nhưng không cần thiết khi vận hành local.

Query hữu ích:

```sql
select operation, count(*)
from cdc_bronze.customer_changes
group by 1 order by 1;

select is_deleted, count(*)
from cdc_silver.customers
group by 1;

select pipeline_name, active, candidate_value
from metadata.etl_watermark
order by 1;
```

## 9. Failure mode và recovery

| Triệu chứng | Kiểm tra | Recovery |
| --- | --- | --- |
| Silver lag | consumer logs/group lag | restart consumer; giữ group offsets |
| replay nhiều | Bronze conflict/replayed count | chấp nhận nếu sau crash; tìm offset commit failure |
| reconcile difference | source vs Silver key/attribute | dừng cutover/deploy, sửa consumer rồi replay có kiểm soát |
| delete không đóng dim | Silver tombstone, snapshot run | chạy dbt build; kiểm tra `hard_deletes` |
| Bronze tăng vô hạn | retention/storage policy | partition/archive theo policy; không xóa trước recovery window |
| event schema không decode | Registry/subject/ID | sửa contract/consumer trước khi advance offsets |

Consumer migration SQL idempotent, nhưng thay đổi schema lớn vẫn cần migration
version/rollback thật. Local script không thay công cụ migration production.

`CDC_CUSTOMER_CONSUMER_GROUP` là progress identity của Silver materializer,
không phải tên tùy ý đổi mỗi lần deploy. Giữ `customer-cdc-silver-v1` ổn
định để restart tiếp tục từ committed offsets. Chỉ bump sang group mới khi
cố ý rebuild/replay Silver, sau khi đã lập kế hoạch storage và cutover; group
mới với `auto.offset.reset=earliest` sẽ đọc lại toàn bộ dữ liệu Kafka còn
retention.

## 10. File cần đọc

- `postgres/sql/003_cdc_customer_layers.sql`: Bronze/Silver/current view/migration.
- `cdc/materialize_customers.py`: decode, transaction, dedupe, ordering.
- `cdc/reconcile_customers.py`: source-to-Silver proof.
- `cdc/verify_materialization.py`: create/update/delete functional proof.
- `dbt/models/staging/stg_customers.sql`: dbt source boundary sau cutover.
- `dbt/snapshots/_snapshots.yml`: delete invalidation.
- `airflow/dags/ecommerce_pipeline.py`: four-table batch ownership.
