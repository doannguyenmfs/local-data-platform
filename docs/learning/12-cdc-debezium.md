# Level 12 — Change Data Capture với PostgreSQL và Debezium

## 1. Khái niệm: CDC là gì?

Change Data Capture (CDC) là cơ chế quan sát các thay đổi đã commit trong hệ
quản trị cơ sở dữ liệu và biến chúng thành một dòng sự kiện có thứ tự. Với
PostgreSQL, nguồn sự thật là Write-Ahead Log (WAL), không phải câu
`SELECT ... WHERE updated_at > watermark`.

Trong project, luồng mới là:

```text
transaction INSERT/UPDATE/DELETE
          │ commit
          ▼
PostgreSQL WAL
          │ pgoutput + logical replication slot
          ▼
Debezium PostgreSQL connector chạy trong Kafka Connect
          │ event key = customer_id
          ▼
Kafka topic ecommerce_cdc.public.customers
```

CDC không phải một lịch chạy Airflow nhanh hơn. Connector là process chạy liên
tục; mỗi commit mới có thể xuất hiện trong Kafka sau vài giây mà không chờ DAG
daily.

## 2. Vì sao cần CDC khi đã có watermark?

Polling theo `updated_at` phù hợp batch analytics và dễ vận hành, nhưng nó dựa
trên contract ứng dụng mà database không tự bảo đảm:

- mọi insert/update phải cập nhật đúng `updated_at`;
- hard delete không còn row để query;
- một record đến muộn với `updated_at` nhỏ hơn watermark có thể bị bỏ qua;
- polling liên tục phải quét index và đánh đổi latency với database load;
- nhiều thay đổi của cùng row giữa hai lần poll bị co lại thành trạng thái cuối.

CDC đọc mutation từ transaction log nên thấy cả hard delete và từng commit. Nó
phù hợp khi cần đồng bộ current state, audit change, cache invalidation hoặc
near-real-time processing. Watermark vẫn hữu ích cho batch/backfill; hai cơ chế
giải quyết hai loại workload khác nhau.

CDC không tự chữa mọi vấn đề. Nếu connector chỉ được tạo hôm nay, initial
snapshot biết trạng thái hiện tại chứ không dựng lại lịch sử update/delete đã bị
WAL xóa từ trước. Consumer vẫn phải idempotent vì crash/retry có thể phát lại
event.

## 3. Bốn thành phần cốt lõi

### 3.1 WAL và LSN

PostgreSQL ghi WAL trước khi data page được coi là durable. Mỗi vị trí WAL có
Log Sequence Number (LSN). Debezium ghi nhận LSN đã xử lý trong Kafka Connect
offset topic; sau restart nó tiếp tục từ vị trí đó.

`wal_level=logical` yêu cầu PostgreSQL ghi đủ thông tin để logical decoder tái
tạo row-level change. `max_wal_senders` giới hạn process gửi WAL và
`max_replication_slots` giới hạn số slot.

### 3.2 `pgoutput`

`pgoutput` là logical-decoding output plug-in có sẵn từ PostgreSQL 10. Nó biến
WAL nội bộ thành logical replication protocol. Ta không cài native plug-in bên
thứ ba; Debezium diễn giải stream này thành event envelope.

### 3.3 Publication

Publication trả lời “table và operation nào được phép publish?”. Project tạo
`ecommerce_cdc_publication` chỉ chứa `public.customers`. Đây là allow-list tại
database, độc lập với `table.include.list` ở connector.

Hai lớp filter là có chủ đích:

- publication ngăn replication user đọc nhầm bảng ngoài phạm vi;
- connector filter kiểm soát topic mà connector tạo.

Bootstrap đặt `publication.autocreate.mode=disabled`: DBA/bootstrap sở hữu DDL,
connector runtime không cần quyền `CREATE` database hay ownership application
table.

### 3.4 Replication slot

`ecommerce_cdc_slot` giữ vị trí WAL mà connector còn cần. Khi connector dừng,
PostgreSQL không được xóa WAL cũ hơn `restart_lsn`; nhờ vậy restart có thể catch
up mà không mất mutation.

Độ bền này có mặt trái: connector chết lâu có thể làm đầy disk PostgreSQL.
Project đặt `max_slot_wal_keep_size=1024MB`, xuất metric retained bytes và cảnh
báo ở 512 MiB. Nếu chạm giới hạn, slot có thể mất khả năng tiếp tục và cần
resnapshot/recovery có chủ đích. Không tùy tiện drop slot để xóa cảnh báo vì làm
mất điểm resume.

## 4. Initial snapshot hoạt động thế nào?

WAL thường không giữ toàn bộ lịch sử từ ngày database được tạo. Khi chưa có
offset, `snapshot.mode=initial` thực hiện:

1. mở consistent database snapshot;
2. ghi lại LSN bắt đầu;
3. đọc row hiện có và phát event `op=r` (`read`);
4. hoàn tất snapshot và lưu progress;
5. stream mutation `c/u/d` từ LSN đã ghi.

Do đó dữ liệu thay đổi trong lúc quét không bị tạo một khoảng trống giữa
“snapshot” và “stream”. Nếu process chết trước khi initial snapshot hoàn tất,
snapshot có thể chạy lại; consumer không được giả định mọi event chỉ đến đúng
một lần.

Đây không phải dbt snapshot:

| Debezium initial snapshot | dbt snapshot |
| --- | --- |
| bootstrap current rows vào event log | tạo lịch sử SCD2 trong warehouse |
| thường chạy một lần khi chưa có offset | chạy mỗi lần `dbt snapshot/build` |
| sau đó chuyển sang WAL streaming | so sánh source state giữa các lần chạy |
| event `op=r` không phải business create | version có `valid_from/valid_to` |

## 5. Event Debezium và ý nghĩa operation

Kafka Connect dùng JSON converter và giữ schema wrapper. Mỗi record ngoài cùng
có `schema` mô tả type/optionality và `payload` chứa Debezium envelope. Ví dụ
dưới đây rút gọn phần schema để tập trung vào payload:

```json
{
  "schema": {"type": "struct", "fields": ["..."]},
  "payload": {
    "before": null,
    "after": {
      "customer_id": "...",
      "first_name": "CDC",
      "last_name": "Created"
    },
    "source": {
      "schema": "public",
      "table": "customers",
      "lsn": 123456,
      "snapshot": "false"
    },
    "op": "c",
    "ts_ms": 1780000000000
  }
}
```

Schemaful JSON dễ học contract và giữ type metadata nhưng lặp schema trên từng
message nên rất lớn. Bước Schema Registry tiếp theo sẽ đưa schema ra registry và
dùng Avro/Protobuf/JSON Schema để giảm payload, kiểm tra compatibility tập trung.

| `op` | Nghĩa | `before` | `after` |
| --- | --- | --- | --- |
| `r` | row từ initial/incremental snapshot | `null` | current row |
| `c` | insert/create | `null` | row mới |
| `u` | update | tùy replica identity | row mới |
| `d` | delete | key/old row tùy replica identity | `null` |

Sau `d`, connector phát thêm tombstone: cùng Kafka key nhưng value `null`.
Delete event nói với consumer rằng business row đã bị xóa; tombstone nói với
Kafka log compaction rằng value cũ của key có thể được dọn. Đây là hai record,
không phải duplicate.

Các source table đều có primary key nên `REPLICA IDENTITY DEFAULT` đủ để event
delete có key. Nếu consumer cần toàn bộ giá trị cũ trong `before`, phải cân nhắc
`REPLICA IDENTITY FULL`; đổi lại WAL lớn hơn và write overhead cao hơn.

## 6. Topic, key, ordering và retention

Debezium mặc định đặt topic theo:

```text
<topic.prefix>.<schema>.<table>
ecommerce_cdc.public.customers
```

Key chứa `customer_id`, nên các mutation của cùng customer được Kafka hash vào
cùng partition và giữ commit order trong partition. Không có global ordering
giữa ba partition.

Broker tắt auto-create. Kafka Connect được phép tạo topic qua Admin API với hai
nhóm:

- table topic: 3 partitions, `cleanup.policy=compact`, LZ4;
- heartbeat/transaction topic mặc định: 1 partition, delete retention 7 ngày.

Local chỉ có một broker nên replication factor là 1. Production nên có ít nhất
ba broker và RF phù hợp; tăng partition sau khi consumer đã dựa vào key mapping
có thể thay đổi partition của key mới.

## 7. Kafka Connect giữ state ở đâu?

`debezium-connect` là một Kafka Connect distributed worker một node. Nó dùng ba
internal topic compacted:

- `ecommerce.connect.configs`: connector configuration;
- `ecommerce.connect.offsets`: LSN/source progress;
- `ecommerce.connect.status`: connector/task state.

PostgreSQL slot và Kafka offset là hai nửa của resume contract. Xóa Kafka offset
nhưng giữ slot, hoặc xóa slot nhưng giữ offset, có thể làm connector không tìm
được điểm tiếp tục hợp lệ. Backup/recovery phải coi chúng là state phối hợp.

PostgreSQL connector luôn có một source task vì một WAL stream cần giữ transaction
order. `tasks.max=10` không làm connector này nhanh gấp mười; muốn scale phải
chia connector/database/table scope có chủ đích và dùng slot riêng.

## 8. Project đã áp dụng như thế nào?

### `docker-compose.yml`

- PostgreSQL bật logical WAL và safety cap.
- `debezium-connect` chạy image 3.6.2.Final, tương thích dòng Kafka 4.3.
- `cdc-bootstrap` là init job hữu hạn.
- `cdc-smoke-test` là tool chạy tay, không phải daemon.

Hai tool COPY code vào image để CI/deployment không phụ thuộc host, đồng thời
Compose bind-mount `cdc/` read-only để local sửa và thử lại không phải rebuild
Python image. Mount này là developer convenience, không phải nơi giữ runtime
state.

### `cdc/bootstrap.py`

Bootstrap kết nối bằng admin local, tạo/rotate `cdc_reader`, grant đúng
`CONNECT`, `USAGE`, `SELECT`, `REPLICATION`, sau đó create/alter publication.
Tên identifier được quote bằng `psycopg.sql.Identifier`. PostgreSQL utility DDL
không nhận bind placeholder ở `PASSWORD`, nên password được quote bằng
`psycopg.sql.Literal`; code không nối chuỗi SQL thủ công.

Sau đó script dùng `PUT /connectors/ecommerce-postgres-cdc/config`. PUT làm thao
tác convergent: chạy lại cập nhật config hiện có thay vì lỗi “already exists”.
Job chỉ success khi connector và source task đều `RUNNING`.

Kafka Connect distributed REST có một khoảng hội tụ ngắn: `PUT` config có thể
thành công nhưng `/status` tạm trả 404 trước khi worker đọc config-log offset
mới. Bootstrap retry riêng 404 này; lỗi HTTP khác vẫn fail ngay để không che lỗi
thật.

REST `/config` có thể trả lại `database.password`. Local lab publish port 8083
để học, nhưng production phải giới hạn network/RBAC và dùng Kafka Connect
ConfigProvider hoặc secret manager thay vì lưu secret literal trong config topic.

### `cdc/verify_cdc.py`

Smoke test lấy metadata và concrete high watermark của mọi partition trước, rồi
assign đúng các offset số đó và dùng một UUID riêng để:

1. insert customer;
2. commit;
3. update và commit;
4. delete và commit;
5. đợi đúng chuỗi `c,u,d` và tombstone.

Test xóa row của chính nó nên không làm bẩn source current state. UUID giúp lọc
khỏi initial snapshot/event khác đang chạy.

Test không dùng `subscribe()` vì group assignment và `latest` offset reset là
bất đồng bộ: partition có thể đã hiện trong assignment nhưng position cuối chưa
resolve. Mutation rất nhanh có thể rơi đúng race đó. Explicit tail assignment
phù hợp one-process diagnostic; application consumer thật vẫn dùng group. Chỉ
assign symbolic `OFFSET_END` vẫn có race vì client có thể resolve sentinel này ở
lần poll sau mutation; concrete high watermark mới đóng khe hở của bài test.

### Observability

Exporter phân biệt HTTP process sống với connector thực sự chạy. Các metric mới:

- `cdc_connector_up`;
- `cdc_replication_slot_active`;
- `cdc_replication_slot_retained_bytes`.

Prometheus cảnh báo connector/task không RUNNING, slot inactive và WAL retained
vượt 512 MiB. Đây là ba failure mode quan trọng hơn việc chỉ ping port 8083.

## 9. Vì sao CDC chưa ghi thẳng vào `staging.customers`?

Hiện `staging.customers` do Airflow watermark extractor sở hữu. Nếu một consumer
CDC cùng upsert/delete vào đó, hai writer có semantics khác nhau:

- batch có candidate/commit barrier theo ngày;
- CDC commit liên tục theo LSN;
- backfill batch có thể hồi sinh row CDC vừa xóa;
- dbt build có thể thấy state giữa một CDC transaction group.

Vì vậy Level 12 dừng ở durable raw change log. Bước Bronze/Silver tiếp theo sẽ:

1. append nguyên envelope vào Bronze;
2. dedupe bằng source coordinates/event identity;
3. materialize Silver current state bằng operation + LSN;
4. chuyển dbt source/ownership khỏi staging batch sau khi đối soát;
5. lúc đó mới retire customer watermark extractor.

Đây là migration theo “parallel run → reconcile → cut over”, không phải thiếu
delete handling. Hard delete đã được capture; chưa được quyền áp dụng vào bảng
batch hiện hữu.

## 10. Chạy và kiểm tra

Khởi động CDC cùng streaming stack:

```bash
docker compose \
  --profile streaming \
  --profile spark \
  --profile lakehouse \
  up -d --build postgres kafka kafka-init debezium-connect cdc-bootstrap
```

Kiểm tra worker/connector:

```bash
curl --fail http://localhost:8083/connectors/ecommerce-postgres-cdc/status

docker compose exec -T postgres psql \
  -U "$ECOMMERCE_POSTGRES_USER" \
  -d "$ECOMMERCE_POSTGRES_DB" \
  -c "select slot_name, active, restart_lsn, confirmed_flush_lsn from pg_replication_slots;"
```

Chạy bằng chứng end-to-end:

```bash
docker compose --profile '*' run --rm --no-deps cdc-smoke-test
```

Kết quả đúng:

```text
CDC smoke test passed: operations=c,u,d; delete_tombstone=true; ...
```

Đọc raw event để học cấu trúc:

```bash
docker compose exec -T kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:19092 \
  --topic ecommerce_cdc.public.customers \
  --from-beginning \
  --property print.key=true \
  --property key.separator=' | '
```

## 11. Failure mode và cách xử lý

| Hiện tượng | Nguyên nhân thường gặp | Hành động đúng |
| --- | --- | --- |
| connector `FAILED` | privilege/publication/config sai | đọc task trace, sửa bootstrap/config, chạy lại bootstrap |
| slot inactive | Connect/task dừng | khôi phục connector trước khi WAL chạm cap |
| retained WAL tăng | consumer không xác nhận LSN | điều tra Connect/Kafka, không drop slot vội |
| duplicate event | retry trước khi offset commit | consumer dedupe/idempotent bằng key + source position |
| initial snapshot lâu | table lớn, source I/O thấp | theo dõi snapshot metric, incremental snapshot/cutover plan |
| topic không tạo | Connect topic creation/RF sai | kiểm tra worker config và connector topic group |
| delete thiếu old columns | replica identity DEFAULT | dùng key để delete hoặc cân nhắc FULL có đo overhead |

`max_slot_wal_keep_size` là guardrail, không phải recovery strategy. Production
cần disk alert sớm, connector HA, backup và quy trình resnapshot được thử trước.

## 12. Mẹo thiết kế và câu hỏi phỏng vấn

**CDC có exactly-once không?**  Không nên tuyên bố chung chung. Debezium và Kafka
Connect lưu offset để resume, nhưng crash boundary có thể dẫn đến replay.
Consumer/sink vẫn cần stable key, dedupe hoặc idempotent merge.

**Tại sao không dùng trigger ghi audit table?** Trigger nằm trên write path ứng
dụng, tăng coupling/latency và phải được gắn đúng lên mọi table. Log-based CDC
đọc transaction log sau commit và ít xâm lấn application schema hơn.

**Publication khác slot thế nào?** Publication chọn dữ liệu được phát; slot nhớ
subscriber đã đọc tới đâu và giữ WAL cần thiết.

**LSN có phải business event ID không?** Không. LSN là vị trí database log,
hữu ích cho ordering/dedupe kỹ thuật nhưng không thay semantic event ID khi event
được phát lại qua hệ thống khác.

**Vì sao giữ full Debezium envelope thay vì SMT unwrap ngay?** Envelope giữ
`before`, `after`, `op`, source LSN, transaction/snapshot metadata. Bronze cần
audit/replay; unwrap quá sớm làm mất context. Silver có thể flatten sau.

**Snapshot `r` có phải insert không?** Nó có thể tạo current state giống insert,
nhưng nghĩa provenance khác. Audit consumer không nên đổi `r` thành business
“customer created”.

## 13. File cần đọc

- `docker-compose.yml`: WAL flags, Connect worker, bootstrap/smoke services.
- `cdc/bootstrap.py`: privilege, publication, connector contract.
- `cdc/verify_cdc.py`: bằng chứng create/update/delete/tombstone.
- `monitoring/platform_exporter.py`: connector và slot metrics.
- `monitoring/prometheus/alerts.yml`: operational thresholds.

## 14. Tài liệu chính thức

- [Debezium PostgreSQL connector](https://debezium.io/documentation/reference/stable/connectors/postgresql.html)
- [Debezium topic auto-creation](https://debezium.io/documentation/reference/3.2/configuration/topic-auto-create-config.html)
- [Debezium 3.6 release notes](https://debezium.io/releases/3.6/release-notes)
- [PostgreSQL logical decoding](https://www.postgresql.org/docs/16/logicaldecoding.html)
- [PostgreSQL replication slots](https://www.postgresql.org/docs/16/logicaldecoding-explanation.html#LOGICALDECODING-REPLICATION-SLOTS)

## 15. Điều kiện hoàn thành Level 12

- PostgreSQL chạy `wal_level=logical` và có WAL retention guardrail.
- Runtime connector không dùng PostgreSQL superuser.
- Publication chỉ chứa table được phép capture.
- Connector và task RUNNING; slot active.
- Initial snapshot tạo `r`, mutation mới tạo `c/u/d`.
- Delete có tombstone và smoke test tự dọn source row.
- Connector/slot/WAL retention có metric và alert.
- Batch staging chưa bị biến thành multi-writer ngoài ý muốn.
