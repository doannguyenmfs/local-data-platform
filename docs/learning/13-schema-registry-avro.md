# Level 13 — Schema Registry, Avro và compatibility

## 1. Khái niệm

Kafka chỉ lưu byte. Broker không biết byte đó có field nào, kiểu gì, producer
mới có phá consumer cũ hay không. Schema Registry bổ sung một control plane cho
event contract:

```text
producer schema ──register/lookup──► Schema Registry
       │                                  │ schema id
       └── magic byte + schema id + Avro payload ──► Kafka
                                                     │
consumer ◄──lookup schema by id──────────────────────┘
```

Message không lặp toàn bộ schema như schemaful JSON. Nó mang schema ID nhỏ;
consumer lấy schema từ Registry rồi cache để deserialize.

## 2. Tại sao project cần nó?

CDC envelope có nhiều metadata và nested record. Khi schema nằm trong từng JSON
message, payload lớn và không có cổng tập trung chặn breaking change. Với
Registry:

- schema được version hóa;
- Avro kiểm tra cấu trúc/kiểu khi serialize và deserialize;
- compatibility rule chặn producer đăng version không đọc được dữ liệu cũ;
- consumer có thể đọc backlog tạo bởi nhiều schema version;
- subject/version trở thành audit trail của event contract.

Registry không thay business validation. Nó biết `email` là string, không biết
email có hợp lệ hay `price` phải dương. Data test/application rule vẫn cần.

## 3. Cơ chế trong project

### 3.1 Apicurio Registry và KafkaSQL

Service `schema-registry` chạy Apicurio `3.2.5`. Backend KafkaSQL ghi mọi thay
đổi Registry vào topic `ecommerce.registry.journal.v3`:

- `cleanup.policy=delete`;
- `retention.ms=-1`;
- `retention.bytes=-1`;
- một partition để giữ thứ tự journal.

Topic này không phải business event topic. Nó là log nguồn để Registry dựng lại
state sau restart; hết hạn record sẽ làm mất lịch sử contract. Local chỉ có RF=1
vì một broker; production cần replication và backup phù hợp.

Apicurio có hai API được dùng:

- native v2 `/apis/registry/v2`: Debezium Avro converter register/lookup;
- Confluent-compatible v7 `/apis/ccompat/v7`: Python client deserialize và
  contract test.

Application API chạy port container `8080`; health management chạy `9000`.
Nhầm hai port tạo healthcheck `404` dù service vẫn sống.

### 3.2 Avro converter ở connector, không ở toàn worker

Kafka Connect worker vẫn giữ JSON converter mặc định để connector tương lai
không bị đổi ngầm. Riêng connector customer khai báo:

```text
key/value.converter = Apicurio AvroConverter
as-confluent = true
use-id = contentId
auto-register = true
find-latest = true
```

`as-confluent=true` tạo wire format magic-byte + schema-id mà
`confluent-kafka` Python client đọc được qua ccompat API. Connector mới dùng
prefix/slot version hóa:

- connector `ecommerce-postgres-cdc-avro`;
- slot `ecommerce_cdc_avro_slot`;
- topic `ecommerce_cdc_avro.public.customers`.

Đây là migration song song an toàn khỏi JSON connector cũ. Đổi format tại chỗ
trong cùng topic sẽ trộn hai wire format và làm consumer khó phân biệt.

### 3.3 Subject và schema reference

Debezium/Apicurio tạo:

- `...customers-key`: record khóa `customer_id`;
- `...customers.Value`: record cột của table customer;
- `...customers-value`: envelope `before/after/source/op/transaction`;
- các reusable subject như Debezium `Source` và transaction block.

Envelope tham chiếu `customers.Value`. Khi thêm/đổi cột PostgreSQL, row subject
là contract thay đổi trực tiếp; envelope chỉ tiếp tục tham chiếu version đó.

### 3.4 Tại sao chọn `BACKWARD_TRANSITIVE`?

Backward nghĩa là schema consumer mới đọc được data do schema cũ tạo. Transitive
nghĩa là so với mọi version cũ, không chỉ version ngay trước:

```text
V4 phải compatible với V1, V2 và V3
```

Điều này phù hợp Kafka retention/backlog và consumer deploy lệch nhịp. Ví dụ:

- thêm optional field có default thường an toàn;
- đổi trực tiếp `string -> long` là breaking;
- xóa field bắt buộc hoặc thêm field không default thường breaking.

Project còn bật `VALIDITY=FULL` để schema Avro sai cú pháp/cấu trúc bị từ chối
trước compatibility.

## 4. Compatibility test hoạt động thế nào?

`schema_registry/verify_contract.py` là read-only:

1. đợi key/row/envelope subject thật do Debezium đăng;
2. xác minh global rule là `BACKWARD_TRANSITIVE`;
3. gửi current key/row schema tới compatibility endpoint và yêu cầu pass;
4. tạo bản copy đổi field đầu sang type không tương thích;
5. yêu cầu Registry trả `is_compatible=false`;
6. không đăng breaking schema thành version mới.

Apicurio 3.2.x ccompat endpoint không tự parse được top-level envelope có
cross-subject references khi compatibility-check độc lập (`422`). Test không
che lỗi này: nó xác minh envelope tồn tại và reference đúng row subject, rồi
positive/negative-check key và row contract—hai schema thay đổi khi database key
hoặc column thay đổi. Smoke test Avro sau đó chứng minh envelope thật deserialize
được.

## 5. Cách kiểm tra

```bash
docker compose --profile '*' run --rm schema-contract-test
docker compose --profile '*' run --rm cdc-smoke-test

curl --fail \
  http://localhost:8084/apis/ccompat/v7/config
curl --fail \
  http://localhost:8084/apis/ccompat/v7/subjects
```

Kết quả đúng gồm:

```text
Schema contract test passed: compatibility=BACKWARD_TRANSITIVE ...
CDC smoke test passed: operations=c,u,d; delete_tombstone=true ...
```

## 6. Failure mode và mẹo

| Hiện tượng | Nguyên nhân | Hành động |
| --- | --- | --- |
| Registry healthy 404 | gọi health trên API port 8080 | dùng management port 9000 trong container |
| Python import thiếu `certifi/httpx` | chỉ cài core client | cài `confluent-kafka[avro]` |
| connector fail khi đổi column | Registry chặn incompatible schema | rollback DDL hoặc version topic/consumer có migration |
| consumer không decode | URL/schema ID/wire format lệch | kiểm tra ccompat URL và `as-confluent` |
| Registry mất state | journal hết hạn/mất Kafka | giữ infinite retention + replicated/backup Kafka production |

Mẹo production: pre-register schema trong CI trước deploy giảm khả năng connector
chết lần đầu gặp DDL mới. Auto-register tiện cho local CDC nhưng production nên
có owner và change approval rõ.

## 7. File cần đọc

- `docker-compose.yml`: Registry, journal topic, health và dependencies.
- `cdc/bootstrap.py`: connector-level Avro converter contract.
- `schema_registry/verify_contract.py`: positive/negative compatibility proof.
- `cdc/verify_cdc.py`: deserialize event thật bằng registry schema ID.
- `kafka/producer/requirements.txt`: client extras cần cho Registry/Avro.

## 8. Tài liệu chính thức

- [Debezium Avro serialization](https://debezium.io/documentation/reference/stable/configuration/avro.html)
- [Apicurio Registry Docker install](https://www.apicur.io/registry/docs/apicurio-registry/3.2.x/getting-started/assembly-installing-registry-docker.html)
- [Apicurio compatibility modes](https://www.apicur.io/registry/docs/apicurio-registry/3.3.x/getting-started/assembly-registry-compatibility-modes.html)
- [Schema lifecycle best practices](https://www.apicur.io/registry/docs/apicurio-registry/3.3.x/getting-started/assembly-schema-lifecycle-best-practices.html)
