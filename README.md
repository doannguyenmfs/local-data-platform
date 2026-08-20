        incremental pipeline
                    │
                    ▼
                 watermark ✅
                    │
                    ▼
                idempotency ✅
                    │
                    ▼
                 backfill ✅
                    │
                    ▼
                 CURRENT
                    │
                    ▼
                late data
                    │
                    ▼
              dbt + DQ
                    │
                    ▼
          Spark + Iceberg
                    │
                    ▼
          Kafka + streaming
                    │
                    ▼
        OpenLineage + Monitoring
                    │
                    ▼
             ALERTING

## Backfill

Trigger an explicit `[start, end)` historical window without changing the
incremental watermark:

```bash
airflow dags trigger ecommerce_pipeline \
  --conf '{
    "run_mode": "backfill",
    "backfill_start": "2026-08-01T00:00:00+00:00",
    "backfill_end": "2026-08-02T00:00:00+00:00"
  }'
```

Both timestamps are required in backfill mode and `backfill_start` must be
earlier than `backfill_end`.
