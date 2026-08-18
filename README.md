                 CURRENT
                    │
                    ▼
        staging → analytics        
                    │
                    ▼
              SCD Type 2
                    │
                    ▼
              dbt + DQ
                    │
                    ▼
          incremental + backfill
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