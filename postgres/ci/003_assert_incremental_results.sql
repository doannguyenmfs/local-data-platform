SET search_path TO :"dbt_schema";

DO $$
DECLARE
    fact_count BIGINT;
    daily_count BIGINT;
    jan_2_paid_sales NUMERIC(16, 2);
    stale_jan_3_count BIGINT;
    jan_4_count BIGINT;
BEGIN
    SELECT COUNT(*) INTO fact_count
    FROM fact_sales;

    SELECT COUNT(*) INTO daily_count
    FROM daily_sales;

    SELECT paid_sales INTO jan_2_paid_sales
    FROM daily_sales
    WHERE sales_date = DATE '2026-01-02';

    SELECT COUNT(*) INTO stale_jan_3_count
    FROM daily_sales
    WHERE sales_date = DATE '2026-01-03';

    SELECT COUNT(*) INTO jan_4_count
    FROM daily_sales
    WHERE sales_date = DATE '2026-01-04';

    IF fact_count <> 3 THEN
        RAISE EXCEPTION 'Expected 3 fact rows, got %', fact_count;
    END IF;

    IF daily_count <> 2 THEN
        RAISE EXCEPTION 'Expected 2 daily rows, got %', daily_count;
    END IF;

    IF jan_2_paid_sales <> 1040.00 THEN
        RAISE EXCEPTION 'Expected Jan 2 paid sales 1040.00, got %', jan_2_paid_sales;
    END IF;

    IF stale_jan_3_count <> 0 THEN
        RAISE EXCEPTION 'Expected stale Jan 3 aggregate to be deleted';
    END IF;

    IF jan_4_count <> 1 THEN
        RAISE EXCEPTION 'Expected Jan 4 aggregate to be created';
    END IF;
END $$;
