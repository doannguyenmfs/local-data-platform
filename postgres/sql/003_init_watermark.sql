INSERT INTO metadata.etl_watermark (
    pipeline_name,
    watermark_value
)
VALUES
    (
        'staging_customers',
        COALESCE(
            (SELECT MAX(updated_at) FROM public.customers),
            '1970-01-01 00:00:00+00'::TIMESTAMPTZ
        )
    ),
    (
        'staging_products',
        COALESCE(
            (SELECT MAX(updated_at) FROM public.products),
            '1970-01-01 00:00:00+00'::TIMESTAMPTZ
        )
    ),
    (
        'staging_orders',
        COALESCE(
            (SELECT MAX(updated_at) FROM public.orders),
            '1970-01-01 00:00:00+00'::TIMESTAMPTZ
        )
    ),
    (
        'staging_order_items',
        COALESCE(
            (SELECT MAX(updated_at) FROM public.order_items),
            '1970-01-01 00:00:00+00'::TIMESTAMPTZ
        )
    ),
    (
        'staging_payments',
        COALESCE(
            (SELECT MAX(updated_at) FROM public.payments),
            '1970-01-01 00:00:00+00'::TIMESTAMPTZ
        )
    )
ON CONFLICT (pipeline_name)
DO NOTHING;