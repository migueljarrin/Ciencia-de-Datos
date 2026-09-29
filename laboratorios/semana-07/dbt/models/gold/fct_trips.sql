{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='source_period',
    cluster_by=['pickup_date_key']
) }}

-- HECHOS: un registro por viaje de taxi amarillo válido (grano = viaje).
-- FKs a dim_date (x2), dim_time, dim_location (x2, role-playing),
-- dim_vendor, dim_payment_type y dim_rate_code.
select
    -- PK
    t.trip_key,

    -- FKs
    to_number(to_char(t.pickup_datetime, 'YYYYMMDD'))   as pickup_date_key,
    to_number(to_char(t.dropoff_datetime, 'YYYYMMDD'))  as dropoff_date_key,
    hour(t.pickup_datetime)                             as pickup_hour_key,
    t.pickup_location_id,
    t.dropoff_location_id,
    t.vendor_id,
    t.payment_type_id,
    t.rate_code_id,

    -- atributos degenerados
    t.pickup_datetime,
    t.dropoff_datetime,
    t.is_store_and_forward,
    t.request_source,
    t.source_period,

    -- métricas
    t.passenger_count,
    t.trip_distance_miles,
    t.trip_duration_minutes,
    t.fare_amount,
    t.extra_amount,
    t.mta_tax_amount,
    t.tip_amount,
    t.tolls_amount,
    t.improvement_surcharge_amount,
    t.congestion_surcharge_amount,
    t.airport_fee_amount,
    t.cbd_congestion_fee_amount,
    t.total_amount,

    t.silver_processed_at,
    current_timestamp()::timestamp_ltz                  as gold_processed_at
from {{ ref('slv_yellow_trips') }} t
where {{ periods_to_process(ref('slv_yellow_trips'), 'source_period', 'silver_processed_at', 'silver_processed_at') }}
