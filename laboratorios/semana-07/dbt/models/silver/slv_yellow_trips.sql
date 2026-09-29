{{ config(
    incremental_strategy='delete+insert',
    unique_key='source_period',
    cluster_by=['source_period']
) }}

-- SILVER: viajes limpios, tipados, deduplicados y estandarizados.
-- Grano: un viaje válido y único.
with classified as (
    {{ classify_yellow_trips() }}
)

select
    trip_key,
    vendor_id,
    pickup_datetime,
    dropoff_datetime,
    round(trip_duration_minutes_raw, 2)::number(10, 2) as trip_duration_minutes,
    passenger_count,
    trip_distance_miles,
    rate_code_id,
    is_store_and_forward,
    pickup_location_id,
    dropoff_location_id,
    payment_type_id,
    fare_amount,
    extra_amount,
    mta_tax_amount,
    tip_amount,
    tolls_amount,
    improvement_surcharge_amount,
    congestion_surcharge_amount,
    airport_fee_amount,
    cbd_congestion_fee_amount,
    total_amount,
    request_source,
    source_file,
    source_period,
    bronze_loaded_at,
    current_timestamp()::timestamp_ltz as silver_processed_at
from classified
where rejection_reason is null
