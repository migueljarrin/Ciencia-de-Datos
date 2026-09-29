{{ config(
    incremental_strategy='delete+insert',
    unique_key='source_period'
) }}

-- SILVER (cuarentena): filas descartadas y su motivo, para auditar la limpieza
-- y conciliar conteos: bronze = silver + rechazados.
with classified as (
    {{ classify_yellow_trips() }}
)

select
    trip_key,
    rejection_reason,
    vendor_id,
    pickup_datetime,
    dropoff_datetime,
    trip_distance_miles,
    fare_amount,
    total_amount,
    source_file,
    source_period,
    bronze_loaded_at,
    current_timestamp()::timestamp_ltz as silver_processed_at
from classified
where rejection_reason is not null
