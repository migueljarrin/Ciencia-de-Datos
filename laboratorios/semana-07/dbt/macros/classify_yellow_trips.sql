{#-
  Estandariza y clasifica cada fila de bronze. Lo usan slv_yellow_trips
  (filas válidas) y slv_yellow_trips_rejected (cuarentena), así ambas
  comparten exactamente las mismas reglas. Justificación en README.md.
-#}
{% macro classify_yellow_trips() -%}
with bronze as (

    select *
    from {{ ref('brz_yellow_tripdata') }}
    where {{ periods_to_process(ref('brz_yellow_tripdata'), '_source_period', '_loaded_at', 'bronze_loaded_at') }}

),

standardized as (

    select
        -- Tipos y nombres consistentes (snake_case, unidades explícitas).
        -- Códigos fuera de los catálogos oficiales -> miembro "desconocido".
        coalesce(v.vendor_id, -1)                                           as vendor_id,
        b.tpep_pickup_datetime::timestamp_ntz                                as pickup_datetime,
        b.tpep_dropoff_datetime::timestamp_ntz                               as dropoff_datetime,
        case when b.passenger_count between 1 and {{ var('max_passengers') }}
             then b.passenger_count::integer end                             as passenger_count,
        round(b.trip_distance, 2)::number(10, 2)                             as trip_distance_miles,
        coalesce(r.rate_code_id, 99)                                         as rate_code_id,
        case upper(trim(b.store_and_fwd_flag))
             when 'Y' then true when 'N' then false end                      as is_store_and_forward,
        coalesce(pu.locationid, 264)                                         as pickup_location_id,
        coalesce(dz.locationid, 264)                                         as dropoff_location_id,
        coalesce(p.payment_type_id, 5)                                       as payment_type_id,
        round(b.fare_amount, 2)::number(10, 2)                               as fare_amount,
        round(b.extra, 2)::number(10, 2)                                     as extra_amount,
        round(b.mta_tax, 2)::number(10, 2)                                   as mta_tax_amount,
        round(b.tip_amount, 2)::number(10, 2)                                as tip_amount,
        round(b.tolls_amount, 2)::number(10, 2)                              as tolls_amount,
        round(b.improvement_surcharge, 2)::number(10, 2)                     as improvement_surcharge_amount,
        round(coalesce(b.congestion_surcharge, 0), 2)::number(10, 2)         as congestion_surcharge_amount,
        round(coalesce(b.airport_fee, 0), 2)::number(10, 2)                  as airport_fee_amount,
        round(coalesce(b.cbd_congestion_fee, 0), 2)::number(10, 2)           as cbd_congestion_fee_amount,
        round(b.total_amount, 2)::number(10, 2)                              as total_amount,
        coalesce(nullif(upper(trim(b.request_source)), ''), 'NOT_REPORTED')  as request_source,
        -- linaje
        b._source_file                                                       as source_file,
        b._source_period                                                     as source_period,
        b._loaded_at                                                         as bronze_loaded_at
    from bronze b
    left join {{ ref('vendors') }}          v  on v.vendor_id = b.vendorid
    left join {{ ref('rate_codes') }}       r  on r.rate_code_id = b.ratecodeid
    left join {{ ref('payment_types') }}    p  on p.payment_type_id = b.payment_type
    left join {{ ref('taxi_zone_lookup') }} pu on pu.locationid = b.pulocationid
    left join {{ ref('taxi_zone_lookup') }} dz on dz.locationid = b.dolocationid

),

keyed as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'vendor_id', 'pickup_datetime', 'dropoff_datetime', 'passenger_count',
            'trip_distance_miles', 'rate_code_id', 'is_store_and_forward',
            'pickup_location_id', 'dropoff_location_id', 'payment_type_id',
            'fare_amount', 'extra_amount', 'mta_tax_amount', 'tip_amount', 'tolls_amount',
            'improvement_surcharge_amount', 'congestion_surcharge_amount',
            'airport_fee_amount', 'cbd_congestion_fee_amount', 'total_amount', 'request_source'
        ]) }} as trip_key,
        *,
        datediff('second', pickup_datetime, dropoff_datetime) / 60.0 as trip_duration_minutes_raw
    from standardized

)

select
    *,
    -- Primera regla que falla = motivo de rechazo (NULL = fila válida).
    case
        when pickup_datetime is null or dropoff_datetime is null
            then 'missing_timestamp'
        when date_trunc('month', pickup_datetime)::date <> source_period
            then 'pickup_outside_file_period'
        when dropoff_datetime < pickup_datetime
            then 'negative_duration'
        when trip_duration_minutes_raw > {{ var('max_trip_hours') }} * 60
            then 'duration_too_long'
        when trip_distance_miles < 0 or trip_distance_miles > {{ var('max_trip_distance_miles') }}
            then 'invalid_distance'
        when fare_amount < 0 or total_amount < 0
            then 'negative_amount'
        when total_amount > {{ var('max_total_amount') }}
            then 'amount_outlier'
        when row_number() over (partition by source_period, trip_key order by bronze_loaded_at desc) > 1
            then 'duplicate'
    end as rejection_reason
from keyed
{%- endmacro %}
