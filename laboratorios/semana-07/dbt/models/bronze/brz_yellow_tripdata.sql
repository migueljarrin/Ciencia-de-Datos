{#-
  BRONZE: datos lo más cercanos posible a la fuente.
  - Mismas columnas, nombres y tipos que el parquet original (sin limpieza).
  - Metadata de linaje: archivo de origen, período (mes) del archivo,
    fecha/hora de carga e id de la ejecución de ingesta.
  Se materializa como vista: no duplica almacenamiento y siempre refleja RAW.
-#}
select
    vendorid,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    ratecodeid,
    store_and_fwd_flag,
    pulocationid,
    dolocationid,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee,
    cbd_congestion_fee,
    request_source,
    -- metadata de linaje
    split_part(_source_file, '/', -1) as _source_file,
    _source_period,
    _loaded_at,
    _ingestion_id
from {{ source('raw', 'yellow_tripdata') }}
