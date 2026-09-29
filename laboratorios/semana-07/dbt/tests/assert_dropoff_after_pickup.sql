-- Consistencia temporal: ningún viaje termina antes de empezar.
select trip_key from {{ ref('fct_trips') }} where dropoff_datetime < pickup_datetime
