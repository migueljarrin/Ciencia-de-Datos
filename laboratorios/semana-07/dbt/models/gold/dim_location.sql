-- Dimensión zona de taxi (role-playing: origen y destino). PK: location_id.
select
    locationid::integer                                        as location_id,
    case when borough in ('N/A', 'Unknown') or borough is null
         then 'Desconocido' else borough end                   as borough,
    case when zone in ('N/A') or zone is null
         then 'Desconocido' else zone end                      as zone_name,
    case when service_zone in ('N/A') or service_zone is null
         then 'Desconocido' else service_zone end              as service_zone,
    zone ilike '%airport%'                                     as is_airport,
    locationid in (264, 265)                                   as is_unknown_or_outside_nyc
from {{ ref('taxi_zone_lookup') }}
