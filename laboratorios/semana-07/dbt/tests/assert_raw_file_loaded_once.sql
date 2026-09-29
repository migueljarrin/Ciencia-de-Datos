-- Un archivo cargado dos veces duplicaría filas en RAW: cada archivo debe pertenecer a una sola ingesta.
select _source_file, count(distinct _ingestion_id) as ingestions
from {{ ref('brz_yellow_tripdata') }}
group by 1 having count(distinct _ingestion_id) > 1
