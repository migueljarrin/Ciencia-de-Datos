-- Dimensión tarifa (RatecodeID). PK: rate_code_id (99 = desconocido).
select
    rate_code_id,
    rate_code_name,
    rate_code_id in (2, 3) as is_airport_rate
from {{ ref('rate_codes') }}
