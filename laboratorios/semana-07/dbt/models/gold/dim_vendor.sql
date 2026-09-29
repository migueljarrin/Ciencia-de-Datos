-- Dimensión proveedor tecnológico (TPEP). PK: vendor_id (-1 = desconocido).
select vendor_id, vendor_name
from {{ ref('vendors') }}
