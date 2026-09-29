-- Dimensión tipo de pago. PK: payment_type_id.
select
    payment_type_id,
    payment_type_name,
    payment_type_id in (1, 2) as is_paid_trip
from {{ ref('payment_types') }}
