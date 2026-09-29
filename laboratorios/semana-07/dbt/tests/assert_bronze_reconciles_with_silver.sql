-- Conciliación: cada fila de bronze termina en silver o en cuarentena (nada se pierde ni se duplica).
with b as (select _source_period as p, count(*) n from {{ ref('brz_yellow_tripdata') }} group by 1),
s as (select source_period as p, count(*) n from {{ ref('slv_yellow_trips') }} group by 1),
r as (select source_period as p, count(*) n from {{ ref('slv_yellow_trips_rejected') }} group by 1)
select b.p, b.n as bronze_rows, coalesce(s.n,0) as silver_rows, coalesce(r.n,0) as rejected_rows
from b left join s on s.p = b.p left join r on r.p = b.p
where b.n <> coalesce(s.n,0) + coalesce(r.n,0)
