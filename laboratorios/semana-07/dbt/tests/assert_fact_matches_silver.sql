-- Todas las filas de silver llegan a la tabla de hechos.
select * from (
    select (select count(*) from {{ ref('slv_yellow_trips') }}) as silver_rows,
           (select count(*) from {{ ref('fct_trips') }}) as fact_rows
) where silver_rows <> fact_rows
