-- Dimensión hora del día (grano: hora). PK: hour_key (0-23).
with hours as (
    select row_number() over (order by seq4()) - 1 as hour_of_day
    from table(generator(rowcount => 24))
)

select
    hour_of_day                                          as hour_key,
    hour_of_day,
    lpad(hour_of_day::varchar, 2, '0') || ':00'          as hour_label,
    case
        when hour_of_day between 0 and 5   then 'Madrugada'
        when hour_of_day between 6 and 11  then 'Mañana'
        when hour_of_day between 12 and 17 then 'Tarde'
        else 'Noche'
    end                                                  as day_part,
    hour_of_day between 7 and 9
        or hour_of_day between 16 and 19                 as is_rush_hour
from hours
