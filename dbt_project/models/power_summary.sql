-- models/power_summary.sql
select
    site,
    avg(power_kw) as avg_power_output
from {{ source('energy', 'power_output_cleaned') }}
group by site