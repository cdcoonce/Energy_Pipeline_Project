{{ config(
    materialized='table',
    post_hook="COPY (SELECT * FROM {{ this }}) TO '../data/exports/power_trends_by_day.csv' (HEADER, DELIMITER ',')"
) }}

with power_location as (
    select * from {{ ref('power_with_locations') }}
)

select
    date(timestamp) as date,
    location as state,
    sum(power_kw) as total_power_kw,
    avg(capacity_factor) as avg_capacity_factor
from power_location
group by date, state
order by date, state