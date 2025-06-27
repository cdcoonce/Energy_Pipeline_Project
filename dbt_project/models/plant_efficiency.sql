{{ config(
    materialized='table',
    post_hook="COPY (SELECT * FROM {{ this }}) TO '../data/exports/plant_efficiency.csv' (HEADER, DELIMITER ',')"
) }}

with source_data as (
    select
        asset_id as plant_id,
        avg(power_kw) as avg_power_kw,
        max(capacity_kw) as capacity_kw
    from {{ source('energy', 'power_output_cleaned') }}
    group by plant_id
)

select
    plant_id,
    avg_power_kw,
    capacity_kw,
    case
        when capacity_kw = 0 then null
        else avg_power_kw / capacity_kw
    end as efficiency
from source_data