-- models/power_with_location.sql
WITH power AS (
    SELECT *
    FROM {{ source('energy', 'power_output_cleaned') }}
),

locations AS (
    SELECT *
    FROM {{ source('energy', 'plant_locations') }}
)

SELECT 
    power.timestamp,
    power.asset_id,
    power.power_kw,
    power.capacity_kw,
    power.capacity_factor,
    locations.location
FROM power
INNER JOIN locations
    ON power.asset_id = locations.plant_id