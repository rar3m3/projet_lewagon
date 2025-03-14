{{ config(
    materialized='table'
) }}

with 

source as (

    select * from {{ source('projet_lewagon', 's_2_2007') }}

),

renamed as (

    select
        "2007" AS annee,
        sondeur,
        `date`,
        SAFE_CAST(REPLACE(
    CASE 
        WHEN LENGTH(nicolas_sarkozy) > 2 THEN SUBSTR(nicolas_sarkozy, 1, LENGTH(nicolas_sarkozy) - 2)
        ELSE nicolas_sarkozy
    END, ',', '.') AS FLOAT64) AS nicolas_sarkozy,

SAFE_CAST(REPLACE(
    CASE 
        WHEN LENGTH(segolene_royal) > 2 THEN SUBSTR(segolene_royal, 1, LENGTH(segolene_royal) - 2)
        ELSE segolene_royal
    END, ',', '.') AS FLOAT64) AS segolene_royal


    from source

)

select * from renamed
