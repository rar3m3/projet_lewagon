{{ config(
    materialized='table'
) }}

with 

source as (

    select * from {{ source('projet_lewagon', 's_2_2022') }}

),

renamed as (

    select
        sondeur,
        dates,
        `Échantillon`,
SAFE_CAST(REPLACE(
    CASE 
        WHEN LENGTH(macron_lrem) > 2 THEN SUBSTR(macron_lrem, 1, LENGTH(macron_lrem) - 2)
        ELSE macron_lrem
    END, ',', '.') AS FLOAT64) AS centre_udf,

SAFE_CAST(REPLACE(
    CASE 
        WHEN LENGTH(le_pen_rn) > 2 THEN SUBSTR(le_pen_rn, 1, LENGTH(le_pen_rn) - 2)
        ELSE le_pen_rn
    END, ',', '.') AS FLOAT64) AS ext_droite_fn


    from source

)

select * from renamed
