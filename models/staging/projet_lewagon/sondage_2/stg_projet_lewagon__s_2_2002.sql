{{ config(
    materialized='table'
) }}

with 

source as (

    select * from {{ source('projet_lewagon', 's_2_2002') }}

),

renamed as (

    select
        sondeur,
        `date`,
       SAFE_CAST(REPLACE(
    CASE 
        WHEN LENGTH(jacques_chirac_rpr) > 2 THEN SUBSTR(jacques_chirac_rpr, 1, LENGTH(jacques_chirac_rpr) - 2)
        ELSE jacques_chirac_rpr
    END, ',', '.') AS FLOAT64) AS jacques_chirac_rpr,

SAFE_CAST(REPLACE(
    CASE 
        WHEN LENGTH(`jean-marie_le_pen_fn`) > 2 THEN SUBSTR(`jean-marie_le_pen_fn`, 1, LENGTH(`jean-marie_le_pen_fn`) - 2)
        ELSE `jean-marie_le_pen_fn`
    END, ',', '.') AS FLOAT64) AS `jean-marie_le_pen_fn`


    from source

)

select * from renamed
