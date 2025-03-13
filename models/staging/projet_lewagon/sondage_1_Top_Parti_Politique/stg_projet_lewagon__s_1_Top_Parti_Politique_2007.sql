{{ config(
    materialized='table'
) }}
with 

source as (

    select * from {{ source('projet_lewagon', 's_1_2007') }}

),

renamed as (

    select
        sondeur,
        `date`,
SAFE_CAST(REPLACE(
        CASE 
            WHEN LENGTH(segolene_royal_ps) > 2 THEN SUBSTR(segolene_royal_ps, 1, LENGTH(segolene_royal_ps) - 2)
            ELSE segolene_royal_ps
        END, ',', '.') AS FLOAT64) AS gauche_ps,

    SAFE_CAST(REPLACE(
        CASE 
            WHEN LENGTH(dominique_voynet_lv) > 2 THEN SUBSTR(dominique_voynet_lv, 1, LENGTH(dominique_voynet_lv) - 2)
            ELSE dominique_voynet_lv
        END, ',', '.') AS FLOAT64) AS gauche_lv,

    SAFE_CAST(REPLACE(
        CASE 
            WHEN LENGTH(`françois_bayrou_udf`) > 2 THEN SUBSTR(`françois_bayrou_udf`, 1, LENGTH(`françois_bayrou_udf`) - 2)
            ELSE `françois_bayrou_udf`
        END, ',', '.') AS FLOAT64) AS centre_udf,

    SAFE_CAST(REPLACE(
        CASE 
            WHEN LENGTH(nicolas_sarkozy_ump) > 2 THEN SUBSTR(nicolas_sarkozy_ump, 1, LENGTH(nicolas_sarkozy_ump) - 2)
            ELSE nicolas_sarkozy_ump
        END, ',', '.') AS FLOAT64) AS droite_ump,

    SAFE_CAST(REPLACE(
        CASE 
            WHEN LENGTH(`jean-marie_le_pen_fn`) > 2 THEN SUBSTR(`jean-marie_le_pen_fn`, 1, LENGTH(`jean-marie_le_pen_fn`) - 2)
            ELSE `jean-marie_le_pen_fn`
        END, ',', '.') AS FLOAT64) AS ext_droite_fn

    from source

)

select * from renamed
