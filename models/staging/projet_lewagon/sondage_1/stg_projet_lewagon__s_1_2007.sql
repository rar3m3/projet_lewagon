{{ config(
    materialized='table'
) }}

with 

source as (

    select * from {{ source('projet_lewagon', 's_1_2007') }}

),

renamed as (

select
    "2007" AS annee,
    sondeur,
    `date`,
           SAFE_CAST(REPLACE(
        CASE 
            WHEN LENGTH(arlette_laguiller_lo) > 2 THEN SUBSTR(arlette_laguiller_lo, 1, LENGTH(arlette_laguiller_lo) - 2)
            ELSE arlette_laguiller_lo
        END, ',', '.') AS FLOAT64) AS laguiller,

    SAFE_CAST(REPLACE(
        CASE 
            WHEN LENGTH(olivier_besancenot_lcr) > 2 THEN SUBSTR(olivier_besancenot_lcr, 1, LENGTH(olivier_besancenot_lcr) - 2)
            ELSE olivier_besancenot_lcr
        END, ',', '.') AS FLOAT64) AS besancenot,

    SAFE_CAST(REPLACE(
        CASE 
            WHEN LENGTH(`marie-george_buffet_pcf`) > 2 THEN SUBSTR(`marie-george_buffet_pcf`, 1, LENGTH(`marie-george_buffet_pcf`) - 2)
            ELSE `marie-george_buffet_pcf`
        END, ',', '.') AS FLOAT64) AS buffet,

    SAFE_CAST(REPLACE(
        CASE 
            WHEN LENGTH(gerard_schivardi_pt) > 2 THEN SUBSTR(gerard_schivardi_pt, 1, LENGTH(gerard_schivardi_pt) - 2)
            ELSE gerard_schivardi_pt
        END, ',', '.') AS FLOAT64) AS schivardi,

    SAFE_CAST(REPLACE(
        CASE 
            WHEN LENGTH(segolene_royal_ps) > 2 THEN SUBSTR(segolene_royal_ps, 1, LENGTH(segolene_royal_ps) - 2)
            ELSE segolene_royal_ps
        END, ',', '.') AS FLOAT64) AS royal,

    SAFE_CAST(REPLACE(
        CASE 
            WHEN LENGTH(jose_bove_dvg) > 2 THEN SUBSTR(jose_bove_dvg, 1, LENGTH(jose_bove_dvg) - 2)
            ELSE jose_bove_dvg
        END, ',', '.') AS FLOAT64) AS bove,

    SAFE_CAST(REPLACE(
        CASE 
            WHEN LENGTH(dominique_voynet_lv) > 2 THEN SUBSTR(dominique_voynet_lv, 1, LENGTH(dominique_voynet_lv) - 2)
            ELSE dominique_voynet_lv
        END, ',', '.') AS FLOAT64) AS voynet,

    SAFE_CAST(REPLACE(
        CASE 
            WHEN LENGTH(`françois_bayrou_udf`) > 2 THEN SUBSTR(`françois_bayrou_udf`, 1, LENGTH(`françois_bayrou_udf`) - 2)
            ELSE `françois_bayrou_udf`
        END, ',', '.') AS FLOAT64) AS bayrou,

    SAFE_CAST(REPLACE(
        CASE 
            WHEN LENGTH(nicolas_sarkozy_ump) > 2 THEN SUBSTR(nicolas_sarkozy_ump, 1, LENGTH(nicolas_sarkozy_ump) - 2)
            ELSE nicolas_sarkozy_ump
        END, ',', '.') AS FLOAT64) AS sarkozy,

    SAFE_CAST(REPLACE(
        CASE 
            WHEN LENGTH(philippe_de_villiers_mpf) > 2 THEN SUBSTR(philippe_de_villiers_mpf, 1, LENGTH(philippe_de_villiers_mpf) - 2)
            ELSE philippe_de_villiers_mpf
        END, ',', '.') AS FLOAT64) AS de_villiers,

    SAFE_CAST(REPLACE(
        CASE 
            WHEN LENGTH(frederic_nihous_cpnt) > 2 THEN SUBSTR(frederic_nihous_cpnt, 1, LENGTH(frederic_nihous_cpnt) - 2)
            ELSE frederic_nihous_cpnt
        END, ',', '.') AS FLOAT64) AS nihous,

    SAFE_CAST(REPLACE(
        CASE 
            WHEN LENGTH(`jean-marie_le_pen_fn`) > 2 THEN SUBSTR(`jean-marie_le_pen_fn`, 1, LENGTH(`jean-marie_le_pen_fn`) - 2)
            ELSE `jean-marie_le_pen_fn`
        END, ',', '.') AS FLOAT64) AS le_pen

from source

)

select * from renamed
