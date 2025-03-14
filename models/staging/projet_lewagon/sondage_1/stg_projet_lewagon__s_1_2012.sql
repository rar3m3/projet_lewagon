{{ config(
    materialized='table'
) }}

with 

source as (

    select * from {{ source('projet_lewagon', 's_1_2012') }}

),

renamed as (

 select
    "2012" AS annee,
    sondeur,
    `date`,
    SAFE_CAST(REPLACE(
        CASE 
            WHEN LENGTH(nathalie_arthaud_lo) > 2 THEN SUBSTR(nathalie_arthaud_lo, 1, LENGTH(nathalie_arthaud_lo) - 2)
            ELSE nathalie_arthaud_lo
        END, ',', '.') AS FLOAT64) AS nathalie_arthaud_lo,

    SAFE_CAST(REPLACE(
        CASE 
            WHEN LENGTH(philippe_poutou_npa) > 2 THEN SUBSTR(philippe_poutou_npa, 1, LENGTH(philippe_poutou_npa) - 2)
            ELSE philippe_poutou_npa
        END, ',', '.') AS FLOAT64) AS philippe_poutou_npa,

    SAFE_CAST(REPLACE(
        CASE 
            WHEN LENGTH(`jean-luc_melenchon_fg`) > 2 THEN SUBSTR(`jean-luc_melenchon_fg`, 1, LENGTH(`jean-luc_melenchon_fg`) - 2)
            ELSE `jean-luc_melenchon_fg`
        END, ',', '.') AS FLOAT64) AS jean_luc_melenchon_fg,

    SAFE_CAST(REPLACE(
        CASE 
            WHEN LENGTH(`françois_hollande_ps`) > 2 THEN SUBSTR(`françois_hollande_ps`, 1, LENGTH(`françois_hollande_ps`) - 2)
            ELSE `françois_hollande_ps`
        END, ',', '.') AS FLOAT64) AS francois_hollande_ps,

    SAFE_CAST(REPLACE(
        CASE 
            WHEN LENGTH(eva_joly_eelv) > 2 THEN SUBSTR(eva_joly_eelv, 1, LENGTH(eva_joly_eelv) - 2)
            ELSE eva_joly_eelv
        END, ',', '.') AS FLOAT64) AS eva_joly_eelv,

    SAFE_CAST(REPLACE(
        CASE 
            WHEN LENGTH(`françois_bayrou_modem`) > 2 THEN SUBSTR(`françois_bayrou_modem`, 1, LENGTH(`françois_bayrou_modem`) - 2)
            ELSE `françois_bayrou_modem`
        END, ',', '.') AS FLOAT64) AS francois_bayrou_modem,

    SAFE_CAST(REPLACE(
        CASE 
            WHEN LENGTH(nicolas_sarkozy_ump) > 2 THEN SUBSTR(nicolas_sarkozy_ump, 1, LENGTH(nicolas_sarkozy_ump) - 2)
            ELSE nicolas_sarkozy_ump
        END, ',', '.') AS FLOAT64) AS nicolas_sarkozy_ump,

    SAFE_CAST(REPLACE(
        CASE 
            WHEN LENGTH(`nicolas_dupont-aignan_dlr`) > 2 THEN SUBSTR(`nicolas_dupont-aignan_dlr`, 1, LENGTH(`nicolas_dupont-aignan_dlr`) - 2)
            ELSE `nicolas_dupont-aignan_dlr`
        END, ',', '.') AS FLOAT64) AS nicolas_dupont_aignan_dlr,

    SAFE_CAST(REPLACE(
        CASE 
            WHEN LENGTH(marine_le_pen_fn) > 2 THEN SUBSTR(marine_le_pen_fn, 1, LENGTH(marine_le_pen_fn) - 2)
            ELSE marine_le_pen_fn
        END, ',', '.') AS FLOAT64) AS marine_le_pen_fn,

    SAFE_CAST(REPLACE(
        CASE 
            WHEN LENGTH(`jacques_cheminade_s&p`) > 2 THEN SUBSTR(`jacques_cheminade_s&p`, 1, LENGTH(`jacques_cheminade_s&p`) - 2)
            ELSE `jacques_cheminade_s&p`
        END, ',', '.') AS FLOAT64) AS jacques_cheminade_sp

from source

)

select * from renamed
