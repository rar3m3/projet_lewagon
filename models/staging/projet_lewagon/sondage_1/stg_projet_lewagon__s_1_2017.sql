with 

source as (

    select * from {{ source('projet_lewagon', 's_1_2017') }}

),

renamed as (

select
    sondeur,
    `date`,
    `Échantillon`,
    SAFE_CAST(REPLACE(
        CASE 
            WHEN LENGTH(abstention) > 2 THEN SUBSTR(abstention, 1, LENGTH(abstention) - 2)
            ELSE abstention
        END, ',', '.') AS FLOAT64) AS abstention_float,

    SAFE_CAST(REPLACE(
        CASE 
            WHEN LENGTH(indecis_echantillon) > 2 THEN SUBSTR(indecis_echantillon, 1, LENGTH(indecis_echantillon) - 2)
            ELSE indecis_echantillon
        END, ',', '.') AS FLOAT64) AS indecis_echantillon_float,

    SAFE_CAST(REPLACE(
        CASE 
            WHEN LENGTH(`nathalie_arthaud_lo`) > 2 THEN SUBSTR(`nathalie_arthaud_lo`, 1, LENGTH(`nathalie_arthaud_lo`) - 2)
            ELSE `nathalie_arthaud_lo`
        END, ',', '.') AS FLOAT64) AS nathalie_arthaud_lo_float,

    SAFE_CAST(REPLACE(
        CASE 
            WHEN LENGTH(philippe_poutou_npa) > 2 THEN SUBSTR(philippe_poutou_npa, 1, LENGTH(philippe_poutou_npa) - 2)
            ELSE philippe_poutou_npa
        END, ',', '.') AS FLOAT64) AS philippe_poutou_npa_float,

    SAFE_CAST(REPLACE(
        CASE 
            WHEN LENGTH(`jean-luc_melenchon_lfi`) > 2 THEN SUBSTR(`jean-luc_melenchon_lfi`, 1, LENGTH(`jean-luc_melenchon_lfi`) - 2)
            ELSE `jean-luc_melenchon_lfi`
        END, ',', '.') AS FLOAT64) AS jean_luc_melenchon_lfi_float,

    SAFE_CAST(REPLACE(
        CASE 
            WHEN LENGTH(`benoît_hamon_ps`) > 2 THEN SUBSTR(`benoît_hamon_ps`, 1, LENGTH(`benoît_hamon_ps`) - 2)
            ELSE `benoît_hamon_ps`
        END, ',', '.') AS FLOAT64) AS benoit_hamon_ps_float,

    SAFE_CAST(REPLACE(
        CASE 
            WHEN LENGTH(emmanuel_macron_em) > 2 THEN SUBSTR(emmanuel_macron_em, 1, LENGTH(emmanuel_macron_em) - 2)
            ELSE emmanuel_macron_em
        END, ',', '.') AS FLOAT64) AS emmanuel_macron_em_float,

    SAFE_CAST(REPLACE(
        CASE 
            WHEN LENGTH(jean_lassalle_res) > 2 THEN SUBSTR(jean_lassalle_res, 1, LENGTH(jean_lassalle_res) - 2)
            ELSE jean_lassalle_res
        END, ',', '.') AS FLOAT64) AS jean_lassalle_res_float,

    SAFE_CAST(REPLACE(
        CASE 
            WHEN LENGTH(`françois_fillon_lr`) > 2 THEN SUBSTR(`françois_fillon_lr`, 1, LENGTH(`françois_fillon_lr`) - 2)
            ELSE `françois_fillon_lr`
        END, ',', '.') AS FLOAT64) AS francois_fillon_lr_float,

    SAFE_CAST(REPLACE(
        CASE 
            WHEN LENGTH(`nicolas_dupont-aignan_dlf`) > 2 THEN SUBSTR(`nicolas_dupont-aignan_dlf`, 1, LENGTH(`nicolas_dupont-aignan_dlf`) - 2)
            ELSE `nicolas_dupont-aignan_dlf`
        END, ',', '.') AS FLOAT64) AS nicolas_dupont_aignan_dlf_float,

    SAFE_CAST(REPLACE(
        CASE 
            WHEN LENGTH(`françois_asselineau_upr`) > 2 THEN SUBSTR(`françois_asselineau_upr`, 1, LENGTH(`françois_asselineau_upr`) - 2)
            ELSE `françois_asselineau_upr`
        END, ',', '.') AS FLOAT64) AS francois_asselineau_upr_float,

    SAFE_CAST(REPLACE(
        CASE 
            WHEN LENGTH(marine_le_pen_fn) > 2 THEN SUBSTR(marine_le_pen_fn, 1, LENGTH(marine_le_pen_fn) - 2)
            ELSE marine_le_pen_fn
        END, ',', '.') AS FLOAT64) AS marine_le_pen_fn_float,

    SAFE_CAST(REPLACE(
        CASE 
            WHEN LENGTH(`jacques_cheminade_s&p`) > 2 THEN SUBSTR(`jacques_cheminade_s&p`, 1, LENGTH(`jacques_cheminade_s&p`) - 2)
            ELSE `jacques_cheminade_s&p`
        END, ',', '.') AS FLOAT64) AS jacques_cheminade_sp_float

from source

)

select * from renamed
