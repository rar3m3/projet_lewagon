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
        END, ',', '.') AS FLOAT64) AS abstention,

    SAFE_CAST(REPLACE(
        CASE 
            WHEN LENGTH(indecis_echantillon) > 2 THEN SUBSTR(indecis_echantillon, 1, LENGTH(indecis_echantillon) - 2)
            ELSE indecis_echantillon
        END, ',', '.') AS FLOAT64) AS indecis_echantillon,

    SAFE_CAST(REPLACE(
        CASE 
            WHEN LENGTH(`jean-luc_melenchon_lfi`) > 2 THEN SUBSTR(`jean-luc_melenchon_lfi`, 1, LENGTH(`jean-luc_melenchon_lfi`) - 2)
            ELSE `jean-luc_melenchon_lfi`
        END, ',', '.') AS FLOAT64) AS gauche_lfi,

    SAFE_CAST(REPLACE(
        CASE 
            WHEN LENGTH(`benoît_hamon_ps`) > 2 THEN SUBSTR(`benoît_hamon_ps`, 1, LENGTH(`benoît_hamon_ps`) - 2)
            ELSE `benoît_hamon_ps`
        END, ',', '.') AS FLOAT64) AS gauche_ps,

    SAFE_CAST(REPLACE(
        CASE 
            WHEN LENGTH(emmanuel_macron_em) > 2 THEN SUBSTR(emmanuel_macron_em, 1, LENGTH(emmanuel_macron_em) - 2)
            ELSE emmanuel_macron_em
        END, ',', '.') AS FLOAT64) AS centre_em,

    SAFE_CAST(REPLACE(
        CASE 
            WHEN LENGTH(`françois_fillon_lr`) > 2 THEN SUBSTR(`françois_fillon_lr`, 1, LENGTH(`françois_fillon_lr`) - 2)
            ELSE `françois_fillon_lr`
        END, ',', '.') AS FLOAT64) AS droite_lr,

    SAFE_CAST(REPLACE(
        CASE 
            WHEN LENGTH(marine_le_pen_fn) > 2 THEN SUBSTR(marine_le_pen_fn, 1, LENGTH(marine_le_pen_fn) - 2)
            ELSE marine_le_pen_fn
        END, ',', '.') AS FLOAT64) AS ext_droite_fn

    from source

)

select * from renamed
