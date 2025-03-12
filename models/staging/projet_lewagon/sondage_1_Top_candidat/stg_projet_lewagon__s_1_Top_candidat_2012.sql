with 

source as (

    select * from {{ source('projet_lewagon', 's_1_2012') }}

),

renamed as (

    select
        sondeur,
        `date`,
SAFE_CAST(REPLACE(
    CASE 
        WHEN LENGTH(`jean-luc_melenchon_fg`) > 2 THEN SUBSTR(`jean-luc_melenchon_fg`, 1, LENGTH(`jean-luc_melenchon_fg`) - 2)
        ELSE `jean-luc_melenchon_fg`
    END, ',', '.') AS FLOAT64) AS `jean-luc_melenchon_fg`,

SAFE_CAST(REPLACE(
    CASE 
        WHEN LENGTH(`françois_hollande_ps`) > 2 THEN SUBSTR(`françois_hollande_ps`, 1, LENGTH(`françois_hollande_ps`) - 2)
        ELSE `françois_hollande_ps`
    END, ',', '.') AS FLOAT64) AS `françois_hollande_ps`,

SAFE_CAST(REPLACE(
    CASE 
        WHEN LENGTH(eva_joly_eelv) > 2 THEN SUBSTR(eva_joly_eelv, 1, LENGTH(eva_joly_eelv) - 2)
        ELSE eva_joly_eelv
    END, ',', '.') AS FLOAT64) AS eva_joly_eelv,

SAFE_CAST(REPLACE(
    CASE 
        WHEN LENGTH(`françois_bayrou_modem`) > 2 THEN SUBSTR(`françois_bayrou_modem`, 1, LENGTH(`françois_bayrou_modem`) - 2)
        ELSE `françois_bayrou_modem`
    END, ',', '.') AS FLOAT64) AS `françois_bayrou_modem`,

SAFE_CAST(REPLACE(
    CASE 
        WHEN LENGTH(nicolas_sarkozy_ump) > 2 THEN SUBSTR(nicolas_sarkozy_ump, 1, LENGTH(nicolas_sarkozy_ump) - 2)
        ELSE nicolas_sarkozy_ump
    END, ',', '.') AS FLOAT64) AS nicolas_sarkozy_ump,

SAFE_CAST(REPLACE(
    CASE 
        WHEN LENGTH(marine_le_pen_fn) > 2 THEN SUBSTR(marine_le_pen_fn, 1, LENGTH(marine_le_pen_fn) - 2)
        ELSE marine_le_pen_fn
    END, ',', '.') AS FLOAT64) AS marine_le_pen_fn


    from source

)

select * from renamed
