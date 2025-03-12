with 

source as (

    select * from {{ source('projet_lewagon', 's_1_2002') }}

),

renamed as (

    select
        sondeur,
        `date`,
SAFE_CAST(REPLACE(
    CASE 
        WHEN LENGTH(lionel_jospin_ps) > 2 THEN SUBSTR(lionel_jospin_ps, 1, LENGTH(lionel_jospin_ps) - 2)
        ELSE lionel_jospin_ps
    END, ',', '.') AS FLOAT64) AS lionel_jospin_ps,

SAFE_CAST(REPLACE(
    CASE 
        WHEN LENGTH(`noël_mamere_lv`) > 2 THEN SUBSTR(`noël_mamere_lv`, 1, LENGTH(`noël_mamere_lv`) - 2)
        ELSE `noël_mamere_lv`
    END, ',', '.') AS FLOAT64) AS `noël_mamere_lv`,

SAFE_CAST(REPLACE(
    CASE 
        WHEN LENGTH(`jean-pierre_chevenement_mdc`) > 2 THEN SUBSTR(`jean-pierre_chevenement_mdc`, 1, LENGTH(`jean-pierre_chevenement_mdc`) - 2)
        ELSE `jean-pierre_chevenement_mdc`
    END, ',', '.') AS FLOAT64) AS `jean-pierre_chevenement_mdc`,

SAFE_CAST(REPLACE(
    CASE 
        WHEN LENGTH(`françois_bayrou_udf`) > 2 THEN SUBSTR(`françois_bayrou_udf`, 1, LENGTH(`françois_bayrou_udf`) - 2)
        ELSE `françois_bayrou_udf`
    END, ',', '.') AS FLOAT64) AS `françois_bayrou_udf`,

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
