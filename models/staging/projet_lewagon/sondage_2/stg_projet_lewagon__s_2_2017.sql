with 

source as (

    select * from {{ source('projet_lewagon', 's_2_2017') }}

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
        WHEN LENGTH(emmanuel_macron) > 2 THEN SUBSTR(emmanuel_macron, 1, LENGTH(emmanuel_macron) - 2)
        ELSE emmanuel_macron
    END, ',', '.') AS FLOAT64) AS emmanuel_macron,

SAFE_CAST(REPLACE(
    CASE 
        WHEN LENGTH(marine_le_pen) > 2 THEN SUBSTR(marine_le_pen, 1, LENGTH(marine_le_pen) - 2)
        ELSE marine_le_pen
    END, ',', '.') AS FLOAT64) AS marine_le_pen


    from source

)

select * from renamed
