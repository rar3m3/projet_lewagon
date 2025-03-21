
with 

source as (

    select * from {{ source('projet_lewagon', 's_2_2012') }}

),

renamed as (

    select
        "2012" AS annee,
        sondeur,
        `date`,
SAFE_CAST(REPLACE(
    CASE 
        WHEN LENGTH(`françois_hollande`) > 2 THEN SUBSTR(`françois_hollande`, 1, LENGTH(`françois_hollande`) - 2)
        ELSE `françois_hollande`
    END, ',', '.') AS FLOAT64) AS hollande,

SAFE_CAST(REPLACE(
    CASE 
        WHEN LENGTH(nicolas_sarkozy) > 2 THEN SUBSTR(nicolas_sarkozy, 1, LENGTH(nicolas_sarkozy) - 2)
        ELSE nicolas_sarkozy
    END, ',', '.') AS FLOAT64) AS sarkozy


    from source

)

select * from renamed
