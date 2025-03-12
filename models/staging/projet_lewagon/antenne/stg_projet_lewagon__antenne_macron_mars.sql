with 

source as (

    select * from {{ source('projet_lewagon', 'antenne_macron_mars') }}

),

renamed as (

SELECT 
    Candidat AS Media,
    D__tail_des_temps AS details_des_temps, 
    SAFE_CAST(Tranche__6h_9h__dur__e_ AS TIME) AS Tranche_6h_9h,
    SAFE_CAST(Tranche__9h_18h__dur__e_ AS TIME) AS Tranche_9h_18h,
    SAFE_CAST(Tranche__18h_24h__dur__e_ AS TIME) AS Tranche_18h_24h,
    SAFE_CAST(Tranche__0h_6h__dur__e_ AS TIME) AS Tranche_0h_6h
  FROM source
  WHERE LOWER(TRIM(`D__tail_des_temps`)) = "total temps d\'antenne"

)

select * from renamed
