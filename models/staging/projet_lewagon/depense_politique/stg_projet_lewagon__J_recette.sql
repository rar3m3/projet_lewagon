with 

source as (

    select * from {{ source('projet_lewagon', 'J_recette') }}

),

renamed as (

 SELECT 
  `1 Dépenses payées par le mandataire` AS depenses_payees_mandataire
FROM source;

)

select * from renamed
