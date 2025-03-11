with 

source as (

    select * from {{ source('projet_lewagon', 'LP_recette') }}

),

renamed as (

    select
        compte,
        intitulé de compte,
        1 recettes perçues par le mandataire,
        2  paiement par les formations politiques,
        3  concours en nature,
        4  totaux

    from source

)

select * from renamed
