with 

source as (

    select * from {{ source('projet_lewagon', 'J_depense') }}

),

renamed as (

    select
        compte,
        intitulé de compte,
        1 dépenses payées par le mandataire,
        2 dépenses payées par les formations politiques,
        3concours en nature,
        4 totaux

    from source

)

select * from renamed
