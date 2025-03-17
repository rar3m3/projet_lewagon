with 

source as (

    select * from {{ source('projet_lewagon', 'type_vote_selon_diplome') }}

),

renamed as (

    select
        type_de_vote,
        annee,
        aucun_diplome,
        diplome_inferieur_au_baccalaureat,
        baccalaureat,
        diplome_de_lenseignement_superieur,
        ensemble

    from source

)

select * from renamed
