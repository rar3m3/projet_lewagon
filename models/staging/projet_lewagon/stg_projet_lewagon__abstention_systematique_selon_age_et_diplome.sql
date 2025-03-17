with 

source as (

    select * from {{ source('projet_lewagon', 'abstention_systematique_selon_age_et_diplome') }}

),

renamed as (

    select
        annee,
        tranche_age,
        aucun_diplome_,
        diplome_du_superieur_,
        ecart_entre_diplomes_du_superieur_et_personnes_sans_diplome__

    from source

)

select * from renamed
