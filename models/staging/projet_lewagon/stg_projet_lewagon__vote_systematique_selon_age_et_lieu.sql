with 

source as (

    select * from {{ source('projet_lewagon', 'vote_systematique_selon_age_et_lieu') }}

),

renamed as (

    select
        tranche_age,
        annee,
        rural_non_periurbain,
        rural_periurbain,
        urbain

    from source

)

select * from renamed
