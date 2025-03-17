with 

source as (

    select * from {{ source('projet_lewagon', 'comportement_vote_selon_groupe_socio_pro') }}

),

renamed as (

    select
        groupe,
        annee,
        vote_systematique,
        vote_intermittent,
        abstention_systematique

    from source

)

select * from renamed
