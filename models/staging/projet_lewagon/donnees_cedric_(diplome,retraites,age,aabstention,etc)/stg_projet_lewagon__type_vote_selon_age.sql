with 

source as (

    select * from {{ source('projet_lewagon', 'type_vote_selon_age') }}

),

renamed as (

    select
        type_de_vote,
        annee,
        ensemble,
        _18_29_ans,
        _30_64_ans,
        _65_ans_ou_plus

    from source

)

select * from renamed
