with 

source as (

    select * from {{ source('projet_lewagon', 'evolution_annee_retraites_entre_1999_et_2021') }}

),

renamed as (

    select
        annee,
        retraites

    from source

)

select * from renamed
