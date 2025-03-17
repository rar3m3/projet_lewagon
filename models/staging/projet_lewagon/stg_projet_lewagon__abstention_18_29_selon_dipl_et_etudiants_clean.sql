with 

source as (

    select * from {{ source('projet_lewagon', 'abstention_18_29_selon_dipl_et_etudiants_clean') }}

),

renamed as (

    select
        diplome,
        annee,
        etudiants,
        hors_etudiants

    from source

)

select * from renamed
