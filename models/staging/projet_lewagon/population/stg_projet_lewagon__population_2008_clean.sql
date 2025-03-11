with 

source as (

    select * from {{ source('projet_lewagon', 'population_2008_clean') }}

),

renamed as (

    select
        code_departement,
        code_commune,
        nom_commune,
        population

    from source

)

select * from renamed
