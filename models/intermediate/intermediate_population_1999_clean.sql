{{ config(
    materialized='view'
) }}

with 

source as (

    select * from {{ source('projet_lewagon', 'population_1999_clean') }}
    WHERE population <> "PSDC99"

),

renamed as (

    select
        code_departement,
        SUM(CAST(population AS INTEGER)) AS population
    from source
GROUP BY code_departement
)

select * from renamed
