{{ config(
    materialized='view'
) }}

with 

source as (

    select * from {{ source('projet_lewagon', 'population_2011_clean') }}
    WHERE population <> "PMUN11"

),

renamed as (

    select
        code_departement,
        SUM(CAST(population AS FLOAT64)) AS population
    from source
GROUP BY code_departement
)

select * from renamed