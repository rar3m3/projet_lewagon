{{ config(
    materialized='table'
) }}

with 

source as (

    select * from {{ source('projet_lewagon', 's_1_2002') }}

),

renamed as (

select
    "2002" AS annee,
    *
from source





)
select * from renamed
