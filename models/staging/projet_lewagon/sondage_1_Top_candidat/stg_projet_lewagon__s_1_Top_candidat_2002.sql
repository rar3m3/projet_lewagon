{{ config(
    materialized='table'
) }}

with 

source as (

    select * from {{ source('projet_lewagon', 's_1_2002') }}

),

renamed as (

    select
    sondeur,
    `date`,
    lionel_jospin_ps,
    noel_mamere_lv,
    jean_pierre_chevenement_mdc,
    francois_bayrou_udf,
    jacques_chirac_rpr,
    jean_marie_le_pen_fn
    from source

)

select * from renamed
