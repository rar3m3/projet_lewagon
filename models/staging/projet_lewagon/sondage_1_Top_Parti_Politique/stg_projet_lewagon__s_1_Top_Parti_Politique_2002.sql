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
    lionel_jospin_ps AS gauche_ps,
    noel_mamere_lv AS gauche_lv,
    jean_pierre_chevenement_mdc AS gauche_mdc,
    francois_bayrou_udf AS centre_udf,
    jacques_chirac_rpr AS droite_rpr,
    jean_marie_le_pen_fn AS ext_droite_fn
    
from source

)

select * from renamed
