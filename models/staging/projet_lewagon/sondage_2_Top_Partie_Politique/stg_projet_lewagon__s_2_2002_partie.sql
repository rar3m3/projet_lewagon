with 

source as (

    select * from {{ source('projet_lewagon', 's_2_2002') }}

),

renamed as (

    select
        sondeur,
        `date`,
        jacques_chirac_rpr AS droite_ump,
        `jean-marie_le_pen_fn AS ext_droite_fn`

    from source

)

select * from renamed
