with 

source as (

    select * from {{ source('projet_lewagon', 's_1_2002') }}

),

renamed as (

    select
        sondeur,
        `date`,
        lionel_jospin_ps,
        `noël_mamere_lv`,
        `jean-pierre_chevenement_mdc`,
        `françois_bayrou_udf`,
        jacques_chirac_rpr,
        `jean-marie_le_pen_fn`,

    from source

)

select * from renamed
