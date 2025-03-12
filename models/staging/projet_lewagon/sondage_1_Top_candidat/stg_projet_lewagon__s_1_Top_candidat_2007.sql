with 

source as (

    select * from {{ source('projet_lewagon', 's_1_2007') }}

),

renamed as (

    select
        sondeur,
        `date`,
        segolene_royal_ps,
        dominique_voynet_lv,
        `françois_bayrou_udf`,
        nicolas_sarkozy_ump,
        `jean-marie_le_pen_fn`

    from source

)

select * from renamed
