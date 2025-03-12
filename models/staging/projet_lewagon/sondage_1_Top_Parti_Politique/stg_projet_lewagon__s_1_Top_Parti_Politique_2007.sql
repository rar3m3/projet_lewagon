with 

source as (

    select * from {{ source('projet_lewagon', 's_1_2007') }}

),

renamed as (

    select
        sondeur,
        `date`,
        segolene_royal_ps AS gauche_ps,
        dominique_voynet_lv AS gauche_lv,
        `françois_bayrou_udf` AS centre_udf,
        nicolas_sarkozy_ump AS droite_ump,
        `jean-marie_le_pen_fn`AS ext_droite_fn

    from source

)

select * from renamed
