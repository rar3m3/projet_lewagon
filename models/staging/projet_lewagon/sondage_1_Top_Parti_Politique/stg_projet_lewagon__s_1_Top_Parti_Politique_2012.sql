with 

source as (

    select * from {{ source('projet_lewagon', 's_1_2012') }}

),

renamed as (

    select
        sondeur,
        `date`,
        `jean-luc_melenchon_fg` AS gauche_fg,
        `françois_hollande_ps` AS gauche_ps,
        eva_joly_eelv AS gauche_eelv,
        `françois_bayrou_modem` AS centre_modem,
        nicolas_sarkozy_ump AS droite_ump,
        marine_le_pen_fn AS ext_droite_fn

    from source

)

select * from renamed
