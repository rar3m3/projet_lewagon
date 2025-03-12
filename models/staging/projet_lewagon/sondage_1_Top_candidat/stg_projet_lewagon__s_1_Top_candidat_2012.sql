with 

source as (

    select * from {{ source('projet_lewagon', 's_1_2012') }}

),

renamed as (

    select
        sondeur,
        `date`,
        `jean-luc_melenchon_fg`,
        `françois_hollande_ps`,
        eva_joly_eelv,
        `françois_bayrou_modem`,
        nicolas_sarkozy_ump,
        marine_le_pen_fn,

    from source

)

select * from renamed
