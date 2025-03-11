with 

source as (

    select * from {{ source('projet_lewagon', 's_1_2012') }}

),

renamed as (

    select
        sondeur,
        date,
        nathalie_arthaud_lo,
        philippe_poutou_npa,
        jean-luc_melenchon_fg,
        françois_hollande_ps,
        eva_joly_eelv,
        françois_bayrou_modem,
        nicolas_sarkozy_ump,
        nicolas_dupont-aignan_dlr,
        marine_le_pen_fn,
        jacques_cheminade_s&p

    from source

)

select * from renamed
