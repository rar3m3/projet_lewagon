with 

source as (

    select * from {{ source('projet_lewagon', 's_1_2017') }}

),

renamed as (

    select
        sondeur,
        date,
        échantillon,
        abstention,
        indecis_echantillon,
        nathalie_arthaud_lo,
        philippe_poutou_npa,
        jean-luc_melenchon_lfi,
        benoît_hamon_ps,
        emmanuel_macron_em,
        jean_lassalle_res,
        françois_fillon_lr,
        nicolas_dupont-aignan_dlf,
        françois_asselineau_upr,
        marine_le_pen_fn,
        jacques_cheminade_s&p,
        unnamed:_16_level_1

    from source

)

select * from renamed
