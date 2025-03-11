with 

source as (

    select * from {{ source('projet_lewagon', 's_1_2022') }}

),

renamed as (

    select
        index,
        sondeur,
        date,
        échantillon,
        arthaud_lo,
        poutou_npa,
        roussel_pcf,
        melenchon_lfi,
        hidalgo_ps,
        jadot_eélv,
        macron_lrem,
        pecresse_lr,
        lassalle_res,
        dupont-aignan_dlf,
        le_pen_rn,
        zemmour_rec

    from source

)

select * from renamed
