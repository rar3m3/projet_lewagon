with 

source as (

    select * from {{ source('projet_lewagon', 's_1_2022') }}

),

renamed as (

    select
        `Index`,
        Sondeur,
        `Date`,
        `Échantillon`,
        Melenchon_LFI AS gauche_lfi,
        Hidalgo_PS AS gauche_ps,
        `Jadot_EÉLV`AS gauche_eelv,
        Macron_LREM AS centre_lrem,
        Pecresse_LR AS droite_lr,
        Le_pen_RN AS ext_droite_rn,
        Zemmour_REC AS ext_droite_rec

    from source

)

select * from renamed
