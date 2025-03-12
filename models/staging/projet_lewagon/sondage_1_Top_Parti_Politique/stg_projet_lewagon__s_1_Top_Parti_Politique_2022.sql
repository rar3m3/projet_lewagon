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
        Arthaud_LO,
        Poutou_NPA,
        Roussel_PCF,
        Melenchon_LFI,
        Hidalgo_PS,
        `Jadot_EÉLV`,
        Macron_LREM,
        Pecresse_LR,
        `Lassalle_RES`,
        `Dupont-aignan_DLF`,
        Le_pen_RN,
        Zemmour_REC

    from source

)

select * from renamed
