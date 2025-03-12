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
        Melenchon_LFI,
        Hidalgo_PS,
        `Jadot_EÉLV`,
        Macron_LREM,
        Pecresse_LR,
        Le_pen_RN,
        Zemmour_REC

    from source

)

select * from renamed
