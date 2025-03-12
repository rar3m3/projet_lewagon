with 

source as (

    select * from {{ source('projet_lewagon', 's_1_2017') }}

),

renamed as (

    select
        sondeur,
        `date`,
        `Échantillon`,
        abstention,
        indecis_echantillon,
        `jean-luc_melenchon_lfi`,
        `benoît_hamon_ps`,
        emmanuel_macron_em,
        `françois_fillon_lr`,
        marine_le_pen_fn,

    from source

)

select * from renamed
