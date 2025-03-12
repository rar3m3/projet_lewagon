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
        `jean-luc_melenchon_lfi` AS gauche_lfi,
        `benoît_hamon_ps` AS gauche_ps,
        emmanuel_macron_em AS centre_em,
        `françois_fillon_lr` AS droite_lr,
        marine_le_pen_fn AS ext_droite_fn,

    from source

)

select * from renamed
