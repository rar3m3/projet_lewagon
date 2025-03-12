with 

source as (

    select * from {{ source('projet_lewagon', 's_2_2022') }}

),

renamed as (

    select
        `index`,
        sondeur,
        dates,
        `Échantillon`,
        macron_lrem AS centre_udf,
        le_pen_rn AS ext_droite_fn

    from source

)

select * from renamed
