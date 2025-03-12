with 

source as (

    select * from {{ source('projet_lewagon', 's_2_2017') }}

),

renamed as (

    select
        `index`,
        sondeur,
        `date`,
        `Échantillon`,
        abstention,
        indecis_echantillon,
        emmanuel_macron AS centre_udf,
        marine_le_pen AS ext_droite_fn

    from source

)

select * from renamed
