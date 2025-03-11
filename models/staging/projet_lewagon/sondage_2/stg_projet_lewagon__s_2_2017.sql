with 

source as (

    select * from {{ source('projet_lewagon', 's_2_2017') }}

),

renamed as (

    select
        index,
        sondeur,
        date,
        échantillon,
        abstention,
        indecis_echantillon,
        emmanuel_macron,
        marine_le_pen

    from source

)

select * from renamed
