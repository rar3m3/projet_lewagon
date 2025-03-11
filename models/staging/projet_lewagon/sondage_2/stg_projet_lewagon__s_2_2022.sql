with 

source as (

    select * from {{ source('projet_lewagon', 's_2_2022') }}

),

renamed as (

    select
        index,
        sondeur,
        dates,
        échantillon,
        macron_lrem,
        le_pen_rn

    from source

)

select * from renamed
