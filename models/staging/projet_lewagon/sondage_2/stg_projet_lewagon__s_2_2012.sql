with 

source as (

    select * from {{ source('projet_lewagon', 's_2_2012') }}

),

renamed as (

    select
        sondeur,
        date,
        françois_hollande,
        nicolas_sarkozy

    from source

)

select * from renamed
