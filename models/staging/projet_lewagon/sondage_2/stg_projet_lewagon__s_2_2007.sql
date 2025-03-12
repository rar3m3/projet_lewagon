with 

source as (

    select * from {{ source('projet_lewagon', 's_2_2007') }}

),

renamed as (

    select
        sondeur,
        `date`,
        nicolas_sarkozy,
        segolene_royal

    from source

)

select * from renamed
