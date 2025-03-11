with 

source as (

    select * from {{ source('projet_lewagon', 'M_recette') }}

),

renamed as (

    select
        string_field_0,
        string_field_1,
        string_field_2,
        string_field_3,
        string_field_4,
        string_field_5

    from source

)

select * from renamed
