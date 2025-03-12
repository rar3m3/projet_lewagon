with 

source as (

    select * from {{ source('projet_lewagon', 's_2_2007') }}

),

renamed as (

    select
        sondeur,
        `date`,
        nicolas_sarkozy AS droite_ump,
        segolene_royal AS gauche_ps

    from source

)

select * from renamed
