with 

source as (

    select * from {{ source('projet_lewagon', 's_2_2012') }}

),

renamed as (

    select
        sondeur,
        `date`,
        `françois_hollande` AS gauche_ps,
        nicolas_sarkozy AS droite_ump

    from source

)

select * from renamed
