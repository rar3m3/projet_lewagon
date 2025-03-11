with 

source as (

    select * from {{ source('projet_lewagon', 'antenne_lepen_premier_tour') }}

),

renamed as (

    select
        candidat,
        d__tail_des_temps,
        tranche__6h_9h__dur__e_,
        tranche__9h_18h__dur__e_,
        tranche__18h_24h__dur__e_,
        tranche__0h_6h__dur__e_

    from source

)

select * from renamed
