with 

source as (

    select * from {{ source('projet_lewagon', 'antenne_lepen_mars') }}

),

renamed as (

    select
        candidat,
        d___tail_des_temps,
        tranche__6h_9h__dur___e_,
        tranche__9h_18h__dur___e_,
        tranche__18h_24h__dur___e_,
        tranche__0h_6h__dur___e_

    from source

)

select * from renamed
