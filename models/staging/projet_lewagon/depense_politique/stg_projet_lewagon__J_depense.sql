with 

source as (

    select * from {{ source('projet_lewagon', 'J_depense') }}

),

renamed as (

    
    SELECT 
        compte,
        LOWER(REGEXP_REPLACE(NORMALIZE(`Intitulé de compte`, NFD), r'\p{M}', '')) AS intitule_de_compte,
        IFNULL(SAFE_CAST(REPLACE(REPLACE(`1 Dépenses payées par le mandataire`, " ", ""), ",", ".") AS FLOAT64), 0) AS depense_payees_par_le_mandataire,
        IFNULL(`2 Dépenses payées par les formations politiques`,0) AS depenses_payees_par_la_formation_politique,
        IFNULL(SAFE_CAST(REPLACE(REPLACE(`3Concours en nature`, " ", ""), ",", ".") AS FLOAT64), 0) AS concours_en_nature,
        IFNULL(SAFE_CAST(REPLACE(REPLACE(`4 Totaux`, " ", ""), ",", ".") AS FLOAT64), 0) AS totaux
    from source

)

select * from renamed
