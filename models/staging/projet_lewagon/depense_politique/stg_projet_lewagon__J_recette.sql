with 

source as (

    select * from {{ source('projet_lewagon', 'J_recette') }}

),

renamed as (

SELECT 
    'Jadot' AS nom_candidat,
    compte,
    LOWER(REGEXP_REPLACE(NORMALIZE(`Intitulé de compte`, NFD), r'\p{M}', '')) AS intitule_de_compte,
    IFNULL(SAFE_CAST(REPLACE(REPLACE(`1 Recettes perçues par le mandataire`, " ", ""), ",", ".") AS FLOAT64), 0) AS recettes_percues_par_le_mandataire,
    IFNULL(`2 Paiement par les formations politiques`,0) AS paiement_payees_par_la_formation_politique,
    IFNULL(SAFE_CAST(REPLACE(REPLACE(`3 Concours en nature`, " ", ""), ",", ".") AS FLOAT64), 0) AS concours_en_nature,
    IFNULL(SAFE_CAST(REPLACE(REPLACE(`4 Totaux`, " ", ""), ",", ".") AS FLOAT64), 0) AS totaux
FROM source

)

select * from renamed
