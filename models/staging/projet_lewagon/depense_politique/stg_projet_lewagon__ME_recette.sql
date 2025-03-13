with 

source as (

    select * from {{ source('projet_lewagon', 'ME_recette') }}

),

renamed as (
SELECT
    'Melenchon' AS nom_candidat,
    string_field_0 AS compte,
    LOWER(REGEXP_REPLACE(NORMALIZE(`string_field_1`, NFD), r'\p{M}', '')) AS intitule_de_compte,
    IFNULL(SAFE_CAST(REPLACE(REPLACE(`string_field_2`, " ", ""), ",", ".") AS FLOAT64), 0) AS recettes_percues_par_le_mandataire,
    IFNULL(SAFE_CAST(string_field_3 AS INT64), 0) AS paiement_payees_par_la_formation_politique,
    IFNULL(SAFE_CAST(REPLACE(REPLACE(`string_field_4`, " ", ""), ",", ".") AS FLOAT64), 0) AS concours_en_nature,
    IFNULL(SAFE_CAST(REPLACE(REPLACE(`string_field_5`, " ", ""), ",", ".") AS FLOAT64), 0) AS totaux
FROM source
)
select * from renamed
