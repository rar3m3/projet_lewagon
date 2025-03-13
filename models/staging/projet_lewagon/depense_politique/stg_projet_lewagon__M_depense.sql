with 

source as (

    select * from {{ source('projet_lewagon', 'M_depense') }}

),

renamed as (

SELECT
    'Macron' AS nom_candidat,
    string_field_0 AS compte,
    LOWER(REGEXP_REPLACE(NORMALIZE(string_field_1, NFD), r'\p{M}', '')) AS intitule_de_compte,
    IFNULL(SAFE_CAST(REPLACE(REPLACE(string_field_2, " ", ""), ",", ".") AS FLOAT64), 0) AS depense_payees_par_le_mandataire,
    IFNULL(SAFE_CAST(string_field_3 AS INT64), 0) AS depenses_payees_par_la_formation_politique,
    IFNULL(SAFE_CAST(REPLACE(REPLACE(string_field_4, " ", ""), ",", ".") AS FLOAT64), 0) AS concours_en_nature,
    IFNULL(SAFE_CAST(REPLACE(REPLACE(string_field_5, " ", ""), ",", ".") AS FLOAT64), 0) AS totaux
FROM source

)

select * from renamed
