SELECT * 
FROM {{ ref('stg_projet_lewagon__J_recette') }}
UNION ALL
SELECT * 
FROM {{ ref('stg_projet_lewagon__LP_recette') }}
UNION ALL
SELECT * 
FROM {{ ref('stg_projet_lewagon__ME_recette') }}
UNION ALL
SELECT * 
FROM {{ ref('stg_projet_lewagon__M_recette') }}
