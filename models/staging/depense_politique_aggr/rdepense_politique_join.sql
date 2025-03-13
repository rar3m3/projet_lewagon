SELECT * 
FROM {{ ref('stg_projet_lewagon__LP_depense') }}
UNION ALL
SELECT * 
FROM {{ ref('stg_projet_lewagon__J_depense') }}
UNION ALL
SELECT * 
FROM {{ ref('stg_projet_lewagon__ME_depense') }}
UNION ALL
SELECT * 
FROM {{ ref('stg_projet_lewagon__M_depense') }}
