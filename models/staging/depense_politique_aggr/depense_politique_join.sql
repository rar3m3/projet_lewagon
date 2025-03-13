WITH lp_recette AS (
    SELECT * 
    FROM {{ source('projet_lewagon', 'LP_recette') }}
),
j_recette AS (
    SELECT * 
    FROM {{ source('projet_lewagon', 'J_recette') }}
),
me_recette AS (
    SELECT * 
    FROM {{ source('projet_lewagon', 'ME_recette') }}
),
m_recette AS (
    SELECT * 
    FROM {{ source('projet_lewagon', 'M_recette') }}
)

SELECT 
    lp.*,
    jr.*,
    mer.*,
    mr.*
FROM lp_recette lp
JOIN j_recette jr
    ON lp.compte = jr.compte
JOIN me_recette mer
    ON lp.compte = mer.string_field_0
JOIN m_recette mr
    ON lp.compte = mr.string_field_0
