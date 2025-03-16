WITH depense AS (
    SELECT * FROM {{ ref('depense_aggr_candidat') }}
),
recette AS (
    SELECT * FROM {{ ref('recette_aggr_candidat') }}
)

SELECT 
    d.*, 
    r.recettes_percues_par_le_mandataire,
    r.paiement_payees_par_la_formation_politique,
    r.concours_en_nature AS concours_en_nature_recette,
    r.totaux_recette 
   



FROM depense d
INNER JOIN recette r ON d.nom_candidat = r.nom_candidat
