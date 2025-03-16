SELECT
nom_candidat, 
ROUND (SUM (recettes_percues_par_le_mandataire),2) AS recettes_percues_par_le_mandataire,
ROUND (SUM(paiement_payees_par_la_formation_politique),2) AS paiement_payees_par_la_formation_politique,
ROUND (SUM (concours_en_nature),2) AS concours_en_nature,
ROUND (SUM (totaux),2) AS totaux_recette
FROM {{ ref('recette_politique_join') }}
GROUP BY nom_candidat