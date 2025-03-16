SELECT
nom_candidat, 
ROUND (SUM (depense_payees_par_le_mandataire),2) AS depense_payees_par_le_mandataire,
ROUND (SUM(depenses_payees_par_la_formation_politique),2) AS depenses_payees_par_la_formation_politique,
ROUND (SUM (concours_en_nature),2) AS concours_en_nature,
ROUND (SUM (totaux),2) AS totaux_depense
FROM {{ ref('rdepense_politique_join') }}
GROUP BY nom_candidat