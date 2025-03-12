SELECT 
    Candidat AS Media,
    D__tail_des_temps AS details_des_temps, 
    SAFE_CAST(Tranche__6h_9h__dur__e_ AS TIME) AS Tranche_6h_9h,
    SAFE_CAST(Tranche__9h_18h__dur__e_ AS TIME) AS Tranche_9h_18h,
    SAFE_CAST(Tranche__18h_24h__dur__e_ AS TIME) AS Tranche_18h_24h,
    SAFE_CAST(Tranche__0h_6h__dur__e_ AS TIME) AS Tranche_0h_6h
  FROM `omega-cider-448409-a9.projet_lewagon.antenne_lepen_premier_tour`
  WHERE LOWER(TRIM(`D__tail_des_temps`)) = 'total temps d\antenne'
