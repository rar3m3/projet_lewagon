WITH details_des_temps AS (
  SELECT 
    SAFE.PARSE_TIME('%H:%M:%S', CAST(Tranche__6h_9h__dur__e_ AS STRING)) AS Tranche_6h_9h,
    SAFE.PARSE_TIME('%H:%M:%S', CAST(Tranche__9h_18h__dur__e_ AS STRING)) AS Tranche_9h_18h,
    SAFE.PARSE_TIME('%H:%M:%S', CAST(Tranche__18h_24h__dur__e_ AS STRING)) AS Tranche_18h_24h,
    SAFE.PARSE_TIME('%H:%M:%S', CAST(Tranche__0h_6h__dur__e_ AS STRING)) AS Tranche_0h_6h
  FROM `omega-cider-448409-a9.projet_lewagon.antenne_macron_mars`
  WHERE LOWER(TRIM(`D__tail_des_temps`)) = "total temps d\'antenne"
) 

SELECT 
  FORMAT('%02d:%02d:%02d', 
         DIV(SUM(EXTRACT(HOUR FROM Tranche_6h_9h) * 3600 
                 + EXTRACT(MINUTE FROM Tranche_6h_9h) * 60 
                 + EXTRACT(SECOND FROM Tranche_6h_9h)), 3600),
         MOD(DIV(SUM(EXTRACT(HOUR FROM Tranche_6h_9h) * 3600 
                     + EXTRACT(MINUTE FROM Tranche_6h_9h) * 60 
                     + EXTRACT(SECOND FROM Tranche_6h_9h)), 60), 60),
         MOD(SUM(EXTRACT(HOUR FROM Tranche_6h_9h) * 3600 
                 + EXTRACT(MINUTE FROM Tranche_6h_9h) * 60 
                 + EXTRACT(SECOND FROM Tranche_6h_9h)), 60)) 
         AS Total_6h_9h,

  FORMAT('%02d:%02d:%02d', 
         DIV(SUM(EXTRACT(HOUR FROM Tranche_9h_18h) * 3600 
                 + EXTRACT(MINUTE FROM Tranche_9h_18h) * 60 
                 + EXTRACT(SECOND FROM Tranche_9h_18h)), 3600),
         MOD(DIV(SUM(EXTRACT(HOUR FROM Tranche_9h_18h) * 3600 
                     + EXTRACT(MINUTE FROM Tranche_9h_18h) * 60 
                     + EXTRACT(SECOND FROM Tranche_9h_18h)), 60), 60),
         MOD(SUM(EXTRACT(HOUR FROM Tranche_9h_18h) * 3600 
                 + EXTRACT(MINUTE FROM Tranche_9h_18h) * 60 
                 + EXTRACT(SECOND FROM Tranche_9h_18h)), 60)) 
         AS Total_9h_18h,

  FORMAT('%02d:%02d:%02d', 
         DIV(SUM(EXTRACT(HOUR FROM Tranche_18h_24h) * 3600 
                 + EXTRACT(MINUTE FROM Tranche_18h_24h) * 60 
                 + EXTRACT(SECOND FROM Tranche_18h_24h)), 3600),
         MOD(DIV(SUM(EXTRACT(HOUR FROM Tranche_18h_24h) * 3600 
                     + EXTRACT(MINUTE FROM Tranche_18h_24h) * 60 
                     + EXTRACT(SECOND FROM Tranche_18h_24h)), 60), 60),
         MOD(SUM(EXTRACT(HOUR FROM Tranche_18h_24h) * 3600 
                 + EXTRACT(MINUTE FROM Tranche_18h_24h) * 60 
                 + EXTRACT(SECOND FROM Tranche_18h_24h)), 60)) 
         AS Total_18h_24h,

  FORMAT('%02d:%02d:%02d', 
         DIV(SUM(EXTRACT(HOUR FROM Tranche_0h_6h) * 3600 
                 + EXTRACT(MINUTE FROM Tranche_0h_6h) * 60 
                 + EXTRACT(SECOND FROM Tranche_0h_6h)), 3600),
         MOD(DIV(SUM(EXTRACT(HOUR FROM Tranche_0h_6h) * 3600 
                     + EXTRACT(MINUTE FROM Tranche_0h_6h) * 60 
                     + EXTRACT(SECOND FROM Tranche_0h_6h)), 60), 60),
         MOD(SUM(EXTRACT(HOUR FROM Tranche_0h_6h) * 3600 
                 + EXTRACT(MINUTE FROM Tranche_0h_6h) * 60 
                 + EXTRACT(SECOND FROM Tranche_0h_6h)), 60)) 
         AS Total_0h_6h

FROM details_des_temps
