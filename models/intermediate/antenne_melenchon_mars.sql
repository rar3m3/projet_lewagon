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

FROM {{ ref('stg_projet_lewagon__antenne_melenchon_mars') }}
