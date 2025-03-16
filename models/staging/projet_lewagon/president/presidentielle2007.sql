
WITH aggregated_data AS (
    SELECT
      "2007" AS annee,
     code_departement,
      SUM(inscrits) AS inscrits,
      SUM(votants) AS votants,
      SUM(exprimes) AS exprimes,
      SUM(voixLAGUILLER) AS laguiller,
      SUM(voixBESANCENOT) AS besancenot,
      SUM(voixSCHIVARDI) AS schivardi,
      SUM(voixBUFFET) AS buffet,
      SUM(voixBOVE) AS bove,
      SUM(voixVOYNET) AS voynet,
      SUM(voixROYAL) AS royal,
      SUM(voixNIHOUS) AS nihous,
      SUM(voixBAYROU) AS bayrou,
      SUM(voixSARKOZY) AS sarkozy,
      SUM(voixVILLIERS) AS villiers,
      SUM(voixLEPEN) AS le_pen,
      SUM(voixT2ROYAL) AS t2royal,
      SUM(voixT2SARKOZY) AS t2sarkozy
    FROM 
      {{ ref('stg_projet_lewagon__pres2007') }}
    GROUP BY code_departement
  )
  SELECT 
    ad.*,
    x.column_name AS max_voix_t1,
    x2.column_name AS max_voix_t2
  FROM 
    aggregated_data ad,
    UNNEST([ 
      STRUCT('laguiller' AS column_name, laguiller AS value),
      STRUCT('besancenot' AS column_name, besancenot AS value),
      STRUCT('schivardi' AS column_name, schivardi AS value),
      STRUCT('buffet' AS column_name, buffet AS value),
      STRUCT('bove' AS column_name, bove AS value),
      STRUCT('voynet' AS column_name, voynet AS value),
      STRUCT('royal' AS column_name, royal AS value),
      STRUCT('nihous' AS column_name, nihous AS value),
      STRUCT('bayrou' AS column_name, bayrou AS value),
      STRUCT('sarkozy' AS column_name, sarkozy AS value),
      STRUCT('villiers' AS column_name, villiers AS value),
      STRUCT('le_pen' AS column_name, le_pen AS value)
    ]) x,
    UNNEST([ 
      STRUCT('royal' AS column_name, t2royal AS value),
      STRUCT('sarkozy' AS column_name, t2sarkozy AS value)
    ]) x2
  WHERE 
    x.value = GREATEST(
      laguiller, besancenot, schivardi, 
      buffet, bove, voynet, 
      royal, nihous, bayrou, 
      sarkozy, villiers, le_pen
    )
    AND 
    x2.value = GREATEST(
      t2royal, 
      t2sarkozy
    )

