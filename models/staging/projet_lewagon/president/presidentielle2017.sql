{{ config(
    materialized='table'
) }}

WITH source AS (
  SELECT * FROM {{ ref('stg_projet_lewagon__pres2017') }}
),
renamed AS (
  WITH aggregated_data AS (
    SELECT
      code_departement,
      SUM(inscrits) AS inscrits,
      SUM(votants) AS votants,
      SUM(exprimes) AS exprimes,
      SUM(voixarthaud) AS arthaud,
      SUM(voixpoutou) AS poutou,
      SUM(voixmelenchon) AS melenchon,
      SUM(voixhamon) AS hamon,
      SUM(voixcheminade) AS cheminade,
      SUM(voixlassalle) AS lassalle,
      SUM(voixmacron) AS macron,
      SUM(voixfillon) AS fillon,
      SUM(voixasselineau) AS asselineau,
      SUM(voixdupontaignan) AS dupontaignan,
      SUM(voixmlepen) AS mlepen,
      SUM(voixt2macron) AS t2macron,
      SUM(voixt2mlepen) AS t2mlepen
    FROM 
      source
    GROUP BY code_departement
  )
  SELECT 
    ad.*,
    x.column_name AS max_voix_t1,
    x2.column_name AS max_voix_t2
  FROM 
    aggregated_data ad,
    UNNEST([ 
      STRUCT('arthaud' AS column_name, arthaud AS value),
      STRUCT('poutou' AS column_name, poutou AS value),
      STRUCT('melenchon' AS column_name, melenchon AS value),
      STRUCT('hamon' AS column_name, hamon AS value),
      STRUCT('cheminade' AS column_name, cheminade AS value),
      STRUCT('lassalle' AS column_name, lassalle AS value),
      STRUCT('macron' AS column_name, macron AS value),
      STRUCT('fillon' AS column_name, fillon AS value),
      STRUCT('asselineau' AS column_name, asselineau AS value),
      STRUCT('dupontaignan' AS column_name, dupontaignan AS value),
      STRUCT('mlepen' AS column_name, mlepen AS value)
    ]) x,
    UNNEST([ 
      STRUCT('t2macron' AS column_name, t2macron AS value),
      STRUCT('t2mlepen' AS column_name, t2mlepen AS value)
    ]) x2
  WHERE 
    x.value = GREATEST(
      arthaud, poutou, melenchon, 
      hamon, cheminade, lassalle, 
      macron, fillon, asselineau, 
      dupontaignan, mlepen
    )
    AND 
    x2.value = GREATEST(
      t2macron, 
      t2mlepen
    )
)
SELECT * 
FROM renamed
