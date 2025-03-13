{{ config(
    materialized='table'
) }}

WITH source AS (
  SELECT * FROM {{ ref('stg_projet_lewagon__pres2022') }}
),
renamed AS (
  WITH aggregated_data AS (
    SELECT
      CONCAT('FR-', dep) AS code_departement,
      SUM(inscrits) AS inscrits,
      SUM(votants) AS votants,
      SUM(exprimes) AS exprimes,
      SUM(voixarthaud) AS arthaud,
      SUM(voixpoutou) AS poutou,
      SUM(voixroussel) AS roussel,
      SUM(voixmelenchon) AS melenchon,
      SUM(voixjadot) AS jadot,
      SUM(voixhidalgo) AS hidalgo,
      SUM(voixlassalle) AS lassalle,
      SUM(voixmacron) AS macron,
      SUM(voixpecresse) AS pecresse,
      SUM(voixzemmour) AS zemmour,
      SUM(voixdupontaignan) AS dupontaignan,
      SUM(voixmlepen) AS mlepen,
      SUM(voixt2macron) AS t2macron
    FROM 
      source
    GROUP BY dep
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
      STRUCT('roussel' AS column_name, roussel AS value),
      STRUCT('melenchon' AS column_name, melenchon AS value),
      STRUCT('jadot' AS column_name, jadot AS value),
      STRUCT('hidalgo' AS column_name, hidalgo AS value),
      STRUCT('lassalle' AS column_name, lassalle AS value),
      STRUCT('macron' AS column_name, macron AS value),
      STRUCT('pecresse' AS column_name, pecresse AS value),
      STRUCT('zemmour' AS column_name, zemmour AS value),
      STRUCT('dupontaignan' AS column_name, dupontaignan AS value),
      STRUCT('mlepen' AS column_name, mlepen AS value)
    ]) x,
    UNNEST([ 
      STRUCT('t2macron' AS column_name, t2macron AS value)
    ]) x2
  WHERE 
    x.value = GREATEST(
      arthaud, poutou, roussel, 
      melenchon, jadot, hidalgo, 
      lassalle, macron, pecresse, 
      zemmour, dupontaignan, mlepen
    )
    AND 
    x2.value = GREATEST(
      t2macron
    )
)
SELECT * 
FROM renamed
