{{ config(
    materialized='table'
) }}

WITH source AS (
  SELECT * FROM {{ ref('stg_projet_lewagon__pres2012') }}
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
      SUM(voixjoly) AS joly,
      SUM(voixhollande) AS hollande,
      SUM(voixcheminade) AS cheminade,
      SUM(voixbayrou) AS bayrou,
      SUM(voixsarkozy) AS sarkozy,
      SUM(voixdupontaignan) AS dupontaignan,
      SUM(voixmlepen) AS mlepen,
      SUM(voixt2hollande) AS t2hollande,
      SUM(voixt2sarkozy) AS t2sarkozy
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
      STRUCT('joly' AS column_name, joly AS value),
      STRUCT('hollande' AS column_name, hollande AS value),
      STRUCT('cheminade' AS column_name, cheminade AS value),
      STRUCT('bayrou' AS column_name, bayrou AS value),
      STRUCT('sarkozy' AS column_name, sarkozy AS value),
      STRUCT('dupontaignan' AS column_name, dupontaignan AS value),
      STRUCT('mlepen' AS column_name, mlepen AS value)
    ]) x,
    UNNEST([ 
      STRUCT('t2hollande' AS column_name, t2hollande AS value),
      STRUCT('t2sarkozy' AS column_name, t2sarkozy AS value)
    ]) x2
  WHERE 
    x.value = GREATEST(
      arthaud, poutou, melenchon, 
      joly, hollande, cheminade, 
      bayrou, sarkozy, dupontaignan, 
      mlepen
    )
    AND 
    x2.value = GREATEST(
      t2hollande, 
      t2sarkozy
    )
)
SELECT * 
FROM renamed
