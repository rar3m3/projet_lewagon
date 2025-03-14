{{ config(
    materialized='table'
) }}

WITH source AS (
  SELECT * FROM {{ ref('stg_projet_lewagon__pres2002') }}
),
renamed AS (
  WITH aggregated_data AS (
    SELECT
      "2002" AS annee,
      code_departement,
      SUM(inscrits) AS inscrits,
      SUM(votants) AS votants,
      SUM(exprimes) AS exprimes,
      SUM(voixlaguiller) AS laguiller,
      SUM(voixbesancenot) AS besancenot,
      SUM(voixgluckstein) AS gluckstein,
      SUM(voixhue) AS hue,
      SUM(voixtaubira) AS taubira,
      SUM(voixmamere) AS mamere,
      SUM(voixchevenement) AS chevenement,
      SUM(voixjospin) AS jospin,
      SUM(voixlepage) AS lepage,
      SUM(voixsaintjosse) AS saintjosse,
      SUM(voixbayrou) AS bayrou,
      SUM(voixchirac) AS chirac,
      SUM(voixmadelin) AS madelin,
      SUM(voixboutin) AS boutin,
      SUM(voixmegret) AS megret,
      SUM(voixlepen) AS lepen,
      SUM(voixt2chirac) AS t2chirac,
      SUM(voixt2lepen) AS t2lepen
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
      STRUCT('laguiller' AS column_name, laguiller AS value),
      STRUCT('besancenot' AS column_name, besancenot AS value),
      STRUCT('gluckstein' AS column_name, gluckstein AS value),
      STRUCT('hue' AS column_name, hue AS value),
      STRUCT('taubira' AS column_name, taubira AS value),
      STRUCT('mamere' AS column_name, mamere AS value),
      STRUCT('chevenement' AS column_name, chevenement AS value),
      STRUCT('jospin' AS column_name, jospin AS value),
      STRUCT('lepage' AS column_name, lepage AS value),
      STRUCT('saintjosse' AS column_name, saintjosse AS value),
      STRUCT('bayrou' AS column_name, bayrou AS value),
      STRUCT('chirac' AS column_name, chirac AS value),
      STRUCT('madelin' AS column_name, madelin AS value),
      STRUCT('boutin' AS column_name, boutin AS value),
      STRUCT('megret' AS column_name, megret AS value),
      STRUCT('lepen' AS column_name, lepen AS value)
    ]) x,
    UNNEST([
      STRUCT('t2chirac' AS column_name, t2chirac AS value),
      STRUCT('t2lepen' AS column_name, t2lepen AS value)
    ]) x2
  WHERE 
    x.value = GREATEST(
      laguiller, besancenot, gluckstein, 
      hue, taubira, mamere, 
      chevenement, jospin, lepage, 
      saintjosse, bayrou, chirac, 
      madelin, boutin, megret, 
      lepen
    )
    AND 
    x2.value = GREATEST(
      t2chirac, 
      t2lepen
    )
)
SELECT * 
FROM renamed
