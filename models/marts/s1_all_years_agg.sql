
{{ config(
    materialized='table'
) }}

WITH agg_table AS (
    select
        PARSE_DATE('%Y',annee) AS annee,
        sondeur,
        candidat,
        score
    from {{ ref('stg_projet_lewagon__s_1_2002') }}
    UNPIVOT (score for candidat in (laguiller
                    ,gluckstein
                    ,besancenot
                    ,hue
                    ,jospin
                    ,taubira
                    ,mamere
                    ,chevenement
                    ,lepage
                    ,bayrou
                    ,boutin
                    ,chirac
                    ,madelin
                    ,saint_josse
                    ,le_pen
                    ,megret))
    UNION ALL
    select
        PARSE_DATE('%Y',annee) AS annee,
        sondeur,
        candidat,
        score
    from {{ ref('stg_projet_lewagon__s_1_2007') }}
    UNPIVOT (score for candidat in (laguiller
    ,besancenot
    ,buffet
    ,schivardi
    ,royal
    ,bove
    ,voynet
    ,bayrou
    ,sarkozy
    ,de_villiers
    ,nihous
    ,le_pen))
    UNION ALL
    select
        PARSE_DATE('%Y',annee) AS annee,
        sondeur,
        candidat,
        score
    from {{ ref('stg_projet_lewagon__s_1_2012') }}
    UNPIVOT (score for candidat in (arthaud
    ,poutou
    ,melenchon
    ,hollande
    ,joly
    ,bayrou
    ,sarkozy
    ,dupont_aignan
    ,le_pen
    ,cheminade))
    UNION ALL
    select
        PARSE_DATE('%Y',annee) AS annee,
        sondeur,
        candidat,
        score
    from {{ ref('stg_projet_lewagon__s_1_2017') }}
    UNPIVOT (score for candidat in (arthaud
    ,poutou
    ,melenchon
    ,hamon
    ,macron
    ,lassalle
    ,fillon
    ,dupont_aignan
    ,asselineau
    ,le_pen
    ,cheminade))
    UNION ALL
    select
        PARSE_DATE('%Y',annee) AS annee,
        sondeur,
        candidat,
        score
    from {{ ref('stg_projet_lewagon__s_1_2022') }}
    UNPIVOT (score for candidat in (arthaud
    ,poutou
    ,roussel
    ,melenchon
    ,hidalgo
    ,jadot
    ,macron
    ,pecresse
    ,lassalle
    ,dupont_aignan
    ,le_pen
    ,zemmour))
)
SELECT
    *
    , CASE
        ------2002
        WHEN (candidat LIKE "%laguiller%" AND annee = "2002-01-01") THEN "Extreme Gauche"
        WHEN (candidat LIKE "%gluckstein%" AND annee = "2002-01-01") THEN "Extreme Gauche"
        WHEN (candidat LIKE "%besancenot%" AND annee = "2002-01-01") THEN "Extreme Gauche"
        WHEN (candidat LIKE "%hue%" AND annee = "2002-01-01") THEN "Extreme Gauche"
        WHEN (candidat LIKE "%jospin%" AND annee = "2002-01-01") THEN "Gauche"
        WHEN (candidat LIKE "%taubira%" AND annee = "2002-01-01") THEN "Gauche"
        WHEN (candidat LIKE "%mamere%" AND annee = "2002-01-01") THEN "Ecolo"
        WHEN (candidat LIKE "%chevenement%" AND annee = "2002-01-01") THEN "Gauche"
        WHEN (candidat LIKE "%lepage%" AND annee = "2002-01-01") THEN "Ecolo"
        WHEN (candidat LIKE "%bayrou%" AND annee = "2002-01-01") THEN "Centre"
        WHEN (candidat LIKE "%boutin%" AND annee = "2002-01-01") THEN "Droite"
        WHEN (candidat LIKE "%chirac%" AND annee = "2002-01-01") THEN "Droite"
        WHEN (candidat LIKE "%madelin%" AND annee = "2002-01-01") THEN "Droite"
        WHEN (candidat LIKE "%saint_josse%" AND annee = "2002-01-01") THEN "Droite"
        WHEN (candidat LIKE "%le_pen%" AND annee = "2002-01-01") THEN "Extreme Droite"
        WHEN (candidat LIKE "%megret%" AND annee = "2002-01-01") THEN "Extreme Droite"
        ------2007
        WHEN (candidat LIKE "%laguiller%" AND annee = "2007-01-01") THEN "Extreme Gauche"
        WHEN (candidat LIKE "%besancenot%" AND annee = "2007-01-01") THEN "Extreme Gauche"
        WHEN (candidat LIKE "%buffet%" AND annee = "2007-01-01") THEN "Extreme Gauche"
        WHEN (candidat LIKE "%schivardi%" AND annee = "2007-01-01") THEN "Extreme Gauche"
        WHEN (candidat LIKE "%royal%" AND annee = "2007-01-01") THEN "Gauche"
        WHEN (candidat LIKE "%bove%" AND annee = "2007-01-01") THEN "Gauche"
        WHEN (candidat LIKE "%voynet%" AND annee = "2007-01-01") THEN "Ecolo"
        WHEN (candidat LIKE "%bayrou%" AND annee = "2007-01-01") THEN "Centre"
        WHEN (candidat LIKE "%sarkozy%" AND annee = "2007-01-01") THEN "Droite"
        WHEN (candidat LIKE "%villiers%" AND annee = "2007-01-01") THEN "Droite"
        WHEN (candidat LIKE "%nihous%" AND annee = "2007-01-01") THEN "Droite"
        WHEN (candidat LIKE "%le_pen%" AND annee = "2007-01-01") THEN "Extreme Droite"
        -------2012
        WHEN (candidat LIKE "%arthaud%" AND annee = "2012-01-01") THEN "Extreme Gauche"
        WHEN (candidat LIKE "%poutou%" AND annee = "2012-01-01") THEN "Extreme Gauche"
        WHEN (candidat LIKE "%melenchon%" AND annee = "2012-01-01") THEN "Extreme Gauche"
        WHEN (candidat LIKE "%hollande%" AND annee = "2012-01-01") THEN "Gauche"
        WHEN (candidat LIKE "%joly%" AND annee = "2012-01-01") THEN "Ecolo"
        WHEN (candidat LIKE "%bayrou%" AND annee = "2012-01-01") THEN "Centre"
        WHEN (candidat LIKE "%sarkozy%" AND annee = "2012-01-01") THEN "Droite"
        WHEN (candidat LIKE "%dupont_aignan%" AND annee = "2012-01-01") THEN "Droite"
        WHEN (candidat LIKE "%le_pen%" AND annee = "2012-01-01") THEN "Extreme Droite"
        WHEN (candidat LIKE "%cheminade%" AND annee = "2012-01-01") THEN "Autre"
        -------2017
        WHEN (candidat LIKE "%arthaud%" AND annee = "2017-01-01") THEN "Extreme Gauche"
        WHEN (candidat LIKE "%poutou%" AND annee = "2017-01-01") THEN "Extreme Gauche"
        WHEN (candidat LIKE "%melenchon%" AND annee = "2017-01-01") THEN "Extreme Gauche"
        WHEN (candidat LIKE "%hamon%" AND annee = "2017-01-01") THEN "Gauche"
        WHEN (candidat LIKE "%macron%" AND annee = "2017-01-01") THEN "Centre"
        WHEN (candidat LIKE "%lassalle%" AND annee = "2017-01-01") THEN "Centre"
        WHEN (candidat LIKE "%fillon%" AND annee = "2017-01-01") THEN "Droite"
        WHEN (candidat LIKE "%dupont_aignan%" AND annee = "2017-01-01") THEN "Droite"
        WHEN (candidat LIKE "%asselineau%" AND annee = "2017-01-01") THEN "Autre"
        WHEN (candidat LIKE "%le_pen%" AND annee = "2017-01-01") THEN "Extreme Droite"
        WHEN (candidat LIKE "%cheminade%" AND annee = "2017-01-01") THEN "Autre"
        -------2022
        WHEN (candidat LIKE "%arthaud%" AND annee = "2022-01-01") THEN "Extreme Gauche"
        WHEN (candidat LIKE "%poutou%" AND annee = "2022-01-01") THEN "Extreme Gauche"
        WHEN (candidat LIKE "%roussel%" AND annee = "2022-01-01") THEN "Extreme Gauche"
        WHEN (candidat LIKE "%melenchon%" AND annee = "2022-01-01") THEN "Extreme Gauche"
        WHEN (candidat LIKE "%hidalgo%" AND annee = "2022-01-01") THEN "Gauche"
        WHEN (candidat LIKE "%jadot%" AND annee = "2022-01-01") THEN "Ecolo"
        WHEN (candidat LIKE "%macron%" AND annee = "2022-01-01") THEN "Centre"
        WHEN (candidat LIKE "%pecresse%" AND annee = "2022-01-01") THEN "Droite"
        WHEN (candidat LIKE "%lassalle%" AND annee = "2022-01-01") THEN "Centre"
        WHEN (candidat LIKE "%dupont_aignan%" AND annee = "2022-01-01") THEN "Droite"
        WHEN (candidat LIKE "%le_pen%" AND annee = "2022-01-01") THEN "Extreme Droite"
        WHEN (candidat LIKE "%zemmour%" AND annee = "2022-01-01") THEN "Extreme Droite"
    END AS affiliation
    , CASE
        ------2002
        WHEN (candidat LIKE "%laguiller%" AND annee = "2002-01-01") THEN "LO"
        WHEN (candidat LIKE "%gluckstein%" AND annee = "2002-01-01") THEN "POI"
        WHEN (candidat LIKE "%besancenot%" AND annee = "2002-01-01") THEN "LCR"
        WHEN (candidat LIKE "%hue%" AND annee = "2002-01-01") THEN "PCF"
        WHEN (candidat LIKE "%jospin%" AND annee = "2002-01-01") THEN "PS"
        WHEN (candidat LIKE "%taubira%" AND annee = "2002-01-01") THEN "PRG"
        WHEN (candidat LIKE "%mamere%" AND annee = "2002-01-01") THEN "LV/EELV"
        WHEN (candidat LIKE "%chevenement%" AND annee = "2002-01-01") THEN "MDC"
        WHEN (candidat LIKE "%lepage%" AND annee = "2002-01-01") THEN "Cap21"
        WHEN (candidat LIKE "%bayrou%" AND annee = "2002-01-01") THEN "UDF"
        WHEN (candidat LIKE "%boutin%" AND annee = "2002-01-01") THEN "FRS"
        WHEN (candidat LIKE "%chirac%" AND annee = "2002-01-01") THEN "RPR"
        WHEN (candidat LIKE "%madelin%" AND annee = "2002-01-01") THEN "DL"
        WHEN (candidat LIKE "%saint_josse%" AND annee = "2002-01-01") THEN "CPNT"
        WHEN (candidat LIKE "%le_pen%" AND annee = "2002-01-01") THEN "FN/RN"
        WHEN (candidat LIKE "%megret%" AND annee = "2002-01-01") THEN "MNR"
        ------2007
        WHEN (candidat LIKE "%laguiller%" AND annee = "2007-01-01") THEN "LO"
        WHEN (candidat LIKE "%besancenot%" AND annee = "2007-01-01") THEN "LCR"
        WHEN (candidat LIKE "%buffet%" AND annee = "2007-01-01") THEN "PCF"
        WHEN (candidat LIKE "%schivardi%" AND annee = "2007-01-01") THEN "PT"
        WHEN (candidat LIKE "%royal%" AND annee = "2007-01-01") THEN "PS"
        WHEN (candidat LIKE "%bove%" AND annee = "2007-01-01") THEN "DVG"
        WHEN (candidat LIKE "%voynet%" AND annee = "2007-01-01") THEN "LV/EELV"
        WHEN (candidat LIKE "%bayrou%" AND annee = "2007-01-01") THEN "UDF"
        WHEN (candidat LIKE "%sarkozy%" AND annee = "2007-01-01") THEN "UMP"
        WHEN (candidat LIKE "%villiers%" AND annee = "2007-01-01") THEN "MPF"
        WHEN (candidat LIKE "%nihous%" AND annee = "2007-01-01") THEN "CPNT"
        WHEN (candidat LIKE "%le_pen%" AND annee = "2007-01-01") THEN "FN/RN"
        -------2012
        WHEN (candidat LIKE "%arthaud%" AND annee = "2012-01-01") THEN "LO"
        WHEN (candidat LIKE "%poutou%" AND annee = "2012-01-01") THEN "NPA"
        WHEN (candidat LIKE "%melenchon%" AND annee = "2012-01-01") THEN "FG"
        WHEN (candidat LIKE "%hollande%" AND annee = "2012-01-01") THEN "PS"
        WHEN (candidat LIKE "%joly%" AND annee = "2012-01-01") THEN "LV/EELV"
        WHEN (candidat LIKE "%bayrou%" AND annee = "2012-01-01") THEN "MoDem"
        WHEN (candidat LIKE "%sarkozy%" AND annee = "2012-01-01") THEN "UMP"
        WHEN (candidat LIKE "%dupont_aignan%" AND annee = "2012-01-01") THEN "DLR"
        WHEN (candidat LIKE "%le_pen%" AND annee = "2012-01-01") THEN "FN/RN"
        WHEN (candidat LIKE "%cheminade%" AND annee = "2012-01-01") THEN "S&P"
        -------2017
        WHEN (candidat LIKE "%arthaud%" AND annee = "2017-01-01") THEN "LO"
        WHEN (candidat LIKE "%poutou%" AND annee = "2017-01-01") THEN "NPA"
        WHEN (candidat LIKE "%melenchon%" AND annee = "2017-01-01") THEN "LFI"
        WHEN (candidat LIKE "%hamon%" AND annee = "2017-01-01") THEN "PS"
        WHEN (candidat LIKE "%macron%" AND annee = "2017-01-01") THEN "EM/LREM"
        WHEN (candidat LIKE "%lassalle%" AND annee = "2017-01-01") THEN "RES"
        WHEN (candidat LIKE "%fillon%" AND annee = "2017-01-01") THEN "LR"
        WHEN (candidat LIKE "%dupont_aignan%" AND annee = "2017-01-01") THEN "DLF"
        WHEN (candidat LIKE "%asselineau%" AND annee = "2017-01-01") THEN "UPR"
        WHEN (candidat LIKE "%le_pen%" AND annee = "2017-01-01") THEN "FN/RN"
        WHEN (candidat LIKE "%cheminade%" AND annee = "2017-01-01") THEN "S&P"
        -------2022
        WHEN (candidat LIKE "%arthaud%" AND annee = "2022-01-01") THEN "LO"
        WHEN (candidat LIKE "%poutou%" AND annee = "2022-01-01") THEN "NPA"
        WHEN (candidat LIKE "%roussel%" AND annee = "2022-01-01") THEN "PCF"
        WHEN (candidat LIKE "%melenchon%" AND annee = "2022-01-01") THEN "LFI"
        WHEN (candidat LIKE "%hidalgo%" AND annee = "2022-01-01") THEN "PS"
        WHEN (candidat LIKE "%jadot%" AND annee = "2022-01-01") THEN "LV/EELV"
        WHEN (candidat LIKE "%macron%" AND annee = "2022-01-01") THEN "EM/LREM"
        WHEN (candidat LIKE "%pecresse%" AND annee = "2022-01-01") THEN "LR"
        WHEN (candidat LIKE "%lassalle%" AND annee = "2022-01-01") THEN "RES"
        WHEN (candidat LIKE "%dupont_aignan%" AND annee = "2022-01-01") THEN "DLF"
        WHEN (candidat LIKE "%le_pen%" AND annee = "2022-01-01") THEN "FN/RN"
        WHEN (candidat LIKE "%zemmour%" AND annee = "2022-01-01") THEN "REC"
    END AS parti
From agg_table
