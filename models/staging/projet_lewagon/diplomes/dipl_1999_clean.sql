
SELECT
    PARSE_DATE('%Y',"1999") AS annee,
    departement,
    libelle_dedepartement,
    'M' AS sexe,
    (aucun_diplomehommes16_a_24_ansrp1999 + 
    aucun_diplomehommes25_ans_ou_plusrp1999) AS aucun_diplome,
    (diplome_de_niveau_cephommes16_a_24_ansrp1999 +
    diplome_de_niveau_cephommes25_ans_ou_plusrp1999 +
    diplome_de_niveau_bepchommes16_a_24_ansrp1999 + 
    diplome_de_niveau_bepchommes25_ans_ou_plusrp1999 +
    diplome_de_niveau_cap_bephommes16_a_24_ansrp1999 + 
    diplome_de_niveau_cap_bephommes25_ans_ou_plusrp1999 + diplome_de_niveau_bac_general_ou_techniquehommes16_a_24_ansrp1999 +
    diplome_de_niveau_bac_general_ou_techniquehommes25_ans_ou_plusrp1999) AS equivalant_au_baccalaureat,
    (diplome_universitaire_de_1er_cycle_hommes16_a_24_ansrp1999 +
    diplome_universitaire_de_1er_cycle_hommes25_ans_ou_plusrp1999 +
    diplome_universitaire_de_2eme_ou_3eme_cycle_hommes16_a_24_ansrp1999 + 
    diplome_universitaire_de_2eme_ou_3eme_cycle_hommes25_ans_ou_plusrp1999) AS niveau_universitaire
FROM {{ ref('stg_projet_lewagon__dpl_1999') }} -- must be dbt built
WHERE region <> "RR23"
UNION ALL
SELECT 
    PARSE_DATE('%Y',"1999") AS annee,
    departement,
    libelle_dedepartement,
    'F' AS sexe,
    (aucun_diplomefemmes16_a_24_ansrp1999 + 
    aucun_diplomefemmes25_ans_ou_plusrp1999) AS `aucun diplome`,
    (diplome_de_niveau_cepfemmes16_a_24_ansrp1999 +
    diplome_de_niveau_cepfemmes25_ans_ou_plusrp1999 +
    diplome_de_niveau_bepcfemmes16_a_24_ansrp1999 + 
    diplome_de_niveau_bepcfemmes25_ans_ou_plusrp1999 +
    diplome_de_niveau_cap_bepfemmes16_a_24_ansrp1999 + 
    diplome_de_niveau_cap_bepfemmes25_ans_ou_plusrp1999 +
    diplome_de_niveau_bac_general_ou_techniquefemmes16_a_24_ansrp1999 +
    diplome_de_niveau_bac_general_ou_techniquefemmes25_ans_ou_plusrp1999) AS `equivalant au baccalaureat`,
    (diplome_universitaire_de_1er_cycle_femmes16_a_24_ansrp1999 +
    diplome_universitaire_de_1er_cycle_femmes25_ans_ou_plusrp1999 +
    diplome_universitaire_de_2eme_ou_3eme_cycle_femmes16_a_24_ansrp1999 + 
    diplome_universitaire_de_2eme_ou_3eme_cycle_femmes25_ans_ou_plusrp1999) AS `niveau universitaire`
FROM {{ ref('stg_projet_lewagon__dpl_1999') }} --must be dbt built
WHERE region <> "RR23"