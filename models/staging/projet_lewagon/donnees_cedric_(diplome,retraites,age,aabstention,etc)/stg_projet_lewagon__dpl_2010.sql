with 

source as (

    select * from {{ source('projet_lewagon', 'dpl_2010') }}

),

renamed as (

SELECT
    region,
    departement,
    libelle_dedepartement,
    COALESCE(ROUND(SAFE_CAST(`aucun_diplômehommes16_a_24_ansrp2010` AS FLOAT64), 0),0) AS aucun_diplomehommes16_a_24_ansrp2010,
    COALESCE(ROUND(SAFE_CAST(`aucun_diplômehommes25_ans_ou_plusrp2010` AS FLOAT64), 0),0) AS aucun_diplomehommes25_ans_ou_plusrp2010,
    COALESCE(ROUND(SAFE_CAST(`aucun_diplômefemmes16_a_24_ansrp2010` AS FLOAT64), 0),0) AS aucun_diplomefemmes16_a_24_ansrp2010,
    COALESCE(ROUND(SAFE_CAST(`aucun_diplômefemmes25_ans_ou_plusrp2010` AS FLOAT64), 0),0) AS aucun_diplomefemmes25_ans_ou_plusrp2010,
    COALESCE(ROUND(SAFE_CAST(`diplôme_de_niveau_cephommes16_a_24_ansrp2010` AS FLOAT64), 0),0) AS diplome_de_niveau_cephommes16_a_24_ansrp2010,
    COALESCE(ROUND(SAFE_CAST(`diplôme_de_niveau_cephommes25_ans_ou_plusrp2010` AS FLOAT64), 0),0) AS diplome_de_niveau_cephommes25_ans_ou_plusrp2010,
    COALESCE(ROUND(SAFE_CAST(`diplôme_de_niveau_cepfemmes16_a_24_ansrp2010` AS FLOAT64), 0),0) AS diplome_de_niveau_cepfemmes16_a_24_ansrp2010,
    COALESCE(ROUND(SAFE_CAST(`diplôme_de_niveau_cepfemmes25_ans_ou_plusrp2010` AS FLOAT64), 0),0) AS diplome_de_niveau_cepfemmes25_ans_ou_plusrp2010,
    COALESCE(ROUND(SAFE_CAST(`diplôme_de_niveau_bepchommes16_a_24_ansrp2010` AS FLOAT64), 0),0) AS diplome_de_niveau_bepchommes16_a_24_ansrp2010,
    COALESCE(ROUND(SAFE_CAST(`diplôme_de_niveau_bepchommes25_ans_ou_plusrp2010` AS FLOAT64), 0),0) AS diplome_de_niveau_bepchommes25_ans_ou_plusrp2010,
    COALESCE(ROUND(SAFE_CAST(`diplôme_de_niveau_bepcfemmes16_a_24_ansrp2010` AS FLOAT64), 0),0) AS diplome_de_niveau_bepcfemmes16_a_24_ansrp2010,
    COALESCE(ROUND(SAFE_CAST(`diplôme_de_niveau_bepcfemmes25_ans_ou_plusrp2010` AS FLOAT64), 0),0) AS diplome_de_niveau_bepcfemmes25_ans_ou_plusrp2010,
    COALESCE(ROUND(SAFE_CAST(`diplôme_de_niveau_cap-bephommes16_a_24_ansrp2010` AS FLOAT64), 0),0) AS diplome_de_niveau_cap_bephommes16_a_24_ansrp2010,
    COALESCE(ROUND(SAFE_CAST(`diplôme_de_niveau_cap-bephommes25_ans_ou_plusrp2010` AS FLOAT64), 0),0) AS diplome_de_niveau_cap_bephommes25_ans_ou_plusrp2010,
    COALESCE(ROUND(SAFE_CAST(`diplôme_de_niveau_cap-bepfemmes16_a_24_ansrp2010` AS FLOAT64), 0),0) AS diplome_de_niveau_cap_bepfemmes16_a_24_ansrp2010,
    COALESCE(ROUND(SAFE_CAST(`diplôme_de_niveau_cap-bepfemmes25_ans_ou_plusrp2010` AS FLOAT64), 0),0) AS diplome_de_niveau_cap_bepfemmes25_ans_ou_plusrp2010,
    COALESCE(ROUND(SAFE_CAST(`diplôme_de_niveau_bac_general_ou_techniquehommes16_a_24_ansrp2010` AS FLOAT64), 0),0) AS diplome_de_niveau_bac_general_ou_techniquehommes16_a_24_ansrp2010,
    COALESCE(ROUND(SAFE_CAST(`diplôme_de_niveau_bac_general_ou_techniquehommes25_ans_ou_plusrp2010` AS FLOAT64), 0),0) AS diplome_de_niveau_bac_general_ou_techniquehommes25_ans_ou_plusrp2010,
    COALESCE(ROUND(SAFE_CAST(`diplôme_de_niveau_bac_general_ou_techniquefemmes16_a_24_ansrp2010` AS FLOAT64), 0),0) AS diplome_de_niveau_bac_general_ou_techniquefemmes16_a_24_ansrp2010,
    COALESCE(ROUND(SAFE_CAST(`diplôme_de_niveau_bac_general_ou_techniquefemmes25_ans_ou_plusrp2010` AS FLOAT64), 0),0) AS diplome_de_niveau_bac_general_ou_techniquefemmes25_ans_ou_plusrp2010,
    COALESCE(ROUND(SAFE_CAST(`diplôme_universitaire_de_1er_cycle_hommes16_a_24_ansrp2010` AS FLOAT64), 0),0) AS diplome_universitaire_de_1er_cycle_hommes16_a_24_ansrp2010,
    COALESCE(ROUND(SAFE_CAST(`diplôme_universitaire_de_1er_cycle_hommes25_ans_ou_plusrp2010` AS FLOAT64), 0),0) AS diplome_universitaire_de_1er_cycle_hommes25_ans_ou_plusrp2010,
    COALESCE(ROUND(SAFE_CAST(`diplôme_universitaire_de_1er_cycle_femmes16_a_24_ansrp2010` AS FLOAT64), 0),0) AS diplome_universitaire_de_1er_cycle_femmes16_a_24_ansrp2010,
    COALESCE(ROUND(SAFE_CAST(`diplôme_universitaire_de_1er_cycle_femmes25_ans_ou_plusrp2010` AS FLOAT64), 0),0) AS diplome_universitaire_de_1er_cycle_femmes25_ans_ou_plusrp2010,
    COALESCE(ROUND(SAFE_CAST(`diplôme_universitaire_de_2eme_ou_3eme_cycle_hommes16_a_24_ansrp2010` AS FLOAT64), 0),0) AS diplome_universitaire_de_2eme_ou_3eme_cycle_hommes16_a_24_ansrp2010,
    COALESCE(ROUND(SAFE_CAST(`diplôme_universitaire_de_2eme_ou_3eme_cycle_hommes25_ans_ou_plusrp2010` AS FLOAT64), 0),0) AS diplome_universitaire_de_2eme_ou_3eme_cycle_hommes25_ans_ou_plusrp2010,
    COALESCE(ROUND(SAFE_CAST(`diplôme_universitaire_de_2eme_ou_3eme_cycle_femmes16_a_24_ansrp2010` AS FLOAT64), 0),0) AS diplome_universitaire_de_2eme_ou_3eme_cycle_femmes16_a_24_ansrp2010,
    COALESCE(ROUND(SAFE_CAST(`diplôme_universitaire_de_2eme_ou_3eme_cycle_femmes25_ans_ou_plusrp2010` AS FLOAT64), 0),0) AS diplome_universitaire_de_2eme_ou_3eme_cycle_femmes25_ans_ou_plusrp2010
from source

)

select * from renamed
