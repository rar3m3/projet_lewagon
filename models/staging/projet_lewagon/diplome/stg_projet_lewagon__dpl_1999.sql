with 

source as (

    select * from {{ source('projet_lewagon', 'dpl_1999') }}

),

renamed as (

    select
        region,
        departement,
        libelle_dedepartement,
        aucun_diplômehommes16_a_24_ansrp1999,
        aucun_diplômehommes25_ans_ou_plusrp1999,
        aucun_diplômefemmes16_a_24_ansrp1999,
        aucun_diplômefemmes25_ans_ou_plusrp1999,
        diplôme_de_niveau_cephommes16_a_24_ansrp1999,
        diplôme_de_niveau_cephommes25_ans_ou_plusrp1999,
        diplôme_de_niveau_cepfemmes16_a_24_ansrp1999,
        diplôme_de_niveau_cepfemmes25_ans_ou_plusrp1999,
        diplôme_de_niveau_bepchommes16_a_24_ansrp1999,
        diplôme_de_niveau_bepchommes25_ans_ou_plusrp1999,
        diplôme_de_niveau_bepcfemmes16_a_24_ansrp1999,
        diplôme_de_niveau_bepcfemmes25_ans_ou_plusrp1999,
        diplôme_de_niveau_cap-bephommes16_a_24_ansrp1999,
        diplôme_de_niveau_cap-bephommes25_ans_ou_plusrp1999,
        diplôme_de_niveau_cap-bepfemmes16_a_24_ansrp1999,
        diplôme_de_niveau_cap-bepfemmes25_ans_ou_plusrp1999,
        diplôme_de_niveau_bac_general_ou_techniquehommes16_a_24_ansrp1999,
        diplôme_de_niveau_bac_general_ou_techniquehommes25_ans_ou_plusrp1999,
        diplôme_de_niveau_bac_general_ou_techniquefemmes16_a_24_ansrp1999,
        diplôme_de_niveau_bac_general_ou_techniquefemmes25_ans_ou_plusrp1999,
        diplôme_universitaire_de_1er_cycle_hommes16_a_24_ansrp1999,
        diplôme_universitaire_de_1er_cycle_hommes25_ans_ou_plusrp1999,
        diplôme_universitaire_de_1er_cycle_femmes16_a_24_ansrp1999,
        diplôme_universitaire_de_1er_cycle_femmes25_ans_ou_plusrp1999,
        diplôme_universitaire_de_2eme_ou_3eme_cycle_hommes16_a_24_ansrp1999,
        diplôme_universitaire_de_2eme_ou_3eme_cycle_hommes25_ans_ou_plusrp1999,
        diplôme_universitaire_de_2eme_ou_3eme_cycle_femmes16_a_24_ansrp1999,
        diplôme_universitaire_de_2eme_ou_3eme_cycle_femmes25_ans_ou_plusrp1999

    from source

)

select * from renamed
