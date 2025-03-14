select
    annee,
    sondeur,
    candidat,
    score
from {{ ref('stg_projet_lewagon__s_1_2002') }}
UNPIVOT (score for candidat in (arlette_laguiller_lo
                ,daniel_gluckstein_poi
                ,olivier_besancenot_lcr
                ,robert_hue_pcf
                ,lionel_jospin_ps
                ,christiane_taubira_prg
                ,noel_mamere_lv
                ,jean_pierre_chevenement_mdc
                ,corinne_lepage_cap21
                ,francois_bayrou_udf
                ,christine_boutin_frs
                ,jacques_chirac_rpr
                ,alain_madelin_dl
                ,jean_saint_josse_cpnt
                ,jean_marie_le_pen_fn
                ,bruno_megret))
UNION ALL
select
    annee,
    sondeur,
    candidat,
    score
from {{ ref('stg_projet_lewagon__s_1_2007') }}
UNPIVOT (score for candidat in (arlette_laguiller_lo
,olivier_besancenot_lcr
,marie_george_buffet_pcf
,gerard_schivardi_pt
,segolene_royal_ps
,jose_bove_dvg
,dominique_voynet_lv
,francois_bayrou
,nicolas_sarkozy
,philippe_de_villiers_mpf
,frederic_nihous_cpnt
,jean_marie_le_pen_fn))
UNION ALL
select
    annee,
    sondeur,
    candidat,
    score
from {{ ref('stg_projet_lewagon__s_1_2012') }}
UNPIVOT (score for candidat in (nathalie_arthaud_lo
,philippe_poutou_npa
,jean_luc_melenchon_fg
,francois_hollande_ps
,eva_joly_eelv
,francois_bayrou_modem
,nicolas_sarkozy_ump
,nicolas_dupont_aignan_dlr
,marine_le_pen_fn
,jacques_cheminade_sp))
UNION ALL
select
    annee,
    sondeur,
    candidat,
    score
from {{ ref('stg_projet_lewagon__s_1_2017') }}
UNPIVOT (score for candidat in (nathalie_arthaud_lo
,philippe_poutou_npa
,jean_luc_melenchon_lfi
,benoit_hamon_ps
,emmanuel_macron_em
,jean_lassalle_res
,francois_fillon_lr
,nicolas_dupont_aignan_dlf
,francois_asselineau_upr
,marine_le_pen_fn
,jacques_cheminade_sp))
UNION ALL
select
    annee,
    sondeur,
    candidat,
    score
from {{ ref('stg_projet_lewagon__s_1_2022') }}
UNPIVOT (score for candidat in (Arthaud_LO
,Poutou_NPA
,Roussel_PCF
,Melenchon_LFI
,Hidalgo_PS
,Jadot_EELV
,Macron_LREM
,Pecresse_LR
,Lassalle_RES
,Dupont_aignan_DLF
,Le_pen_RN
,Zemmour_REC))
