{{ config(
    materialized='table'
) }}

with 

source as (

    select * from {{ source('projet_lewagon', 's_1_2002') }}

),

renamed as (

    select
        sondeur,
        `date`,
            SAFE_CAST(REPLACE(
        CASE 
            WHEN LENGTH(arlette_laguiller_lo) > 2 THEN SUBSTR(arlette_laguiller_lo, 1, LENGTH(arlette_laguiller_lo) - 2)
            ELSE arlette_laguiller_lo
        END, ',', '.') AS FLOAT64) AS arlette_laguiller_lo_float,
    
    SAFE_CAST(REPLACE(
        CASE 
            WHEN LENGTH(daniel_gluckstein_poi) > 2 THEN SUBSTR(daniel_gluckstein_poi, 1, LENGTH(daniel_gluckstein_poi) - 2)
            ELSE daniel_gluckstein_poi
        END, ',', '.') AS FLOAT64) AS daniel_gluckstein_poi_float,
    
    SAFE_CAST(REPLACE(
        CASE 
            WHEN LENGTH(olivier_besancenot_lcr) > 2 THEN SUBSTR(olivier_besancenot_lcr, 1, LENGTH(olivier_besancenot_lcr) - 2)
            ELSE olivier_besancenot_lcr
        END, ',', '.') AS FLOAT64) AS olivier_besancenot_lcr_float,
    
    SAFE_CAST(REPLACE(
        CASE 
            WHEN LENGTH(robert_hue_pcf) > 2 THEN SUBSTR(robert_hue_pcf, 1, LENGTH(robert_hue_pcf) - 2)
            ELSE robert_hue_pcf
        END, ',', '.') AS FLOAT64) AS robert_hue_pcf_float,
    
    SAFE_CAST(REPLACE(
        CASE 
            WHEN LENGTH(lionel_jospin_ps) > 2 THEN SUBSTR(lionel_jospin_ps, 1, LENGTH(lionel_jospin_ps) - 2)
            ELSE lionel_jospin_ps
        END, ',', '.') AS FLOAT64) AS lionel_jospin_ps_float,
    
    SAFE_CAST(REPLACE(
        CASE 
            WHEN LENGTH(christiane_taubira_prg) > 2 THEN SUBSTR(christiane_taubira_prg, 1, LENGTH(christiane_taubira_prg) - 2)
            ELSE christiane_taubira_prg
        END, ',', '.') AS FLOAT64) AS christiane_taubira_prg_float,
    
    SAFE_CAST(REPLACE(
        CASE 
            WHEN LENGTH(`noël_mamere_lv`) > 2 THEN SUBSTR(`noël_mamere_lv`, 1, LENGTH(`noël_mamere_lv`) - 2)
            ELSE `noël_mamere_lv`
        END, ',', '.') AS FLOAT64) AS noel_mamere_lv_float,
    
    SAFE_CAST(REPLACE(
        CASE 
            WHEN LENGTH(`jean-pierre_chevenement_mdc`) > 2 THEN SUBSTR(`jean-pierre_chevenement_mdc`, 1, LENGTH(`jean-pierre_chevenement_mdc`) - 2)
            ELSE `jean-pierre_chevenement_mdc`
        END, ',', '.') AS FLOAT64) AS jean_pierre_chevenement_mdc_float,
    
    SAFE_CAST(REPLACE(
        CASE 
            WHEN LENGTH(corinne_lepage_cap21) > 2 THEN SUBSTR(corinne_lepage_cap21, 1, LENGTH(corinne_lepage_cap21) - 2)
            ELSE corinne_lepage_cap21
        END, ',', '.') AS FLOAT64) AS corinne_lepage_cap21_float,
    
    SAFE_CAST(REPLACE(
        CASE 
            WHEN LENGTH(`françois_bayrou_udf`) > 2 THEN SUBSTR(`françois_bayrou_udf`, 1, LENGTH(`françois_bayrou_udf`) - 2)
            ELSE `françois_bayrou_udf`
        END, ',', '.') AS FLOAT64) AS francois_bayrou_udf_float,
    
    SAFE_CAST(REPLACE(
        CASE 
            WHEN LENGTH(christine_boutin_frs) > 2 THEN SUBSTR(christine_boutin_frs, 1, LENGTH(christine_boutin_frs) - 2)
            ELSE christine_boutin_frs
        END, ',', '.') AS FLOAT64) AS christine_boutin_frs_float,
    
    SAFE_CAST(REPLACE(
        CASE 
            WHEN LENGTH(jacques_chirac_rpr) > 2 THEN SUBSTR(jacques_chirac_rpr, 1, LENGTH(jacques_chirac_rpr) - 2)
            ELSE jacques_chirac_rpr
        END, ',', '.') AS FLOAT64) AS jacques_chirac_rpr_float,
    
    SAFE_CAST(REPLACE(
        CASE 
            WHEN LENGTH(alain_madelin_dl) > 2 THEN SUBSTR(alain_madelin_dl, 1, LENGTH(alain_madelin_dl) - 2)
            ELSE alain_madelin_dl
        END, ',', '.') AS FLOAT64) AS alain_madelin_dl_float,
    
    SAFE_CAST(REPLACE(
        CASE 
            WHEN LENGTH(`jean_saint-josse_cpnt`) > 2 THEN SUBSTR(`jean_saint-josse_cpnt`, 1, LENGTH(`jean_saint-josse_cpnt`) - 2)
            ELSE `jean_saint-josse_cpnt`
        END, ',', '.') AS FLOAT64) AS jean_saint_josse_cpnt_float,
    
    SAFE_CAST(REPLACE(
        CASE 
            WHEN LENGTH(`jean-marie_le_pen_fn`) > 2 THEN SUBSTR(`jean-marie_le_pen_fn`, 1, LENGTH(`jean-marie_le_pen_fn`) - 2)
            ELSE `jean-marie_le_pen_fn`
        END, ',', '.') AS FLOAT64) AS jean_marie_le_pen_fn_float,
    
    SAFE_CAST(REPLACE(
        CASE 
            WHEN LENGTH(bruno_megret_mnr) > 2 THEN SUBSTR(bruno_megret_mnr, 1, LENGTH(bruno_megret_mnr) - 2)
            ELSE bruno_megret_mnr
        END, ',', '.') AS FLOAT64) AS bruno_megret_mnr_float

    from source

)

select * from renamed
