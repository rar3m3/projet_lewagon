{{ config(
    materialized='table'
) }}

with 

source as (

    select * from {{ source('projet_lewagon', 's_1_2002') }}

),

renamed as (

select
    "2002" AS annee
    ,sondeur
    ,`date`
    ,arlette_laguiller_lo AS laguiller
    ,daniel_gluckstein_poi AS gluckstein
    ,olivier_besancenot_lcr AS besancenot
    ,robert_hue_pcf AS hue
    ,lionel_jospin_ps AS jospin
    ,christiane_taubira_PRG AS taubira
    ,noel_mamere_lv AS mamere
    ,jean_pierre_chevenement_mdc AS chevenement
    ,corinne_lepage_cap21 AS lepage
    ,francois_bayrou_udf AS bayrou
    ,christine_boutin_frs AS boutin
    ,jacques_chirac_rpr AS chirac
    ,alain_madelin_dl AS madelin
    ,jean_saint_josse_cpnt AS saint_josse
    ,jean_marie_le_pen_fn AS le_pen
    ,bruno_megret AS megret
from source

)
select * from renamed
