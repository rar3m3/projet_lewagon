with 

source as (

    select * from {{ source('projet_lewagon', 's_1_2002') }}

),

renamed as (

    select
        sondeur,
        date,
        arlette_laguiller_lo,
        daniel_gluckstein_poi,
        olivier_besancenot_lcr,
        robert_hue_pcf,
        lionel_jospin_ps,
        christiane_taubira_prg,
        noël_mamere_lv,
        jean-pierre_chevenement_mdc,
        corinne_lepage_cap21,
        françois_bayrou_udf,
        christine_boutin_frs,
        jacques_chirac_rpr,
        alain_madelin_dl,
        jean_saint-josse_cpnt,
        jean-marie_le_pen_fn,
        bruno_megret_mnr

    from source

)

select * from renamed
