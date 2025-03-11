with 

source as (

    select * from {{ source('projet_lewagon', 's_1_2007') }}

),

renamed as (

    select
        sondeur,
        date,
        arlette_laguiller_lo,
        olivier_besancenot_lcr,
        marie-george_buffet_pcf,
        gerard_schivardi_pt,
        segolene_royal_ps,
        jose_bove_dvg,
        dominique_voynet_lv,
        françois_bayrou_udf,
        nicolas_sarkozy_ump,
        philippe_de_villiers_mpf,
        frederic_nihous_cpnt,
        jean-marie_le_pen_fn

    from source

)

select * from renamed
