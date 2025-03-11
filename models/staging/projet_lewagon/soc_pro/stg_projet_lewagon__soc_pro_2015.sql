with 

source as (

    select * from {{ source('projet_lewagon', 'soc_pro_2015') }}

),

renamed as (

    select
        region,
        departement,
        libelle_de departement,
        agriculteurs actifs_ayant_un_emploi rp2015,
        agriculteurs chômeurs rp2015,
        artisans__commercants__chefs_d_entreprise actifs_ayant_un_emploi rp2015,
        artisans__commercants__chefs_d_entreprise chômeurs rp2015,
        cadres_et_professions_intellectuelles_superieures actifs_ayant_un_emploi rp2015,
        cadres_et_professions_intellectuelles_superieures chômeurs rp2015,
        professions_intermediaires actifs_ayant_un_emploi rp2015,
        professions_intermediaires chômeurs rp2015,
        employes actifs_ayant_un_emploi rp2015,
        employes chômeurs rp2015,
        ouvriers actifs_ayant_un_emploi rp2015,
        ouvriers chômeurs rp2015

    from source

)

select * from renamed
