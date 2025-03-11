with 

source as (

    select * from {{ source('projet_lewagon', 'soc_pro_2010') }}

),

renamed as (

    select
        region,
        departement,
        libelle_de departement,
        agriculteurs actifs_ayant_un_emploi rp2010,
        agriculteurs chômeurs rp2010,
        artisans__commercants__chefs_d_entreprise actifs_ayant_un_emploi rp2010,
        artisans__commercants__chefs_d_entreprise chômeurs rp2010,
        cadres_et_professions_intellectuelles_superieures actifs_ayant_un_emploi rp2010,
        cadres_et_professions_intellectuelles_superieures chômeurs rp2010,
        professions_intermediaires actifs_ayant_un_emploi rp2010,
        professions_intermediaires chômeurs rp2010,
        employes actifs_ayant_un_emploi rp2010,
        employes chômeurs rp2010,
        ouvriers actifs_ayant_un_emploi rp2010,
        ouvriers chômeurs rp2010

    from source

)

select * from renamed
