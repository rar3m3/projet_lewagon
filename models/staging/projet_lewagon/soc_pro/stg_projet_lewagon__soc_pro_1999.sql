with 

source as (

    select * from {{ source('projet_lewagon', 'soc_pro_1999') }}

),

renamed as (

    select
        region,
        departement,
        libelle_de departement,
        agriculteurs actifs_ayant_un_emploi rp1999,
        agriculteurs chômeurs rp1999,
        artisans__commercants__chefs_d_entreprise actifs_ayant_un_emploi rp1999,
        artisans__commercants__chefs_d_entreprise chômeurs rp1999,
        cadres_et_professions_intellectuelles_superieures actifs_ayant_un_emploi rp1999,
        cadres_et_professions_intellectuelles_superieures chômeurs rp1999,
        professions_intermediaires actifs_ayant_un_emploi rp1999,
        professions_intermediaires chômeurs rp1999,
        employes actifs_ayant_un_emploi rp1999,
        employes chômeurs rp1999,
        ouvriers actifs_ayant_un_emploi rp1999,
        ouvriers chômeurs rp1999

    from source

)

select * from renamed
