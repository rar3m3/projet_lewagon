with 

source as (

    select * from {{ source('projet_lewagon', 'soc_pro_2021') }}

),

renamed as (

    select
        region,
        departement,
        libelle_de departement,
        agriculteurs actifs_ayant_un_emploi rp2021,
        agriculteurs chômeurs rp2021,
        artisans__commercants__chefs_d_entreprise actifs_ayant_un_emploi rp2021,
        artisans__commercants__chefs_d_entreprise chômeurs rp2021,
        cadres_et_professions_intellectuelles_superieures actifs_ayant_un_emploi rp2021,
        cadres_et_professions_intellectuelles_superieures chômeurs rp2021,
        professions_intermediaires actifs_ayant_un_emploi rp2021,
        professions_intermediaires chômeurs rp2021,
        employes actifs_ayant_un_emploi rp2021,
        employes chômeurs rp2021,
        ouvriers actifs_ayant_un_emploi rp2021,
        ouvriers chômeurs rp2021

    from source

)

select * from renamed
