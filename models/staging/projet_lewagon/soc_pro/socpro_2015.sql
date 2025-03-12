{{ config(
    materialized='table'
) }}

WITH source AS (
    SELECT * FROM {{ ref('intermediate_soc_pro_2015') }}
),

renamed AS (
    SELECT
        `Region`,
        `Departement`,
        `Libelle_de departement`,
        (agriculteurs_actifs_ayant_un_emploi_rp2015 +
         artisans_commercants_chefs_d_entreprise_actifs_ayant_un_emploi_rp2015 +
         cadres_et_professions_intellectuelles_superieures_actifs_ayant_un_emploi_rp2015 +
         professions_intermediaires_actifs_ayant_un_emploi_rp2015 +
         employes_actifs_ayant_un_emploi_rp2015 +
         ouvriers_actifs_ayant_un_emploi_rp2015) AS total_actifs,
        (agriculteurs_chomeurs_rp2015 +
         artisans_commercants_chefs_d_entreprise_chomeurs_rp2015 +
         cadres_et_professions_intellectuelles_superieures_chomeurs_rp2015 +
         professions_intermediaires_chomeurs_rp2015 +
         employes_chomeurs_rp2015 +
         ouvriers_chomeurs_rp2015) AS total_chomeurs,
        (agriculteurs_chomeurs_rp2015 +
         agriculteurs_actifs_ayant_un_emploi_rp2015) AS total_agriculteurs,
        (artisans_commercants_chefs_d_entreprise_actifs_ayant_un_emploi_rp2015 +
         artisans_commercants_chefs_d_entreprise_chomeurs_rp2015) AS total_artisants_commercants_chefs,
        (cadres_et_professions_intellectuelles_superieures_actifs_ayant_un_emploi_rp2015 +
         cadres_et_professions_intellectuelles_superieures_chomeurs_rp2015) AS total_cadres,
        (professions_intermediaires_actifs_ayant_un_emploi_rp2015 +
         professions_intermediaires_chomeurs_rp2015) AS total_intermediaires,
        (employes_actifs_ayant_un_emploi_rp2015 +
         employes_chomeurs_rp2015) AS total_employes,
        (ouvriers_actifs_ayant_un_emploi_rp2015 +
         ouvriers_chomeurs_rp2015) AS total_ouvriers

    FROM source
)

SELECT * FROM renamed
