{{ config(
    materialized='view'
) }}

WITH source AS (
    SELECT * FROM {{ source('projet_lewagon', 'soc_pro_1999') }}
WHERE `Region` <> "RR23"
),

renamed AS (
    SELECT
        `Region`,
        `Departement`,
        `Libelle_de departement` AS Libelle_de_departement,
        CAST(`Agriculteurs Actifs_ayant_un_emploi RP1999` AS INTEGER) AS agriculteurs_actifs_ayant_un_emploi_rp1999,
        CAST(`Agriculteurs Chômeurs RP1999` AS INTEGER) AS agriculteurs_chomeurs_rp1999,
        CAST(`Artisans__commercants__chefs_d_entreprise Actifs_ayant_un_emploi RP1999` AS INTEGER) AS artisans_commercants_chefs_d_entreprise_actifs_ayant_un_emploi_rp1999,
        CAST(`Artisans__commercants__chefs_d_entreprise Chômeurs RP1999` AS INTEGER) AS artisans_commercants_chefs_d_entreprise_chomeurs_rp1999,
        CAST(`Cadres_et_professions_intellectuelles_superieures Actifs_ayant_un_emploi RP1999` AS INTEGER) AS cadres_et_professions_intellectuelles_superieures_actifs_ayant_un_emploi_rp1999,
        CAST(`Cadres_et_professions_intellectuelles_superieures Chômeurs RP1999` AS INTEGER) AS cadres_et_professions_intellectuelles_superieures_chomeurs_rp1999,
        CAST(`Professions_intermediaires Actifs_ayant_un_emploi RP1999` AS INTEGER) AS professions_intermediaires_actifs_ayant_un_emploi_rp1999,
        CAST(`Professions_intermediaires Chômeurs RP1999` AS INTEGER) AS professions_intermediaires_chomeurs_rp1999,
        CAST(`Employes Actifs_ayant_un_emploi RP1999` AS INTEGER) AS employes_actifs_ayant_un_emploi_rp1999,
        CAST(`Employes Chômeurs RP1999` AS INTEGER) AS employes_chomeurs_rp1999,
        CAST(`Ouvriers Actifs_ayant_un_emploi RP1999` AS INTEGER) AS ouvriers_actifs_ayant_un_emploi_rp1999,
        CAST(`Ouvriers Chômeurs RP1999` AS INTEGER) AS ouvriers_chomeurs_rp1999,
        

    FROM source
)

SELECT * FROM renamed