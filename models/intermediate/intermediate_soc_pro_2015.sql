{{ config(
    materialized='view'
) }}

WITH source AS (
    SELECT * FROM {{ source('projet_lewagon', 'soc_pro_2015') }}
    WHERE `Region` <> "RR23"
),

renamed AS (
    SELECT
        `Region`,
        `Departement`,
        `Libelle_de departement` AS Libelle_de_departement,
        CAST(`Agriculteurs Actifs_ayant_un_emploi RP2015` AS FLOAT64) AS agriculteurs_actifs_ayant_un_emploi_rp2015,
        CAST(`Agriculteurs Chômeurs RP2015` AS FLOAT64) AS agriculteurs_chomeurs_rp2015,
        CAST(`Artisans__commercants__chefs_d_entreprise Actifs_ayant_un_emploi RP2015` AS FLOAT64) AS artisans_commercants_chefs_d_entreprise_actifs_ayant_un_emploi_rp2015,
        CAST(`Artisans__commercants__chefs_d_entreprise Chômeurs RP2015` AS FLOAT64) AS artisans_commercants_chefs_d_entreprise_chomeurs_rp2015,
        CAST(`Cadres_et_professions_intellectuelles_superieures Actifs_ayant_un_emploi RP2015` AS FLOAT64) AS cadres_et_professions_intellectuelles_superieures_actifs_ayant_un_emploi_rp2015,
        CAST(`Cadres_et_professions_intellectuelles_superieures Chômeurs RP2015` AS FLOAT64) AS cadres_et_professions_intellectuelles_superieures_chomeurs_rp2015,
        CAST(`Professions_intermediaires Actifs_ayant_un_emploi RP2015` AS FLOAT64) AS professions_intermediaires_actifs_ayant_un_emploi_rp2015,
        CAST(`Professions_intermediaires Chômeurs RP2015` AS FLOAT64) AS professions_intermediaires_chomeurs_rp2015,
        CAST(`Employes Actifs_ayant_un_emploi RP2015` AS FLOAT64) AS employes_actifs_ayant_un_emploi_rp2015,
        CAST(`Employes Chômeurs RP2015` AS FLOAT64) AS employes_chomeurs_rp2015,
        CAST(`Ouvriers Actifs_ayant_un_emploi RP2015` AS FLOAT64) AS ouvriers_actifs_ayant_un_emploi_rp2015,
        CAST(`Ouvriers Chômeurs RP2015` AS FLOAT64) AS ouvriers_chomeurs_rp2015
    FROM source
)

SELECT * FROM renamed
