
    SELECT
        CAST("2010-01-01" AS date) AS annee,
        `Region`,
        `Departement`,
        `Libelle_de_departement`,
        CAST(agriculteurs_actifs_ayant_un_emploi_rp2010 +
         artisans_commercants_chefs_d_entreprise_actifs_ayant_un_emploi_rp2010 +
         cadres_et_professions_intellectuelles_superieures_actifs_ayant_un_emploi_rp2010 +
         professions_intermediaires_actifs_ayant_un_emploi_rp2010 +
         employes_actifs_ayant_un_emploi_rp2010 +
         ouvriers_actifs_ayant_un_emploi_rp2010 AS INT64) AS total_actifs,
        CAST(agriculteurs_chomeurs_rp2010 +
         artisans_commercants_chefs_d_entreprise_chomeurs_rp2010 +
         cadres_et_professions_intellectuelles_superieures_chomeurs_rp2010 +
         professions_intermediaires_chomeurs_rp2010 +
         employes_chomeurs_rp2010 +
         ouvriers_chomeurs_rp2010 AS INT64) AS total_chomeurs,
        CAST(agriculteurs_chomeurs_rp2010 +
         agriculteurs_actifs_ayant_un_emploi_rp2010 AS INT64) AS total_agriculteurs,
        CAST(artisans_commercants_chefs_d_entreprise_actifs_ayant_un_emploi_rp2010 +
         artisans_commercants_chefs_d_entreprise_chomeurs_rp2010 AS INT64) AS total_artisants_commercants_chefs,
        CAST(cadres_et_professions_intellectuelles_superieures_actifs_ayant_un_emploi_rp2010 +
         cadres_et_professions_intellectuelles_superieures_chomeurs_rp2010 AS INT64) AS total_cadres,
        CAST(professions_intermediaires_actifs_ayant_un_emploi_rp2010 +
         professions_intermediaires_chomeurs_rp2010 AS INT64) AS total_intermediaires,
        CAST(employes_actifs_ayant_un_emploi_rp2010 +
         employes_chomeurs_rp2010 AS INT64) AS total_employes,
        CAST(ouvriers_actifs_ayant_un_emploi_rp2010 +
         ouvriers_chomeurs_rp2010 AS INT64) AS total_ouvriers

    FROM {{ ref('intermediate_soc_pro_2010') }}

