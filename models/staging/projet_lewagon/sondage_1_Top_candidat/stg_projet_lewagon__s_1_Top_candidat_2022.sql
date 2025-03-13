{{ config(
    materialized='table'
) }}

with 

source as (

    select * from {{ source('projet_lewagon', 's_1_2022') }}

),

renamed as (

    select
        Sondeur,
        `Date`,
        `Échantillon`,
SAFE_CAST(REPLACE(
    CASE 
        WHEN LENGTH(Melenchon_LFI) > 2 THEN SUBSTR(Melenchon_LFI, 1, LENGTH(Melenchon_LFI) - 2)
        ELSE Melenchon_LFI
    END, ',', '.') AS FLOAT64) AS Melenchon_LFI,

SAFE_CAST(REPLACE(
    CASE 
        WHEN LENGTH(Hidalgo_PS) > 2 THEN SUBSTR(Hidalgo_PS, 1, LENGTH(Hidalgo_PS) - 2)
        ELSE Hidalgo_PS
    END, ',', '.') AS FLOAT64) AS Hidalgo_PS,

SAFE_CAST(REPLACE(
    CASE 
        WHEN LENGTH(`Jadot_EÉLV`) > 2 THEN SUBSTR(`Jadot_EÉLV`, 1, LENGTH(`Jadot_EÉLV`) - 2)
        ELSE `Jadot_EÉLV`
    END, ',', '.') AS FLOAT64) AS `Jadot_EÉLV`,

SAFE_CAST(REPLACE(
    CASE 
        WHEN LENGTH(Macron_LREM) > 2 THEN SUBSTR(Macron_LREM, 1, LENGTH(Macron_LREM) - 2)
        ELSE Macron_LREM
    END, ',', '.') AS FLOAT64) AS Macron_LREM,

SAFE_CAST(REPLACE(
    CASE 
        WHEN LENGTH(Pecresse_LR) > 2 THEN SUBSTR(Pecresse_LR, 1, LENGTH(Pecresse_LR) - 2)
        ELSE Pecresse_LR
    END, ',', '.') AS FLOAT64) AS Pecresse_LR,

SAFE_CAST(REPLACE(
    CASE 
        WHEN LENGTH(Le_pen_RN) > 2 THEN SUBSTR(Le_pen_RN, 1, LENGTH(Le_pen_RN) - 2)
        ELSE Le_pen_RN
    END, ',', '.') AS FLOAT64) AS Le_pen_RN,

SAFE_CAST(REPLACE(
    CASE 
        WHEN LENGTH(Zemmour_REC) > 2 THEN SUBSTR(Zemmour_REC, 1, LENGTH(Zemmour_REC) - 2)
        ELSE Zemmour_REC
    END, ',', '.') AS FLOAT64) AS Zemmour_REC


    from source

)

select * from renamed
