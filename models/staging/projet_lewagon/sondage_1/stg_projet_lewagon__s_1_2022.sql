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
            WHEN LENGTH(Arthaud_LO) > 2 THEN SUBSTR(Arthaud_LO, 1, LENGTH(Arthaud_LO) - 2)
            ELSE Arthaud_LO
        END, ',', '.') AS FLOAT64) AS Arthaud_LO_float,

    SAFE_CAST(REPLACE(
        CASE 
            WHEN LENGTH(Poutou_NPA) > 2 THEN SUBSTR(Poutou_NPA, 1, LENGTH(Poutou_NPA) - 2)
            ELSE Poutou_NPA
        END, ',', '.') AS FLOAT64) AS Poutou_NPA_float,

    SAFE_CAST(REPLACE(
        CASE 
            WHEN LENGTH(Roussel_PCF) > 2 THEN SUBSTR(Roussel_PCF, 1, LENGTH(Roussel_PCF) - 2)
            ELSE Roussel_PCF
        END, ',', '.') AS FLOAT64) AS Roussel_PCF_float,

    SAFE_CAST(REPLACE(
        CASE 
            WHEN LENGTH(Melenchon_LFI) > 2 THEN SUBSTR(Melenchon_LFI, 1, LENGTH(Melenchon_LFI) - 2)
            ELSE Melenchon_LFI
        END, ',', '.') AS FLOAT64) AS Melenchon_LFI_float,

    SAFE_CAST(REPLACE(
        CASE 
            WHEN LENGTH(Hidalgo_PS) > 2 THEN SUBSTR(Hidalgo_PS, 1, LENGTH(Hidalgo_PS) - 2)
            ELSE Hidalgo_PS
        END, ',', '.') AS FLOAT64) AS Hidalgo_PS_float,

    SAFE_CAST(REPLACE(
        CASE 
            WHEN LENGTH(`Jadot_EÉLV`) > 2 THEN SUBSTR(`Jadot_EÉLV`, 1, LENGTH(`Jadot_EÉLV`) - 2)
            ELSE `Jadot_EÉLV`
        END, ',', '.') AS FLOAT64) AS Jadot_EELV_float,

    SAFE_CAST(REPLACE(
        CASE 
            WHEN LENGTH(Macron_LREM) > 2 THEN SUBSTR(Macron_LREM, 1, LENGTH(Macron_LREM) - 2)
            ELSE Macron_LREM
        END, ',', '.') AS FLOAT64) AS Macron_LREM_float,

    SAFE_CAST(REPLACE(
        CASE 
            WHEN LENGTH(Pecresse_LR) > 2 THEN SUBSTR(Pecresse_LR, 1, LENGTH(Pecresse_LR) - 2)
            ELSE Pecresse_LR
        END, ',', '.') AS FLOAT64) AS Pecresse_LR_float,

    SAFE_CAST(REPLACE(
        CASE 
            WHEN LENGTH(`Lassalle_RES`) > 2 THEN SUBSTR(`Lassalle_RES`, 1, LENGTH(`Lassalle_RES`) - 2)
            ELSE `Lassalle_RES`
        END, ',', '.') AS FLOAT64) AS Lassalle_RES_float,

    SAFE_CAST(REPLACE(
        CASE 
            WHEN LENGTH(`Dupont-aignan_DLF`) > 2 THEN SUBSTR(`Dupont-aignan_DLF`, 1, LENGTH(`Dupont-aignan_DLF`) - 2)
            ELSE `Dupont-aignan_DLF`
        END, ',', '.') AS FLOAT64) AS Dupont_aignan_DLF_float,

    SAFE_CAST(REPLACE(
        CASE 
            WHEN LENGTH(Le_pen_RN) > 2 THEN SUBSTR(Le_pen_RN, 1, LENGTH(Le_pen_RN) - 2)
            ELSE Le_pen_RN
        END, ',', '.') AS FLOAT64) AS Le_pen_RN_float,

    SAFE_CAST(REPLACE(
        CASE 
            WHEN LENGTH(Zemmour_REC) > 2 THEN SUBSTR(Zemmour_REC, 1, LENGTH(Zemmour_REC) - 2)
            ELSE Zemmour_REC
        END, ',', '.') AS FLOAT64) AS Zemmour_REC_float

from source

)

select * from renamed
