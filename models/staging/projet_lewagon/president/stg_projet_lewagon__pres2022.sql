{{ config(
    materialized='table'
) }}

with 

source as (

    select * from {{ source('projet_lewagon', 'pres2022') }}

),

renamed as (

    select
        CONCAT('FR-',dep) AS code_departement,
        nomdep,
        codecommune,
        nomcommune,
        inscrits,
        votants,
        exprimes,
        voixarthaud,
        voixpoutou,
        voixroussel,
        voixmelenchon,
        voixjadot,
        voixhidalgo,
        voixlassalle,
        voixmacron,
        voixpecresse,
        voixzemmour,
        voixdupontaignan,
        voixmlepen,
        voixt2macron,
        voixt2mlepen,
        COALESCE(SAFE_CAST(pvoixarthaud AS FLOAT64), 0) AS pvoixarthaud,
        COALESCE(SAFE_CAST(pvoixpoutou AS FLOAT64), 0) AS pvoixpoutou,
        COALESCE(SAFE_CAST(pvoixroussel AS FLOAT64), 0) AS pvoixroussel,
        COALESCE(SAFE_CAST(pvoixmelenchon AS FLOAT64), 0) AS pvoixmelenchon,
        COALESCE(SAFE_CAST(pvoixjadot AS FLOAT64), 0) AS pvoixjadot,
        COALESCE(SAFE_CAST(pvoixhidalgo AS FLOAT64), 0) AS pvoixhidalgo,
        COALESCE(SAFE_CAST(pvoixlassalle AS FLOAT64), 0) AS pvoixlassalle,
        COALESCE(SAFE_CAST(pvoixmacron AS FLOAT64), 0) AS pvoixmacron,
        COALESCE(SAFE_CAST(pvoixpecresse AS FLOAT64), 0) AS pvoixpecresse,
        COALESCE(SAFE_CAST(pvoixzemmour AS FLOAT64), 0) AS pvoixzemmour,
        COALESCE(SAFE_CAST(pvoixdupontaignan AS FLOAT64), 0) AS pvoixdupontaignan,
        COALESCE(SAFE_CAST(pvoixmlepen AS FLOAT64), 0) AS pvoixmlepen,
        COALESCE(SAFE_CAST(pvoixarthaudratio AS FLOAT64), 0) AS pvoixarthaudratio,
        COALESCE(SAFE_CAST(pvoixpoutouratio AS FLOAT64), 0) AS pvoixpoutouratio,
        COALESCE(SAFE_CAST(pvoixrousselratio AS FLOAT64), 0) AS pvoixrousselratio,
        COALESCE(SAFE_CAST(pvoixmelenchonratio AS FLOAT64), 0) AS pvoixmelenchonratio,
        COALESCE(SAFE_CAST(pvoixjadotratio AS FLOAT64), 0) AS pvoixjadotratio,
        COALESCE(SAFE_CAST(pvoixhidalgoratio AS FLOAT64), 0) AS pvoixhidalgoratio,
        COALESCE(SAFE_CAST(pvoixlassalleratio AS FLOAT64), 0) AS pvoixlassalleratio,
        COALESCE(SAFE_CAST(pvoixmacronratio AS FLOAT64), 0) AS pvoixmacronratio,
        COALESCE(SAFE_CAST(pvoixpecresseratio AS FLOAT64), 0) AS pvoixpecresseratio,
        COALESCE(SAFE_CAST(pvoixzemmourratio AS FLOAT64), 0) AS pvoixzemmourratio,
        COALESCE(SAFE_CAST(pvoixdupontaignanratio AS FLOAT64), 0) AS pvoixdupontaignanratio,
        COALESCE(SAFE_CAST(pvoixmlepenratio AS FLOAT64), 0) AS pvoixmlepenratio,
        pvoixt2macron,
        pvoixt2macronratio,
        pvoixt2mlepen,
        pvoixt2mlepenratio,
        voteg,
        votecg,
        votec,
        votecd,
        voted,
        votetg,
        votetd,
        votegcg,
        votedcd,
        COALESCE(SAFE_CAST(pvoteg AS FLOAT64), 0) AS pvoteg,
        COALESCE(SAFE_CAST(pvotecg AS FLOAT64), 0) AS pvotecg,
        COALESCE(SAFE_CAST(pvotec AS FLOAT64), 0) AS pvotec,
        COALESCE(SAFE_CAST(pvotecd AS FLOAT64), 0) AS pvotecd,
        COALESCE(SAFE_CAST(pvoted AS FLOAT64), 0) AS pvoted,
        COALESCE(SAFE_CAST(pvotetg AS FLOAT64), 0) AS pvotetg,
        COALESCE(SAFE_CAST(pvotetd AS FLOAT64), 0) AS pvotetd,
        COALESCE(SAFE_CAST(pvotegcg AS FLOAT64), 0) AS pvotegcg,
        COALESCE(SAFE_CAST(pvotedcd AS FLOAT64), 0) AS pvotedcd,
        COALESCE(SAFE_CAST(pvotegratio AS FLOAT64), 0) AS pvotegratio,
        COALESCE(SAFE_CAST(pvotecgratio AS FLOAT64), 0) AS pvotecgratio,
        COALESCE(SAFE_CAST(pvotecratio AS FLOAT64), 0) AS pvotecratio,
        COALESCE(SAFE_CAST(pvotecdratio AS FLOAT64), 0) AS pvotecdratio,
        COALESCE(SAFE_CAST(pvotedratio AS FLOAT64), 0) AS pvotedratio,
        COALESCE(SAFE_CAST(pvotegcgratio AS FLOAT64), 0) AS pvotegcgratio,
        COALESCE(SAFE_CAST( pvotedcdratio AS FLOAT64), 0) AS pvotedcdratio,
        COALESCE(SAFE_CAST(pvotetgratio AS FLOAT64), 0) AS pvotetgratio,
        COALESCE(SAFE_CAST(pvotetdratio AS FLOAT64), 0) AS pvotetdratio,
        pervoteg,
        pervotecg,
        pervotec,
        pervotecd,
        pervoted,
        pervotegcg,
        pervotedcd,
        pervotetg,
        pervotetd,
        plm,
        plmdoublon,
        ppar,
        perpar,
        pparratio,
        pabs,
        pblancsnuls,
        inscritst2,
        votantst2,
        exprimest2,
        pabst2,
        pblancsnulst2

    from source

)

select * from renamed
