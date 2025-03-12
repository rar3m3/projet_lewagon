{{ config(
    materialized='table'
) }}

with 

source as (

    select * from {{ source('projet_lewagon', 'pres2002') }}

),

renamed as (

    select
        dep,
        nomdep,
        codecommune,
        nomcommune,
        COALESCE(SAFE_CAST(inscrits AS INT64), 0) AS inscrits,
        COALESCE(SAFE_CAST(votants AS INT64), 0) AS votants,
        COALESCE(SAFE_CAST(exprimes AS INT64), 0) AS exprimes,
        COALESCE(SAFE_CAST(voixlaguiller AS INT64), 0) AS voixlaguiller,
        COALESCE(SAFE_CAST(voixbesancenot AS INT64), 0) AS voixbesancenot,
        COALESCE(SAFE_CAST(voixgluckstein AS INT64), 0) AS voixgluckstein,
        COALESCE(SAFE_CAST(voixhue AS INT64), 0) AS voixhue,
        COALESCE(SAFE_CAST(voixtaubira AS INT64), 0) AS voixtaubira,
        COALESCE(SAFE_CAST(voixmamere AS INT64), 0) AS voixmamere,
        COALESCE(SAFE_CAST(voixchevenement AS INT64), 0) AS voixchevenement,
        COALESCE(SAFE_CAST(voixjospin AS INT64), 0) AS voixjospin,
        COALESCE(SAFE_CAST(voixlepage AS INT64), 0) AS voixlepage,
        COALESCE(SAFE_CAST(voixsaintjosse AS INT64), 0) AS voixsaintjosse,
        COALESCE(SAFE_CAST(voixbayrou AS INT64), 0) AS voixbayrou,
        COALESCE(SAFE_CAST(voixchirac AS INT64), 0) AS voixchirac,
        COALESCE(SAFE_CAST(voixmadelin AS INT64), 0) AS voixmadelin,
        COALESCE(SAFE_CAST(voixboutin AS INT64), 0) AS voixboutin,
        COALESCE(SAFE_CAST(voixmegret AS INT64), 0) AS voixmegret,
        COALESCE(SAFE_CAST(voixlepen AS INT64), 0) AS voixlepen,
        COALESCE(SAFE_CAST(voixt2chirac AS INT64), 0) AS voixt2chirac,
        COALESCE(SAFE_CAST(voixt2lepen AS INT64), 0) AS voixt2lepen,
        COALESCE(SAFE_CAST(pvoixlaguiller AS FLOAT64), 0) AS pvoixlaguiller,
        COALESCE(SAFE_CAST(pvoixbesancenot AS FLOAT64), 0) AS pvoixbesancenot,
        COALESCE(SAFE_CAST(pvoixgluckstein AS FLOAT64), 0) AS pvoixgluckstein,
        COALESCE(SAFE_CAST(pvoixhue AS FLOAT64), 0) AS pvoixhue,
        COALESCE(SAFE_CAST(pvoixmamere AS FLOAT64), 0) AS pvoixmamere,
        COALESCE(SAFE_CAST(pvoixtaubira AS FLOAT64), 0) AS pvoixtaubira,
        COALESCE(SAFE_CAST(pvoixchevenement AS FLOAT64), 0) AS pvoixchevenement,
        COALESCE(SAFE_CAST(pvoixjospin AS FLOAT64), 0) AS pvoixjospin,
        COALESCE(SAFE_CAST(pvoixlepage AS FLOAT64), 0) AS pvoixlepage,
        COALESCE(SAFE_CAST(pvoixsaintjosse AS FLOAT64), 0) AS pvoixsaintjosse,
        COALESCE(SAFE_CAST(pvoixbayrou AS FLOAT64), 0) AS pvoixbayrou,
        COALESCE(SAFE_CAST(pvoixchirac AS FLOAT64), 0) AS pvoixchirac,
        COALESCE(SAFE_CAST(pvoixmadelin AS FLOAT64), 0) AS pvoixmadelin,
        COALESCE(SAFE_CAST(pvoixboutin AS FLOAT64), 0) AS pvoixboutin,
        COALESCE(SAFE_CAST(pvoixmegret AS FLOAT64), 0) AS pvoixmegret,
        COALESCE(SAFE_CAST(pvoixlepen AS FLOAT64), 0) AS pvoixlepen,
        COALESCE(SAFE_CAST(pvoixlaguillerratio AS FLOAT64), 0) AS pvoixlaguillerratio,
        COALESCE(SAFE_CAST(pvoixbesancenotratio AS FLOAT64), 0) AS pvoixbesancenotratio,
        COALESCE(SAFE_CAST(pvoixglucksteinratio AS FLOAT64), 0) AS pvoixglucksteinratio,
        COALESCE(SAFE_CAST(pvoixhueratio AS FLOAT64), 0) AS pvoixhueratio,
        COALESCE(SAFE_CAST(pvoixmamereratio AS FLOAT64), 0) AS pvoixmamereratio,
        COALESCE(SAFE_CAST(pvoixtaubiraratio AS FLOAT64), 0) AS pvoixtaubiraratio,
        COALESCE(SAFE_CAST(pvoixchevenementratio AS FLOAT64), 0) AS pvoixchevenementratio,
        COALESCE(SAFE_CAST(pvoixjospinratio AS FLOAT64), 0) AS pvoixjospinratio,
        COALESCE(SAFE_CAST(pvoixlepageratio AS FLOAT64), 0) AS pvoixlepageratio,
        COALESCE(SAFE_CAST(pvoixsaintjosseratio AS FLOAT64), 0) AS pvoixsaintjosseratio,
        COALESCE(SAFE_CAST(pvoixbayrouratio AS FLOAT64), 0) AS pvoixbayrouratio,
        COALESCE(SAFE_CAST(pvoixchiracratio AS FLOAT64), 0) AS pvoixchiracratio,
        COALESCE(SAFE_CAST(pvoixmadelinratio AS FLOAT64), 0) AS pvoixmadelinratio,
        COALESCE(SAFE_CAST(pvoixboutinratio AS FLOAT64), 0) AS pvoixboutinratio,
        COALESCE(SAFE_CAST(pvoixmegretratio AS FLOAT64), 0) AS pvoixmegretratio,
        COALESCE(SAFE_CAST(pvoixlepenratio AS FLOAT64), 0) AS pvoixlepenratio,
        COALESCE(SAFE_CAST(pvoixt2chirac AS FLOAT64), 0) AS pvoixt2chirac,
        COALESCE(SAFE_CAST(pvoixt2chiracratio AS FLOAT64), 0) AS pvoixt2chiracratio,
        COALESCE(SAFE_CAST(pvoixt2lepen AS FLOAT64), 0) AS pvoixt2lepen,
        COALESCE(SAFE_CAST(pvoixt2lepenratio AS FLOAT64), 0) AS pvoixt2lepenratio,
        COALESCE(SAFE_CAST(voteg AS FLOAT64), 0) AS voteg,
        COALESCE(SAFE_CAST(votecg AS FLOAT64), 0) AS votecg,
        COALESCE(SAFE_CAST(votec AS FLOAT64), 0) AS votec,
        COALESCE(SAFE_CAST(votecd AS FLOAT64), 0) AS votecd,
        COALESCE(SAFE_CAST(voted AS FLOAT64), 0) AS voted,
        COALESCE(SAFE_CAST(votetg AS FLOAT64), 0) AS votetg,
        COALESCE(SAFE_CAST(votetd AS FLOAT64), 0) AS votetd,
        COALESCE(SAFE_CAST(votegcg AS FLOAT64), 0) AS votegcg,
        COALESCE(SAFE_CAST(votedcd AS FLOAT64), 0) AS votedcd,
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
        COALESCE(SAFE_CAST( pvotecdratio AS FLOAT64), 0) AS pvotecdratio,
        COALESCE(SAFE_CAST(pvotedratio AS FLOAT64), 0) AS pvotedratio,
        COALESCE(SAFE_CAST(pvotegcgratio AS FLOAT64), 0) AS pvotegcgratio,
        COALESCE(SAFE_CAST(pvotedcdratio AS FLOAT64), 0) AS pvotedcdratio,
        COALESCE(SAFE_CAST(pvotetgratio AS FLOAT64), 0) AS pvotetgratio,
        COALESCE(SAFE_CAST(pvotetdratio AS FLOAT64), 0) AS pvotetdratio,
        COALESCE(SAFE_CAST(pervoteg AS FLOAT64), 0) AS pervoteg,
        COALESCE(SAFE_CAST(pervotecg AS FLOAT64), 0) AS pervotecg,
        COALESCE(SAFE_CAST(pervotec AS FLOAT64), 0) AS pervotec,
        COALESCE(SAFE_CAST(pervotecd AS FLOAT64), 0) AS pervotecd,
        COALESCE(SAFE_CAST(pervoted AS FLOAT64), 0) AS pervoted,
        COALESCE(SAFE_CAST(pervotegcg AS FLOAT64), 0) AS pervotegcg,
        COALESCE(SAFE_CAST(pervotedcd AS FLOAT64), 0) AS pervotedcd,
        COALESCE(SAFE_CAST(pervotetg AS FLOAT64), 0) AS pervotetg,
        COALESCE(SAFE_CAST(pervotetd AS FLOAT64), 0) AS pervotetd,
        COALESCE(SAFE_CAST(plm AS INT64), 0) AS plm,
        COALESCE(SAFE_CAST(plmdoublon AS INT64), 0) AS plmdoublon,
        COALESCE(SAFE_CAST(ppar AS FLOAT64), 0) AS ppar,
        COALESCE(SAFE_CAST(perpar AS FLOAT64), 0) AS perpar,
        COALESCE(SAFE_CAST(pparratio AS FLOAT64), 0) AS pparratio,
        paris,
        nomcanton,
        COALESCE(SAFE_CAST(pabs AS FLOAT64), 0) AS pabs,
        COALESCE(SAFE_CAST(pblancsnuls AS FLOAT64), 0) AS pblancsnuls,
        inscritst2,
        votantst2,
        exprimest2,
        COALESCE(SAFE_CAST(pabst2 AS FLOAT64), 0) AS pabst2,
        COALESCE(SAFE_CAST(pblancsnulst2 AS FLOAT64), 0) AS pblancsnulst2

    from source

)

select * from renamed
