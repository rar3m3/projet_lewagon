with 

source as (

    select * from {{ source('projet_lewagon', 'pres2002') }}

),

renamed as (

    select
        CONCAT('FR-',dep) AS code_departement,
        SUM(inscrits)inscrits,
        SUM(votants)votants,
        SUM(exprimes)exprimes,
        SUM(voixlaguiller)voixlaguiller,
        SUM(voixbesancenot)voixbesancenot,
        SUM(voixgluckstein)voixgluckstein,
        SUM(voixhue)voixhue,
        SUM(voixtaubira)voixtaubira,
        SUM(voixmamere)voixmamere,
        SUM(voixchevenement)voixchevenement,
        SUM(voixjospin)voixjospin,
        SUM(voixlepage)voixlepage,
        SUM(voixsaintjosse)voixsaintjosse,
        SUM(voixbayrou)voixbayrou,
        SUM(voixchirac)voixchirac,
        SUM(voixmadelin)voixmadelin,
        SUM(voixboutin)voixboutin,
        SUM(voixmegret)voixmegret,
        SUM(voixlepen)voixlepen,
        SUM(voixt2chirac)voixt2chirac,
        SUM(voixt2lepen)voixt2lepen,

    from source
    GROUP BY (code_departement)

)

select * from renamed
