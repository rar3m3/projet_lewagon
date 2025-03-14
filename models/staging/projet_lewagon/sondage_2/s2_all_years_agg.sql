select
    annee,
    sondeur,
    candidat,
    score
from {{ ref('stg_projet_lewagon__s_2_2002') }}
UNPIVOT (score for candidat in (jacques_chirac_rpr
,jean_marie_le_pen_fn
))
UNION ALL
select
    annee,
    sondeur,
    candidat,
    score
from {{ ref('stg_projet_lewagon__s_2_2007') }}
UNPIVOT (score for candidat in (nicolas_sarkozy
,segolene_royal))
UNION ALL
select
    annee,
    sondeur,
    candidat,
    score
from {{ ref('stg_projet_lewagon__s_2_2012') }}
UNPIVOT (score for candidat in (francois_hollande
,nicolas_sarkozy))
UNION ALL
select
    annee,
    sondeur,
    candidat,
    score
from {{ ref('stg_projet_lewagon__s_2_2017') }}
UNPIVOT (score for candidat in (emmanuel_macron
,marine_le_pen))
UNION ALL
select
    annee,
    sondeur,
    candidat,
    score
from {{ ref('stg_projet_lewagon__s_2_2022') }}
UNPIVOT (score for candidat in (macron_lrem
,le_pen_rn))
