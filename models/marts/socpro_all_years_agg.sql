SELECT *
FROM {{ ref('socpro_1999') }}
UNION ALL
SELECT *
FROM {{ ref('socpro_2010') }}
UNION ALL
SELECT *
FROM {{ ref('socpro_2015') }}
UNION ALL
SELECT *
FROM {{ ref('socpro_2021') }}