SELECT
CONCAT('FR-', code_departement) AS code_departement,
population2011,
population2012,
population2013,
population2014,
population2015,
population2016,
population2017,
population2018,
population2019,
population2020,
population2021,
population2022,
FROM {{ ref('population_1999-2022_FLOAT') }}