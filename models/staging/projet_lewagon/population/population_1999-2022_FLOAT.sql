SELECT
code_departement,
CAST(population2011 AS INT64) AS population2011,
CAST(population2012 AS INT64) AS population2012,
CAST(population2013 AS INT64) AS population2013,
CAST(population2014 AS INT64) AS population2014,
CAST(population2015 AS INT64) AS population2015,
CAST(population2016 AS INT64) AS population2016,
CAST(population2017 AS INT64) AS population2017,
CAST(population2018 AS INT64) AS population2018,
CAST(population2019 AS INT64) AS population2019,
CAST(population2020 AS INT64) AS population2020,
CAST(population2021 AS INT64) AS population2021,
CAST(population2022 AS INT64) AS population2022
FROM {{ ref('population_1999_2022_clean') }}