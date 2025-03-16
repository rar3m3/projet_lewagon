{{ config(
    materialized='table'
) }}

WITH agg_table AS (
    SELECT
        CONCAT('FR-',pop1999.code_departement) AS code_departement,
        CAST(pop1999.population AS INT64) AS population1999,
        CAST(pop2006.population AS INT64) AS population2006,
        CAST(pop2007.population AS INT64) AS population2007,
        CAST(pop2008.population AS INT64) AS population2008,
        CAST(pop2009.population AS INT64) AS population2009,
        CAST(pop2010.population AS INT64) AS population2010,
        CAST(pop2011.population AS INT64) AS population2011,
        CAST(pop2012.population AS INT64) AS population2012,
        CAST(pop2013.population AS INT64) AS population2013,
        CAST(pop2014.population AS INT64) AS population2014,
        CAST(pop2015.population AS INT64) AS population2015,
        CAST(pop2016.population AS INT64) AS population2016,
        CAST(pop2017.population AS INT64) AS population2017,
        CAST(pop2018.population AS INT64) AS population2018,
        CAST(pop2019.population AS INT64) AS population2019,
        CAST(pop2020.population AS INT64) AS population2020,
        CAST(pop2021.population AS INT64) AS population2021,
        CAST(pop2022.population AS INT64) AS population2022
    FROM
        {{ ref('population_1999_clean') }} AS pop1999
    LEFT JOIN
        {{ ref('population_2006_clean') }} AS pop2006
        USING (code_departement)
    LEFT JOIN
        {{ ref('population_2007_clean') }} AS pop2007
        USING (code_departement)
    LEFT JOIN
        {{ ref('population_2008_clean') }} AS pop2008
        USING (code_departement)
    LEFT JOIN
        {{ ref('population_2009_clean') }} AS pop2009
        USING (code_departement)
    LEFT JOIN
        {{ ref('population_2010_clean') }} AS pop2010
        USING (code_departement)
    LEFT JOIN
        {{ ref('population_2011_clean') }} AS pop2011
        USING (code_departement)
    LEFT JOIN
        {{ ref('population_2012_clean') }} AS pop2012
        USING (code_departement)
    LEFT JOIN
        {{ ref('population_2013_clean') }} AS pop2013
        USING (code_departement)
    LEFT JOIN
        {{ ref('population_2014_clean') }} AS pop2014
        USING (code_departement)
    LEFT JOIN
        {{ ref('population_2015_clean') }} AS pop2015
        USING (code_departement)
    LEFT JOIN
        {{ ref('population_2016_clean') }} AS pop2016
        USING (code_departement)
    LEFT JOIN
        {{ ref('population_2017_clean') }} AS pop2017
        USING (code_departement)
    LEFT JOIN
        {{ ref('population_2018_clean') }} AS pop2018
        USING (code_departement)
    LEFT JOIN
        {{ ref('population_2019_clean') }} AS pop2019
        USING (code_departement)
    LEFT JOIN
        {{ ref('population_2020_clean') }} AS pop2020
        USING (code_departement)
    LEFT JOIN
        {{ ref('population_2021_clean') }} AS pop2021
        USING (code_departement)
    LEFT JOIN
        {{ ref('population_2022_clean') }} AS pop2022
        USING (code_departement)
) 

SELECT
    code_departement,
    annee,
    population
FROM agg_table
UNPIVOT (population for annee in
        (population1999 AS "1999",
        population2006 AS "2006",
        population2007 AS "2007",
        population2008 AS "2008",
        population2009 AS "2009",
        population2010 AS "2010",
        population2011 AS "2011",
        population2012 AS "2012",
        population2013 AS "2013",
        population2014 AS "2014",
        population2015 AS "2015",
        population2016 AS "2016",
        population2017 AS "2017",
        population2018 AS "2018",
        population2019 AS "2019",
        population2020 AS "2020",
        population2021 AS "2021",
        population2022 AS "2022"
        ))
