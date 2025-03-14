{{ config(
    materialized='table'
) }}

SELECT
    CONCAT('FR-',pop1999.code_departement) AS code_departement,
    pop1999.population AS population1999, 
    pop2006.population AS population2006,
    pop2007.population AS population2007,
    pop2008.population AS population2008,   
    pop2009.population AS population2009,
    pop2010.population AS population2010,
    pop2011.population AS population2011,   
    pop2012.population AS population2012,
    pop2013.population AS population2013,
    pop2014.population AS population2014,  
    pop2015.population AS population2015,
    pop2016.population AS population2016,
    pop2017.population AS population2017,   
    pop2018.population AS population2018,
    pop2019.population AS population2019,
    pop2020.population AS population2020,
    pop2021.population AS population2021,
    pop2022.population AS population2022  
FROM
    {{ ref('intermediate_population_1999_clean') }} AS pop1999
LEFT JOIN
    {{ ref('intermediate_population_2006_clean') }} AS pop2006
    USING (code_departement)
LEFT JOIN
    {{ ref('intermediate_population_2007_clean') }} AS pop2007
    USING (code_departement)
LEFT JOIN
    {{ ref('intermediate_population_2008_clean') }} AS pop2008
    USING (code_departement)
LEFT JOIN
    {{ ref('intermediate_population_2009_clean') }} AS pop2009
    USING (code_departement)
LEFT JOIN
    {{ ref('intermediate_population_2010_clean') }} AS pop2010
    USING (code_departement)
LEFT JOIN
    {{ ref('intermediate_population_2011_clean') }} AS pop2011
    USING (code_departement)
LEFT JOIN
    {{ ref('intermediate_population_2012_clean') }} AS pop2012
    USING (code_departement)
LEFT JOIN
    {{ ref('intermediate_population_2013_clean') }} AS pop2013
    USING (code_departement)
LEFT JOIN
    {{ ref('intermediate_population_2014_clean') }} AS pop2014
    USING (code_departement)
LEFT JOIN
    {{ ref('intermediate_population_2015_clean') }} AS pop2015
    USING (code_departement)
LEFT JOIN
    {{ ref('intermediate_population_2016_clean') }} AS pop2016
    USING (code_departement)
LEFT JOIN
    {{ ref('intermediate_population_2017_clean') }} AS pop2017
    USING (code_departement)
LEFT JOIN
    {{ ref('intermediate_population_2018_clean') }} AS pop2018
    USING (code_departement)
LEFT JOIN
    {{ ref('intermediate_population_2019_clean') }} AS pop2019
    USING (code_departement)
LEFT JOIN
    {{ ref('intermediate_population_2020_clean') }} AS pop2020
    USING (code_departement)
LEFT JOIN
    {{ ref('intermediate_population_2021_clean') }} AS pop2021
    USING (code_departement)
LEFT JOIN
    {{ ref('intermediate_population_2022_clean') }} AS pop2022
    USING (code_departement)
