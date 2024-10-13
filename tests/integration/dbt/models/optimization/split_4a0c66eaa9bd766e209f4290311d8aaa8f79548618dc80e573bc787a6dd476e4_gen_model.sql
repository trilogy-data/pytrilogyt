-- Generated from preql source: c:\Users\ethan\coding_projects\pypreql-etl\tests\integration\preql\_internal_cached_intermediates.preql
-- Do not edit manually
{{ config(materialized='table') }}

WITH 
highfalutin as (
SELECT
    avalues."int_array" as "generic_int_array"
FROM
    (
select [1,2,3,4] as int_array, 2 as scalar
) as avalues),
quizzical as (
SELECT
    cast(get_current_timestamp() as datetime) as "_trilogyt__created_at"
),
cooperative as (
SELECT
    highfalutin."generic_int_array" as "generic_int_array"
FROM
    highfalutin
GROUP BY 
    highfalutin."generic_int_array"),
questionable as (
SELECT
    unnest(cooperative."generic_int_array") as "generic_split"
FROM
    cooperative),
abundant as (
SELECT
    questionable."generic_split" as "cte_generic_split"
FROM
    questionable
WHERE
    questionable."generic_split" in ( 1,2,3 )
)
SELECT
    abundant."cte_generic_split" as "cte_generic_split",
    quizzical."_trilogyt__created_at" as "_trilogyt__created_at"
FROM
    abundant
    FULL JOIN quizzical on 1=1