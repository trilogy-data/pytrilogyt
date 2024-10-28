-- Generated from preql source: C:\Users\ethan\AppData\Local\Temp\tmpqajhtm8f\_internal_cached_intermediates_29424b014951ebce6d4d399944cacc9cb566d219637a8039360e043088d50797.preql
-- Do not edit manually
{{ config(materialized='table') }}

WITH 
highfalutin as (
SELECT
    avalues."int_array" as "generic_int_array"
FROM
    ((
select [1,2,3,4] as int_array, 2 as scalar
)) as avalues),
quizzical as (
SELECT
    cast(get_current_timestamp() as datetime) as "_trilogyt__created_at"
),
dynamic as (
SELECT
    highfalutin."generic_int_array" as "generic_int_array"
FROM
    highfalutin
GROUP BY 
    highfalutin."generic_int_array"),
wakeful as (
SELECT
    unnest(dynamic."generic_int_array") as "generic_split"
FROM
    dynamic),
cheerful as (
SELECT
    wakeful."generic_split" as "cte_generic_split"
FROM
    wakeful
WHERE
    wakeful."generic_split" in (1,2,3)
)
SELECT
    cheerful."cte_generic_split" as "cte_generic_split",
    quizzical."_trilogyt__created_at" as "_trilogyt__created_at"
FROM
    cheerful
    FULL JOIN quizzical on 1=1