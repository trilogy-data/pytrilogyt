-- Generated from preql source: C:\Users\ethan\AppData\Local\Temp\tmpqajhtm8f\_internal_cached_intermediates_7d15abe526e0d0d7b96cb658888f0006aac45ab01d5bb07bd1009eb788487a25.preql
-- Do not edit manually
{{ config(materialized='table') }}

WITH 
highfalutin as (
SELECT
    avalues."int_array" as "generic_int_array",
    avalues."scalar" as "generic_scalar"
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
    unnest(highfalutin."generic_int_array") as "generic_split",
    highfalutin."generic_scalar" as "generic_scalar"
FROM
    highfalutin),
wakeful as (
SELECT
    dynamic."generic_split" as "cte_generic_split",
    dynamic."generic_scalar" as "cte_generic_scalar"
FROM
    dynamic
WHERE
    dynamic."generic_split" in (1,2,3)
),
cheerful as (
SELECT
    wakeful."cte_generic_split" as "cte_generic_split",
    quizzical."_trilogyt__created_at" as "_trilogyt__created_at"
FROM
    wakeful
    FULL JOIN quizzical on 1=1)
SELECT
    cheerful."cte_generic_split" as "cte_generic_split",
    cheerful."_trilogyt__created_at" as "_trilogyt__created_at"
FROM
    cheerful
GROUP BY 
    cheerful."_trilogyt__created_at",
    cheerful."cte_generic_split"