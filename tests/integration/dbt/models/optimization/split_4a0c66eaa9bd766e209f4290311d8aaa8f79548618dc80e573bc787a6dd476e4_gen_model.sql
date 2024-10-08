-- Generated from preql source: c:\Users\ethan\coding_projects\pypreql-etl\tests\integration\preql\_internal_cached_intermediates.preql
-- Do not edit manually
{{ config(materialized='table') }}

WITH 
lark as (
SELECT
    avalues."int_array" as "generic_int_array"
FROM
    (
select [1,2,3,4] as int_array
) as avalues),
falcon as (
SELECT
    cast(get_current_timestamp() as datetime) as "_trilogyt__created_at"
),
sweltering as (
SELECT
    unnest(lark."generic_int_array") as "generic_split"
FROM
    lark),
spiritual as (
SELECT
    sweltering."generic_split" as "cte_generic_split"
FROM
    sweltering
WHERE
    sweltering."generic_split" in ( 1,2,3 )
)
SELECT
    spiritual."cte_generic_split" as "cte_generic_split",
    falcon."_trilogyt__created_at" as "_trilogyt__created_at"
FROM
    spiritual
    FULL JOIN falcon on 1=1