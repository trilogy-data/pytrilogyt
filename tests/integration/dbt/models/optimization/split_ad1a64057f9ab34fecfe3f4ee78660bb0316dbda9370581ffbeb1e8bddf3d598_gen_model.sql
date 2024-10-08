-- Generated from preql source: c:\Users\ethan\coding_projects\pypreql-etl\tests\integration\preql\_internal_cached_intermediates.preql
-- Do not edit manually
{{ config(materialized='table') }}

WITH 
juicy as (
SELECT
    avalues."int_array" as "generic_int_array"
FROM
    (
select [1,2,3,4] as int_array
) as avalues),
quetzal as (
SELECT
    cast(get_current_timestamp() as datetime) as "_trilogyt__created_at"
),
skylark as (
SELECT
    unnest(juicy."generic_int_array") as "generic_split"
FROM
    juicy),
abundant as (
SELECT
    skylark."generic_split" as "generic_split"
FROM
    skylark)
SELECT
    abundant."generic_split" as "generic_split",
    quetzal."_trilogyt__created_at" as "_trilogyt__created_at"
FROM
    abundant
    FULL JOIN quetzal on 1=1