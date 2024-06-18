-- Generated from preql source: c:\Users\ethan\coding_projects\pypreql-etl\tests\integration\preql\_internal_cached_intermediates.preql
-- Do not edit manually
{{ config(materialized='table') }}

WITH 
shark as (
SELECT
    [1,2,3,4] as "generic_int_array",
    cast(get_current_timestamp() as datetime) as "_preqlt__created_at"

),
odd as (
SELECT
    unnest(shark."generic_int_array") as "generic_split"
FROM
    shark as shark
),
canary as (
SELECT
    odd."generic_split" as "generic_split",
    shark."_preqlt__created_at" as "_preqlt__created_at"
FROM
    odd as odd
    FULL JOIN shark on 1=1
)
SELECT
    canary."generic_split",
    canary."_preqlt__created_at"
FROM
    canary
