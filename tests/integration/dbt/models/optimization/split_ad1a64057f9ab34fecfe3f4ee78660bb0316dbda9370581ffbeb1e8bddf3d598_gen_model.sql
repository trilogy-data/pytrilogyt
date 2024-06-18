-- Generated from preql source: c:\Users\ethan\coding_projects\pypreql-etl\tests\integration\preql\_internal_cached_intermediates.preql
-- Do not edit manually
{{ config(materialized='table') }}

WITH 
fabulous as (
SELECT
    [1,2,3,4] as "generic_int_array",
    cast(get_current_timestamp() as datetime) as "_preqlt__created_at"

),
imported as (
SELECT
    unnest(fabulous."generic_int_array") as "generic_split"
FROM
    fabulous as fabulous
),
goldfinch as (
SELECT
    imported."generic_split" as "generic_split",
    fabulous."_preqlt__created_at" as "_preqlt__created_at"
FROM
    imported as imported
    LEFT OUTER JOIN fabulous on 1=1
)
SELECT
    goldfinch."generic_split",
    goldfinch."_preqlt__created_at"
FROM
    goldfinch
