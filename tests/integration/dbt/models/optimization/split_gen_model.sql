-- Generated from preql source: c:\Users\ethan\coding_projects\pypreql-etl\tests\integration\preql\_internal_cached_intermediates.preql
-- Do not edit manually
{{ config(materialized='table') }}

WITH 
puffin as (
SELECT
    [1,2,3,4] as "generic_int_array",
    cast(get_current_timestamp() as datetime) as "_preqlt__created_at"

),
macho as (
SELECT
    unnest(puffin."generic_int_array") as "generic_split"
FROM
    puffin as puffin
),
premium as (
SELECT
    macho."generic_split" as "generic_split",
    puffin."_preqlt__created_at" as "_preqlt__created_at"
FROM
    macho as macho
    FULL JOIN puffin on 1=1
)
SELECT
    premium."generic_split",
    premium."_preqlt__created_at"
FROM
    premium
