-- Generated from preql source: c:\Users\ethan\coding_projects\pypreql-etl\tests\integration\preql\_internal_cached_intermediates.preql
-- Do not edit manually
{{ config(materialized='table') }}

WITH 
dingo as (
SELECT
    [1,2,3,4] as "generic_int_array",
    cast(get_current_timestamp() as datetime) as "_preqlt__created_at"

),
owl as (
SELECT
    unnest(dingo."generic_int_array") as "generic_split"
FROM
    dingo as dingo
),
rook as (
SELECT
    owl."generic_split" as "generic_split",
    dingo."_preqlt__created_at" as "_preqlt__created_at"
FROM
    owl as owl
    LEFT OUTER JOIN dingo on 1=1
)
SELECT
    rook."generic_split",
    rook."_preqlt__created_at"
FROM
    rook
