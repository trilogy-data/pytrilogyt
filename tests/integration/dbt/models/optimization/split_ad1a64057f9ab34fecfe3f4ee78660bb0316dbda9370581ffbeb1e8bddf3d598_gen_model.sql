-- Generated from preql source: c:\Users\ethan\coding_projects\pypreql-etl\tests\integration\preql\_internal_cached_intermediates.preql
-- Do not edit manually
{{ config(materialized='table') }}

WITH 
thrush as (
SELECT
    [1,2,3,4] as "generic_int_array",
    cast(get_current_timestamp() as datetime) as "_preqlt__created_at"

),
lark as (
SELECT
    unnest(thrush."generic_int_array") as "generic_split"
FROM
    thrush as thrush
),
penguin as (
SELECT
    lark."generic_split" as "generic_split",
    thrush."_preqlt__created_at" as "_preqlt__created_at"
FROM
    lark as lark
    FULL JOIN thrush on 1=1
)
SELECT
    penguin."generic_split",
    penguin."_preqlt__created_at"
FROM
    penguin
