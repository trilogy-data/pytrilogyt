-- Generated from preql source: c:\Users\ethan\coding_projects\pypreql-etl\tests\integration\preql\_internal_cached_intermediates.preql
-- Do not edit manually
{{ config(materialized='table') }}

WITH 
penguin as (
SELECT
    [1,2,3,4] as "generic_int_array",
    cast(get_current_timestamp() as datetime) as "_preqlt__created_at"

),
komodo as (
SELECT
    unnest(penguin."generic_int_array") as "generic_split"
FROM
    penguin as penguin
),
colossal as (
SELECT
    komodo."generic_split" as "generic_split",
    penguin."_preqlt__created_at" as "_preqlt__created_at"
FROM
    komodo as komodo
    FULL JOIN penguin on 1=1
)
SELECT
    colossal."generic_split",
    colossal."_preqlt__created_at"
FROM
    colossal
