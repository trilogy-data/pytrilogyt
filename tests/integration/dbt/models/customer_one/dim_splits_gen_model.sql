-- Generated from preql source: c:\Users\ethan\coding_projects\pypreql-etl\tests\integration\preql\customer_one.preql
-- Do not edit manually
{{ config(materialized='table') }}

WITH 
sedate as (
SELECT
    cast(get_current_timestamp() as datetime) as "_preqlt__created_at",
    unnest([1,2,3,4]) as "generic_split"

),
obsolete as (
SELECT
    sedate."generic_split" as "generic_split",
    sedate."_preqlt__created_at" as "_preqlt__created_at"
FROM
    sedate as sedate

GROUP BY 
    sedate."generic_split")
SELECT
    obsolete."generic_split",
    obsolete."_preqlt__created_at"
FROM
    obsolete
