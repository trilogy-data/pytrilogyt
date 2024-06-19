-- Generated from preql source: c:\Users\ethan\coding_projects\pypreql-etl\tests\integration\dbt\io.preql
-- Do not edit manually
{{ config(materialized='table') }}

WITH 
barracuda as (
SELECT
    1 as "test",
    cast(get_current_timestamp() as datetime) as "_preqlt__created_at"

)
SELECT
    barracuda."test",
    barracuda."_preqlt__created_at"
FROM
    barracuda
