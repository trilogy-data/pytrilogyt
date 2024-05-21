-- Generated from preql source: c:\Users\ethan\coding_projects\pypreql-etl\tests\integration\dbt\io.preql
-- Do not edit manually
{{ config(materialized='table') }}

WITH 
swift as (
SELECT
    1 as "io_test",
    cast(get_current_timestamp() as datetime) as "_preqlt__created_at"

)
SELECT
    swift."io_test",
    swift."_preqlt__created_at"
FROM
    swift
