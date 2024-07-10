-- Generated from preql source: c:\Users\ethan\coding_projects\pypreql-etl\tests\integration\dbt\io.preql
-- Do not edit manually
{{ config(materialized='table') }}


SELECT
    1 as "test",
    cast(get_current_timestamp() as datetime) as "_trilogyt__created_at"


