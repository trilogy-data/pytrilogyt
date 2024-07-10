-- Generated from preql source: c:\Users\ethan\coding_projects\pypreql-etl\tests\integration\preql\_internal_cached_intermediates.preql
-- Do not edit manually
{{ config(materialized='table') }}

WITH 
jay as (
SELECT
    [1,2,3,4] as "generic_int_array",
    cast(get_current_timestamp() as datetime) as "_trilogyt__created_at"

),
magpie as (
SELECT
    unnest(jay."generic_int_array") as "generic_split"
FROM
    jay as jay
)

SELECT
    magpie."generic_split" as "generic_split",
    jay."_trilogyt__created_at" as "_trilogyt__created_at"
FROM
    magpie as magpie
    FULL JOIN jay on 1=1

