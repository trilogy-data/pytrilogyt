-- Generated from preql source: c:\Users\ethan\coding_projects\pypreql-etl\tests\integration\preql\_internal_cached_intermediates.preql
-- Do not edit manually
{{ config(materialized='table') }}

WITH 
quizzical as (
SELECT
    cast(get_current_timestamp() as datetime) as "_trilogyt__created_at"
),
highfalutin as (
SELECT
    avalues."int_array" as "generic_int_array",
    avalues."scalar" as "generic_scalar"
FROM
    (
select [1,2,3,4] as int_array, 2 as scalar
) as avalues)
SELECT
    highfalutin."generic_int_array" as "generic_int_array",
    highfalutin."generic_scalar" as "generic_scalar",
    quizzical."_trilogyt__created_at" as "_trilogyt__created_at"
FROM
    highfalutin
    FULL JOIN quizzical on 1=1