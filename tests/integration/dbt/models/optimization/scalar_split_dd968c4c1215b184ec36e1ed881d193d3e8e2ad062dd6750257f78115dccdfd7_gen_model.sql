-- Generated from preql source: c:\Users\ethan\coding_projects\pypreql-etl\tests\integration\preql\_internal_cached_intermediates.preql
-- Do not edit manually
{{ config(materialized='table') }}

WITH 
highfalutin as (
SELECT
    avalues."int_array" as "generic_int_array",
    avalues."scalar" as "generic_scalar"
FROM
    (
select [1,2,3,4] as int_array, 2 as scalar
) as avalues),
quizzical as (
SELECT
    cast(get_current_timestamp() as datetime) as "_trilogyt__created_at"
),
wakeful as (
SELECT
    unnest(highfalutin."generic_int_array") as "generic_split",
    highfalutin."generic_scalar" as "generic_scalar"
FROM
    highfalutin),
cheerful as (
SELECT
    wakeful."generic_split" as "generic_split",
    wakeful."generic_scalar" as "generic_scalar"
FROM
    wakeful)
SELECT
    cheerful."generic_split" as "generic_split",
    cheerful."generic_scalar" as "generic_scalar",
    quizzical."_trilogyt__created_at" as "_trilogyt__created_at"
FROM
    cheerful
    FULL JOIN quizzical on 1=1