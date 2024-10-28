-- Generated from preql source: C:\Users\ethan\AppData\Local\Temp\tmpqajhtm8f\_internal_cached_intermediates_498b2d20ac0aeedac6fceb8efc30f13fc2109432edf99d1bbfb2c4a93ec3ca67.preql
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
    ((
select [1,2,3,4] as int_array, 2 as scalar
)) as avalues)
SELECT
    highfalutin."generic_int_array" as "generic_int_array",
    highfalutin."generic_scalar" as "generic_scalar",
    quizzical."_trilogyt__created_at" as "_trilogyt__created_at"
FROM
    highfalutin
    FULL JOIN quizzical on 1=1