-- Generated from preql source: C:\Users\ethan\AppData\Local\Temp\tmpqajhtm8f\_internal_cached_intermediates_498b2d20ac0aeedac6fceb8efc30f13fc2109432edf99d1bbfb2c4a93ec3ca67.preql
-- Do not edit manually
{{ config(materialized='table') }}

WITH 
highfalutin as (
SELECT
    avalues."int_array" as "generic_int_array"
FROM
    ((
select [1,2,3,4] as int_array, 2 as scalar
)) as avalues),
quizzical as (
SELECT
    cast(get_current_timestamp() as datetime) as "_trilogyt__created_at"
),
dynamic as (
SELECT
    highfalutin."generic_int_array" as "generic_int_array"
FROM
    highfalutin
GROUP BY 
    highfalutin."generic_int_array"),
wakeful as (
SELECT
    unnest(dynamic."generic_int_array") as "generic_split"
FROM
    dynamic),
cheerful as (
SELECT
    wakeful."generic_split" as "generic_split"
FROM
    wakeful)
SELECT
    cheerful."generic_split" as "generic_split",
    quizzical."_trilogyt__created_at" as "_trilogyt__created_at"
FROM
    cheerful
    FULL JOIN quizzical on 1=1