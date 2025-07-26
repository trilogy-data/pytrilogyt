-- Generated from preql source: _opt_generic_136f6db6_build
-- Do not edit manually
{{ config(materialized='table') }}

WITH 
quizzical as (
SELECT
    "generic_avalues"."int_array" as "generic_int_array",
    "generic_avalues"."scalar" as "generic_scalar"
FROM
    (
select [1,2,3,4] as int_array, 2 as scalar
) as "generic_avalues")
SELECT
    "quizzical"."generic_scalar" as "generic_scalar",
    unnest("quizzical"."generic_int_array") as "generic_split"
FROM
    "quizzical"