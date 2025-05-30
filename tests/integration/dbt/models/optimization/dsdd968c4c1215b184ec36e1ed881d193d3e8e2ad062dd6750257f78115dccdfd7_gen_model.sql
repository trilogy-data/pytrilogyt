-- Generated from preql source: _internal_cached_intermediates_c159bd00d1cef8c530b3e8658dceb6708b4cec2f7826e2805a292f6d617a984d
-- Do not edit manually
{{ config(materialized='table') }}

WITH 
quizzical as (
SELECT
    "generic_avalues"."int_array" as "generic_int_array",
    "generic_avalues"."scalar" as "generic_scalar"
FROM
    ((
select [1,2,3,4] as int_array, 2 as scalar
)) as "generic_avalues")
SELECT
    unnest("quizzical"."generic_int_array") as "generic_split",
    "quizzical"."generic_scalar" as "generic_scalar"
FROM
    "quizzical"