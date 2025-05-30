-- Generated from preql source: _internal_cached_intermediates_136f6db6bb3e3ad124fef197c0ee42749ec9bc72498fb9aeb5347c44af91ed77
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
)) as "generic_avalues"),
highfalutin as (
SELECT
    "quizzical"."generic_scalar" as "generic_scalar",
    unnest("quizzical"."generic_int_array") as "generic_split"
FROM
    "quizzical")
SELECT
    "highfalutin"."generic_split" as "cte_generic_split",
    "highfalutin"."generic_scalar" as "cte_generic_scalar"
FROM
    "highfalutin"
WHERE
    "highfalutin"."generic_split" in (1,2,3)
