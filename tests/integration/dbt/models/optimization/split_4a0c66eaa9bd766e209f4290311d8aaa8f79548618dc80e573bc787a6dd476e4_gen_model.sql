-- Generated from preql source: _internal_cached_intermediates_7d15abe526e0d0d7b96cb658888f0006aac45ab01d5bb07bd1009eb788487a25
-- Do not edit manually
{{ config(materialized='table') }}

WITH 
quizzical as (
SELECT
    generic_avalues."int_array" as "generic_int_array",
    generic_avalues."scalar" as "generic_scalar"
FROM
    ((
select [1,2,3,4] as int_array, 2 as scalar
)) as generic_avalues),
highfalutin as (
SELECT
    unnest(quizzical."generic_int_array") as "generic_split",
    quizzical."generic_scalar" as "generic_scalar"
FROM
    quizzical)
SELECT
    highfalutin."generic_split" as "cte_generic_split"
FROM
    highfalutin
WHERE
    highfalutin."generic_split" in (1,2,3)
