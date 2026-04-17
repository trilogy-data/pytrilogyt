-- Generated from preql source: _opt_generic_a36e85ce_build
-- Do not edit manually
{{ config(materialized='table') }}

WITH 
quizzical as (
SELECT
    "generic_avalues"."int_array" as "generic_int_array"
FROM
    (
select [1,2,3,4] as int_array, 2 as scalar
) as "generic_avalues"),
highfalutin as (
SELECT
    unnest("quizzical"."generic_int_array") as "generic_split"
FROM
    "quizzical")
SELECT
    "highfalutin"."generic_split" as "cte_generic_split"
FROM
    "highfalutin"
WHERE
    "highfalutin"."generic_split" in (1,2,3)

GROUP BY
    1