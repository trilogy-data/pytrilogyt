-- Generated from preql source: customer_one
-- Do not edit manually
{{ config(materialized='table') }}

WITH 
quizzical as (
SELECT
    "dsgeneric_scalar_445831a9"."generic_int_array" as "generic_int_array"
FROM
    {{ ref('dsgeneric_scalar_445831a9_gen_model') }} as "dsgeneric_scalar_445831a9"
GROUP BY
    1),
wakeful as (
SELECT
    unnest("quizzical"."generic_int_array") as "generic_split"
FROM
    "quizzical")
SELECT
    "wakeful"."generic_split" as "generic_split"
FROM
    "wakeful"
GROUP BY
    1