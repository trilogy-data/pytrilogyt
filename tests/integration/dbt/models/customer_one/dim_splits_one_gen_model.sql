-- Generated from preql source: customer_one
-- Do not edit manually
{{ config(materialized='table') }}

WITH 
highfalutin as (
SELECT
    "dsgeneric_scalar_445831a9"."generic_int_array" as "generic_int_array"
FROM
    {{ ref('dsgeneric_scalar_445831a9_gen_model') }} as "dsgeneric_scalar_445831a9"
GROUP BY 
    "dsgeneric_scalar_445831a9"."generic_int_array")
SELECT
    unnest("highfalutin"."generic_int_array") as "generic_split"
FROM
    "highfalutin"