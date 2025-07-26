-- Generated from preql source: customer_two
-- Do not edit manually
{{ config(materialized='table') }}
SELECT
    unnest("dsgeneric_scalar_445831a9"."generic_int_array") as "generic_split",
    "dsgeneric_scalar_445831a9"."generic_scalar" as "generic_scalar"
FROM
    "{{ ref('dsgeneric_scalar_445831a9_gen_model') }}" as "dsgeneric_scalar_445831a9"