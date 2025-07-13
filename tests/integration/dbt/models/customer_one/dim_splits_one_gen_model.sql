-- Generated from preql source: customer_one
-- Do not edit manually
{{ config(materialized='table') }}
SELECT
    "dsgeneric_scalar_0c9429cc"."generic_split" as "generic_split"
FROM
    "{{ ref('dsgeneric_scalar_0c9429cc_gen_model') }}" as "dsgeneric_scalar_0c9429cc"
GROUP BY 
    "dsgeneric_scalar_0c9429cc"."generic_split"