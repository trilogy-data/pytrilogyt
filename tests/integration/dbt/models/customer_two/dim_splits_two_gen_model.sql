-- Generated from preql source: customer_two
-- Do not edit manually
{{ config(materialized='table') }}
SELECT
    "dsgeneric_scalar_0c9429cc"."generic_split" as "generic_split",
    "dsgeneric_scalar_0c9429cc"."generic_scalar" as "generic_scalar"
FROM
    "{{ ref('dsgeneric_scalar_0c9429cc_gen_model') }}" as "dsgeneric_scalar_0c9429cc"
GROUP BY 
    "dsgeneric_scalar_0c9429cc"."generic_scalar",
    "dsgeneric_scalar_0c9429cc"."generic_split"