-- Generated from preql source: customer_three
-- Do not edit manually
{{ config(materialized='table') }}
SELECT
    "dscte_generic_split_4a0c66ea"."cte_generic_split" as "cte_generic_split"
FROM
    ({{ ref('dscte_generic_split_4a0c66ea_gen_model') }}) as "dscte_generic_split_4a0c66ea"