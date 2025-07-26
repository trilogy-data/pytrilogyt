-- Generated from preql source: customer_three
-- Do not edit manually
{{ config(materialized='table') }}
SELECT
    "dscte_generic_scalar_02e41b09"."cte_generic_split" as "cte_generic_split"
FROM
    "{{ ref('dscte_generic_scalar_02e41b09_gen_model') }}" as "dscte_generic_scalar_02e41b09"
GROUP BY 
    "dscte_generic_scalar_02e41b09"."cte_generic_split"