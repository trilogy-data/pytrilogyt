-- Generated from preql source: customer_three
-- Do not edit manually
{{ config(materialized='table') }}

WITH 
quizzical as (
SELECT
    cast(get_current_timestamp() as datetime) as "_trilogyt__created_at"
),
dynamic as (
SELECT
    scalar_scalar_split_split_2a1b9ef4ddfd30b8a720fd5891233a0df824a60bcbb6553d4acd76e27d402670."cte_generic_split" as "cte_generic_split"
FROM
    {{ ref('scalar_scalar_split_split_2a1b9ef4ddfd30b8a720fd5891233a0df824a60bcbb6553d4acd76e27d402670_gen_model') }} as scalar_scalar_split_split_2a1b9ef4ddfd30b8a720fd5891233a0df824a60bcbb6553d4acd76e27d402670
GROUP BY 
    scalar_scalar_split_split_2a1b9ef4ddfd30b8a720fd5891233a0df824a60bcbb6553d4acd76e27d402670."cte_generic_split")
SELECT
    dynamic."cte_generic_split" as "cte_generic_split",
    quizzical."_trilogyt__created_at" as "_trilogyt__created_at"
FROM
    dynamic
    FULL JOIN quizzical on 1=1