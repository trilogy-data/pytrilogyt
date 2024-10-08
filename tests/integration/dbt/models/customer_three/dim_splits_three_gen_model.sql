-- Generated from preql source: c:\Users\ethan\coding_projects\pypreql-etl\tests\integration\preql\customer_three.preql
-- Do not edit manually
{{ config(materialized='table') }}

WITH 
falcon as (
SELECT
    cast(get_current_timestamp() as datetime) as "_trilogyt__created_at"
),
sweltering as (
SELECT
    split_4a0c66eaa9bd766e209f4290311d8aaa8f79548618dc80e573bc787a6dd476e4."cte_generic_split" as "cte_generic_split"
FROM
    {{ ref('split_4a0c66eaa9bd766e209f4290311d8aaa8f79548618dc80e573bc787a6dd476e4_gen_model') }} as split_4a0c66eaa9bd766e209f4290311d8aaa8f79548618dc80e573bc787a6dd476e4
GROUP BY 
    split_4a0c66eaa9bd766e209f4290311d8aaa8f79548618dc80e573bc787a6dd476e4."cte_generic_split")
SELECT
    sweltering."cte_generic_split" as "cte_generic_split",
    falcon."_trilogyt__created_at" as "_trilogyt__created_at"
FROM
    sweltering
    FULL JOIN falcon on 1=1