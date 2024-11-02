-- Generated from preql source: customer_three
-- Do not edit manually
{{ config(materialized='table') }}

WITH 
quizzical as (
SELECT
    cast(get_current_timestamp() as datetime) as "_trilogyt__created_at"
)
SELECT
    split_4a0c66eaa9bd766e209f4290311d8aaa8f79548618dc80e573bc787a6dd476e4."cte_generic_split" as "cte_generic_split",
    quizzical."_trilogyt__created_at" as "_trilogyt__created_at"
FROM
    {{ ref('split_4a0c66eaa9bd766e209f4290311d8aaa8f79548618dc80e573bc787a6dd476e4_gen_model') }} as split_4a0c66eaa9bd766e209f4290311d8aaa8f79548618dc80e573bc787a6dd476e4
    FULL JOIN quizzical on 1=1