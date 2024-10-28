-- Generated from preql source: c:\Users\ethan\coding_projects\pypreql-etl\tests\integration\preql\customer_three.preql
-- Do not edit manually
{{ config(materialized='table') }}

WITH 
quizzical as (
SELECT
    cast(get_current_timestamp() as datetime) as "_trilogyt__created_at"
),
dynamic as (
SELECT
    dim_splits_three."cte_generic_split" as "cte_generic_split"
FROM
    {{ ref('dim_splits_three_gen_model') }} as dim_splits_three
GROUP BY 
    dim_splits_three."cte_generic_split")
SELECT
    dynamic."cte_generic_split" as "cte_generic_split",
    quizzical."_trilogyt__created_at" as "_trilogyt__created_at"
FROM
    dynamic
    FULL JOIN quizzical on 1=1