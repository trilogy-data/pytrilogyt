-- Generated from preql source: c:\Users\ethan\coding_projects\pypreql-etl\tests\integration\preql\customer_one.preql
-- Do not edit manually
{{ config(materialized='table') }}

WITH 
quizzical as (
SELECT
    cast(get_current_timestamp() as datetime) as "_trilogyt__created_at"
),
dynamic as (
SELECT
    dim_splits_new."generic_split" as "generic_split",
    quizzical."_trilogyt__created_at" as "_trilogyt__created_at"
FROM
    {{ ref('dim_splits_new_gen_model') }} as dim_splits_new
    FULL JOIN quizzical on 1=1)
SELECT
    dynamic."generic_split" as "generic_split",
    dynamic."_trilogyt__created_at" as "_trilogyt__created_at"
FROM
    dynamic
GROUP BY 
    dynamic."generic_split",
    dynamic."_trilogyt__created_at"