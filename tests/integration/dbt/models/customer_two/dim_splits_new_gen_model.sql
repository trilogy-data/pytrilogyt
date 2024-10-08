-- Generated from preql source: c:\Users\ethan\coding_projects\pypreql-etl\tests\integration\preql\customer_two.preql
-- Do not edit manually
{{ config(materialized='table') }}

WITH 
falcon as (
SELECT
    cast(get_current_timestamp() as datetime) as "_trilogyt__created_at"
)
SELECT
    split_04d2c2b1c72fd3be7d8ba5dea1534aae9959d5c5a80b05b06d26b5fd9d8b82f4."generic_split" as "generic_split",
    falcon."_trilogyt__created_at" as "_trilogyt__created_at"
FROM
    {{ ref('split_04d2c2b1c72fd3be7d8ba5dea1534aae9959d5c5a80b05b06d26b5fd9d8b82f4_gen_model') }} as split_04d2c2b1c72fd3be7d8ba5dea1534aae9959d5c5a80b05b06d26b5fd9d8b82f4
    FULL JOIN falcon on 1=1