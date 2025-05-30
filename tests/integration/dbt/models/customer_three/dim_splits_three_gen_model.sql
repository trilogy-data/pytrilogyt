-- Generated from preql source: customer_three
-- Do not edit manually
{{ config(materialized='table') }}

WITH 
quizzical as (
SELECT
    cast(get_current_timestamp() as datetime) as "_trilogyt__created_at"
)
SELECT
    "ds83fdbaa987de1c9e184157380531270fdaee84ab7aaca8a1a401ee8ac4015908"."cte_generic_split" as "cte_generic_split",
    "quizzical"."_trilogyt__created_at" as "_trilogyt__created_at"
FROM
    "{{ ref('ds83fdbaa987de1c9e184157380531270fdaee84ab7aaca8a1a401ee8ac4015908_gen_model') }}" as "ds83fdbaa987de1c9e184157380531270fdaee84ab7aaca8a1a401ee8ac4015908"
    FULL JOIN "quizzical" on 1=1