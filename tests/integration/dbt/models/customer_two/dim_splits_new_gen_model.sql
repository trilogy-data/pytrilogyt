-- Generated from preql source: c:\Users\ethan\coding_projects\pypreql-etl\tests\integration\preql\customer_two.preql
-- Do not edit manually
{{ config(materialized='table') }}

WITH 
scrawny as (
SELECT
    cast(get_current_timestamp() as datetime) as "_preqlt__created_at",
    unnest([1,2,3,4]) as "generic_split"

),
wakeful as (
SELECT
    scrawny."generic_split" as "generic_split",
    scrawny."_preqlt__created_at" as "_preqlt__created_at"
FROM
    scrawny as scrawny

GROUP BY 
    scrawny."generic_split")
SELECT
    wakeful."generic_split",
    wakeful."_preqlt__created_at"
FROM
    wakeful
