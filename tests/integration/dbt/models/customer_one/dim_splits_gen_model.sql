-- Generated from preql source: c:\Users\ethan\coding_projects\pypreql-etl\tests\integration\preql\customer_one.preql
-- Do not edit manually
{{ config(materialized='table') }}

WITH 
sordid as (
SELECT
    local_split."generic_split" as "generic_split"
FROM
    {{ ref('split_gen_model') }} as local_split
),
crow as (
SELECT
    cast(get_current_timestamp() as datetime) as "_preqlt__created_at"

),
premium as (
SELECT
    sordid."generic_split" as "generic_split",
    crow."_preqlt__created_at" as "_preqlt__created_at"
FROM
    sordid as sordid
    FULL JOIN crow on 1=1
)
SELECT
    premium."generic_split",
    premium."_preqlt__created_at"
FROM
    premium
