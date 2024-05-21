-- Generated from preql source: c:\Users\ethan\coding_projects\pypreql-etl\tests\integration\preql\customer_two.preql
-- Do not edit manually
{{ config(materialized='table') }}

WITH 
fabulous as (
SELECT
    local_split."generic_split" as "generic_split"
FROM
    {{ ref('split_gen_model') }} as local_split
),
kestrel as (
SELECT
    cast(get_current_timestamp() as datetime) as "_preqlt__created_at"

),
barracuda as (
SELECT
    fabulous."generic_split" as "generic_split",
    kestrel."_preqlt__created_at" as "_preqlt__created_at"
FROM
    fabulous as fabulous
    FULL JOIN kestrel on 1=1
)
SELECT
    barracuda."generic_split",
    barracuda."_preqlt__created_at"
FROM
    barracuda
