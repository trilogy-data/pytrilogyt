-- Generated from preql source: c:\Users\ethan\coding_projects\pypreql-etl\tests\integration\preql\customer_two.preql
-- Do not edit manually
{{ config(materialized='table') }}

WITH 
thrush as (
SELECT
    local_split."generic_split" as "generic_split"
FROM
    {{ ref('split_gen_model') }} as local_split
),
goose as (
SELECT
    cast(get_current_timestamp() as datetime) as "_preqlt__created_at"

),
imported as (
SELECT
    thrush."generic_split" as "generic_split",
    goose."_preqlt__created_at" as "_preqlt__created_at"
FROM
    thrush as thrush
    FULL JOIN goose on 1=1
)
SELECT
    imported."generic_split",
    imported."_preqlt__created_at"
FROM
    imported
