-- Generated from preql source: c:\Users\ethan\coding_projects\pypreql-etl\tests\integration\preql\customer_one.preql
-- Do not edit manually
{{ config(materialized='table') }}

WITH 
quizzical as (
SELECT
    local_split."generic_split" as "generic_split"
FROM
    {{ ref('split_gen_model') }} as local_split
),
elated as (
SELECT
    cast(get_current_timestamp() as datetime) as "_preqlt__created_at"

),
bear as (
SELECT
    quizzical."generic_split" as "generic_split",
    elated."_preqlt__created_at" as "_preqlt__created_at"
FROM
    quizzical as quizzical
    FULL JOIN elated on 1=1
)
SELECT
    bear."generic_split",
    bear."_preqlt__created_at"
FROM
    bear
