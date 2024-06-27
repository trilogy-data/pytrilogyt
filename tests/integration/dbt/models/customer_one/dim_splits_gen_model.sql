-- Generated from preql source: c:\Users\ethan\coding_projects\pypreql-etl\tests\integration\preql\customer_one.preql
-- Do not edit manually
{{ config(materialized='table') }}

WITH 
budgie as (
SELECT
    local_split_ad1a64057f9ab34fecfe3f4ee78660bb0316dbda9370581ffbeb1e8bddf3d598."generic_split" as "generic_split"
FROM
    {{ ref('split_ad1a64057f9ab34fecfe3f4ee78660bb0316dbda9370581ffbeb1e8bddf3d598_gen_model') }} as local_split_ad1a64057f9ab34fecfe3f4ee78660bb0316dbda9370581ffbeb1e8bddf3d598
),
deeply as (
SELECT
    cast(get_current_timestamp() as datetime) as "_preqlt__created_at"

),
sloppy as (
SELECT
    budgie."generic_split" as "generic_split",
    deeply."_preqlt__created_at" as "_preqlt__created_at"
FROM
    budgie as budgie
    FULL JOIN deeply on 1=1
)
SELECT
    sloppy."generic_split",
    sloppy."_preqlt__created_at"
FROM
    sloppy
