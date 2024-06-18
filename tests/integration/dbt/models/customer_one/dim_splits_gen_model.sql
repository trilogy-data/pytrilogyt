-- Generated from preql source: c:\Users\ethan\coding_projects\pypreql-etl\tests\integration\preql\customer_one.preql
-- Do not edit manually
{{ config(materialized='table') }}

WITH 
tiger as (
SELECT
    local_split_ad1a64057f9ab34fecfe3f4ee78660bb0316dbda9370581ffbeb1e8bddf3d598."generic_split" as "generic_split"
FROM
    {{ ref('split_ad1a64057f9ab34fecfe3f4ee78660bb0316dbda9370581ffbeb1e8bddf3d598_gen_model') }} as local_split_ad1a64057f9ab34fecfe3f4ee78660bb0316dbda9370581ffbeb1e8bddf3d598
),
canary as (
SELECT
    cast(get_current_timestamp() as datetime) as "_preqlt__created_at"

),
falcon as (
SELECT
    tiger."generic_split" as "generic_split",
    canary."_preqlt__created_at" as "_preqlt__created_at"
FROM
    tiger as tiger
    FULL JOIN canary on 1=1
)
SELECT
    falcon."generic_split",
    falcon."_preqlt__created_at"
FROM
    falcon
