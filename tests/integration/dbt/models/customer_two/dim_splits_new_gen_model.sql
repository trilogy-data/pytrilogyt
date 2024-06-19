-- Generated from preql source: c:\Users\ethan\coding_projects\pypreql-etl\tests\integration\preql\customer_two.preql
-- Do not edit manually
{{ config(materialized='table') }}

WITH 
wary as (
SELECT
    local_split_ad1a64057f9ab34fecfe3f4ee78660bb0316dbda9370581ffbeb1e8bddf3d598."generic_split" as "generic_split"
FROM
    {{ ref('split_ad1a64057f9ab34fecfe3f4ee78660bb0316dbda9370581ffbeb1e8bddf3d598_gen_model') }} as local_split_ad1a64057f9ab34fecfe3f4ee78660bb0316dbda9370581ffbeb1e8bddf3d598
),
python as (
SELECT
    cast(get_current_timestamp() as datetime) as "_preqlt__created_at"

),
viper as (
SELECT
    wary."generic_split" as "generic_split",
    python."_preqlt__created_at" as "_preqlt__created_at"
FROM
    wary as wary
    LEFT OUTER JOIN python on 1=1
)
SELECT
    viper."generic_split",
    viper."_preqlt__created_at"
FROM
    viper
