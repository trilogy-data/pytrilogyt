-- Generated from preql source: c:\Users\ethan\coding_projects\pypreql-etl\tests\integration\preql\customer_two.preql
-- Do not edit manually
{{ config(materialized='table') }}

WITH 
cuckoo as (
SELECT
    cast(get_current_timestamp() as datetime) as "_trilogyt__created_at"

)

SELECT
    split_ad1a64057f9ab34fecfe3f4ee78660bb0316dbda9370581ffbeb1e8bddf3d598."generic_split" as "generic_split",
    cuckoo."_trilogyt__created_at" as "_trilogyt__created_at"
FROM
    {{ ref('split_ad1a64057f9ab34fecfe3f4ee78660bb0316dbda9370581ffbeb1e8bddf3d598_gen_model') }} as split_ad1a64057f9ab34fecfe3f4ee78660bb0316dbda9370581ffbeb1e8bddf3d598
    FULL JOIN cuckoo on 1=1

