-- Generated from preql source: customer_two
-- Do not edit manually
{{ config(materialized='table') }}

WITH 
quizzical as (
SELECT
    cast(get_current_timestamp() as datetime) as "_trilogyt__created_at"
)
SELECT
    dsdd968c4c1215b184ec36e1ed881d193d3e8e2ad062dd6750257f78115dccdfd7."generic_split" as "generic_split",
    dsdd968c4c1215b184ec36e1ed881d193d3e8e2ad062dd6750257f78115dccdfd7."generic_scalar" as "generic_scalar",
    quizzical."_trilogyt__created_at" as "_trilogyt__created_at"
FROM
    {{ ref('dsdd968c4c1215b184ec36e1ed881d193d3e8e2ad062dd6750257f78115dccdfd7_gen_model') }} as dsdd968c4c1215b184ec36e1ed881d193d3e8e2ad062dd6750257f78115dccdfd7
    FULL JOIN quizzical on 1=1