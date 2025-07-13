-- Generated from preql source: _opt_generic_c159bd00_build
-- Do not edit manually
{{ config(materialized='table') }}
SELECT
    "generic_avalues"."int_array" as "generic_int_array",
    "generic_avalues"."scalar" as "generic_scalar"
FROM
    (
select [1,2,3,4] as int_array, 2 as scalar
) as "generic_avalues"