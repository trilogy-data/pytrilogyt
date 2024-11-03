-- Generated from preql source: _internal_cached_intermediates_498b2d20ac0aeedac6fceb8efc30f13fc2109432edf99d1bbfb2c4a93ec3ca67
-- Do not edit manually
{{ config(materialized='table') }}

SELECT
    generic_avalues."int_array" as "generic_int_array",
    generic_avalues."scalar" as "generic_scalar"
FROM
    ((
select [1,2,3,4] as int_array, 2 as scalar
)) as generic_avalues