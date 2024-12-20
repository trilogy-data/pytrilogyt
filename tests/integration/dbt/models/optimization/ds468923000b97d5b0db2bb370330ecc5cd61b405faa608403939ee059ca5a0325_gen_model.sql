-- Generated from preql source: _internal_cached_intermediates_318aa3be58aecbf062e14ff6ee69c778f5f3b01959b2ebec2e5c58e27ae4b480
-- Do not edit manually
{{ config(materialized='table') }}

SELECT
    generic_avalues."scalar" as "generic_scalar",
    generic_avalues."int_array" as "generic_int_array"
FROM
    ((
select [1,2,3,4] as int_array, 2 as scalar
)) as generic_avalues