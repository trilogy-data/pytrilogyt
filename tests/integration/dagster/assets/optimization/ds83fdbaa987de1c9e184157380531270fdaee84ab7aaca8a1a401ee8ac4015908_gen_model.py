from dagster_duckdb import DuckDBResource
from dagster import asset


@asset(deps=[])
def ds83fdbaa987de1c9e184157380531270fdaee84ab7aaca8a1a401ee8ac4015908(duck_db: DuckDBResource) -> None:
    with duck_db.get_connection() as conn:
        conn.execute(
           ''' 
CREATE OR REPLACE TABLE ds83fdbaa987de1c9e184157380531270fdaee84ab7aaca8a1a401ee8ac4015908 AS

WITH 
quizzical as (
SELECT
    "generic_avalues"."int_array" as "generic_int_array",
    "generic_avalues"."scalar" as "generic_scalar"
FROM
    ((
select [1,2,3,4] as int_array, 2 as scalar
)) as "generic_avalues"),
highfalutin as (
SELECT
    "quizzical"."generic_scalar" as "generic_scalar",
    unnest("quizzical"."generic_int_array") as "generic_split"
FROM
    "quizzical")
SELECT
    "highfalutin"."generic_split" as "cte_generic_split",
    "highfalutin"."generic_scalar" as "cte_generic_scalar"
FROM
    "highfalutin"
WHERE
    "highfalutin"."generic_split" in (1,2,3)
 '''
        )
    