from dagster import asset
from dagster_duckdb import DuckDBResource


@asset(deps=[])
def dscte_generic_split_4a0c66ea(duck_db: DuckDBResource) -> None:
    with duck_db.get_connection() as conn:
        conn.execute(
            """ 
CREATE OR REPLACE TABLE "dscte_generic_split_4a0c66ea" (
    cte_generic_split int
); INSERT INTO "dscte_generic_split_4a0c66ea" 

WITH 
quizzical as (
SELECT
    "generic_avalues"."int_array" as "generic_int_array"
FROM
    (
select [1,2,3,4] as int_array, 2 as scalar
) as "generic_avalues"),
highfalutin as (
SELECT
    unnest("quizzical"."generic_int_array") as "generic_split"
FROM
    "quizzical")
SELECT
    "highfalutin"."generic_split" as "cte_generic_split"
FROM
    "highfalutin"
WHERE
    "highfalutin"."generic_split" in (1,2,3)

GROUP BY
    1 """
        )
