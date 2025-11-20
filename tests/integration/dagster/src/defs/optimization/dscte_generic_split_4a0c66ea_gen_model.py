from dagster_duckdb import DuckDBResource
from dagster import asset


@asset(deps=[])
def dscte_generic_split_4a0c66ea(duck_db: DuckDBResource) -> None:
    with duck_db.get_connection() as conn:
        conn.execute(
           ''' 
CREATE OR REPLACE TABLE dscte_generic_split_4a0c66ea AS

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
    "quizzical"),
wakeful as (
SELECT
    "highfalutin"."generic_split" as "cte_generic_split"
FROM
    "highfalutin"
WHERE
    "highfalutin"."generic_split" in (1,2,3)
)
SELECT
    "wakeful"."cte_generic_split" as "cte_generic_split"
FROM
    "wakeful"
GROUP BY 
    "wakeful"."cte_generic_split" '''
        )
    