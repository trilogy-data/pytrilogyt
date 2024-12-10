from dagster import asset
from dagster_duckdb import DuckDBResource


@asset(deps=[])
def split_4a0c66eaa9bd766e209f4290311d8aaa8f79548618dc80e573bc787a6dd476e4(
    duck_db: DuckDBResource,
) -> None:
    with duck_db.get_connection() as conn:
        conn.execute(
            """ 
CREATE OR REPLACE TABLE split_4a0c66eaa9bd766e209f4290311d8aaa8f79548618dc80e573bc787a6dd476e4 AS

WITH 
quizzical as (
SELECT
    generic_avalues."int_array" as "generic_int_array",
    generic_avalues."scalar" as "generic_scalar"
FROM
    ((
select [1,2,3,4] as int_array, 2 as scalar
)) as generic_avalues),
highfalutin as (
SELECT
    quizzical."generic_scalar" as "generic_scalar",
    unnest(quizzical."generic_int_array") as "generic_split"
FROM
    quizzical)
SELECT
    highfalutin."generic_split" as "cte_generic_split"
FROM
    highfalutin
WHERE
    highfalutin."generic_split" in (1,2,3)
 """
        )
