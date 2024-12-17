from dagster import Definitions, asset
from dagster_duckdb import DuckDBResource


@asset
def iris_dataset(duckdb: DuckDBResource) -> None:
    script = """WITH 
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
    unnest(quizzical."generic_int_array") as "generic_split",
    quizzical."generic_scalar" as "generic_scalar"
FROM
    quizzical)
SELECT
    highfalutin."generic_split" as "cte_generic_split"
FROM
    highfalutin
WHERE
    highfalutin."generic_split" in (1,2,3)
"""
    with duckdb.get_connection() as conn:
        conn.execute(f"CREATE TABLE test AS {script}")


defs = Definitions(
    assets=[iris_dataset],
    resources={
        "duckdb": DuckDBResource(
            database="my_duckdb_database.duckdb",  # required
        )
    },
)
