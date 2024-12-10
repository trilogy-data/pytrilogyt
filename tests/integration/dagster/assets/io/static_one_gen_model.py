from dagster import asset
from dagster_duckdb import DuckDBResource


@asset(deps=[])
def static_one(duck_db: DuckDBResource) -> None:
    with duck_db.get_connection() as conn:
        conn.execute(
            """ 
SELECT
    1 as "test"
 """
        )
