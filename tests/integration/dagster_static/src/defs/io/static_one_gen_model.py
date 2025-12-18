from dagster import asset
from dagster_duckdb import DuckDBResource


@asset(deps=[])
def static_one(duck_db: DuckDBResource) -> None:
    with duck_db.get_connection() as conn:
        conn.execute(
            """ 
CREATE OR REPLACE TABLE "static_one" (
    test int
); INSERT INTO "static_one" 
SELECT
    1 as "test"
 """
        )
