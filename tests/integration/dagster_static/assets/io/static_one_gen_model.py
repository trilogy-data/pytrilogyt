
from dagster_duckdb import DuckDBResource
from dagster import asset


@asset(deps=[])
def static_one(duck_db: DuckDBResource) -> None:
    with duck_db.get_connection() as conn:
        conn.execute(
           ''' 
CREATE OR REPLACE TABLE static_one AS

SELECT
    1 as "test"
 '''
        )
    