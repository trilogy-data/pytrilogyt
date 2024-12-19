
from dagster_duckdb import DuckDBResource
from dagster import asset


@asset(deps=[])
def ds445831a9e9e484d7f90c672304367319328b5fd8d8093cc8a12c9d788f4e4bd7(duck_db: DuckDBResource) -> None:
    with duck_db.get_connection() as conn:
        conn.execute(
           ''' 
CREATE OR REPLACE TABLE ds445831a9e9e484d7f90c672304367319328b5fd8d8093cc8a12c9d788f4e4bd7 AS

SELECT
    generic_avalues."int_array" as "generic_int_array",
    generic_avalues."scalar" as "generic_scalar"
FROM
    ((
select [1,2,3,4] as int_array, 2 as scalar
)) as generic_avalues '''
        )
    