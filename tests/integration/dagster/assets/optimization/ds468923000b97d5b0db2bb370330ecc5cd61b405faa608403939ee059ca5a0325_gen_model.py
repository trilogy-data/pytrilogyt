from dagster_duckdb import DuckDBResource
from dagster import asset


@asset(deps=[])
def ds468923000b97d5b0db2bb370330ecc5cd61b405faa608403939ee059ca5a0325(duck_db: DuckDBResource) -> None:
    with duck_db.get_connection() as conn:
        conn.execute(
           ''' 
CREATE OR REPLACE TABLE ds468923000b97d5b0db2bb370330ecc5cd61b405faa608403939ee059ca5a0325 AS
SELECT
    "generic_avalues"."scalar" as "generic_scalar",
    "generic_avalues"."int_array" as "generic_int_array"
FROM
    ((
select [1,2,3,4] as int_array, 2 as scalar
)) as "generic_avalues" '''
        )
    