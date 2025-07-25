from dagster import asset
from dagster_duckdb import DuckDBResource


@asset(deps=[])
def dsgeneric_scalar_445831a9(duck_db: DuckDBResource) -> None:
    with duck_db.get_connection() as conn:
        conn.execute(
            """ 
CREATE OR REPLACE TABLE dsgeneric_scalar_445831a9 AS
SELECT
    "generic_avalues"."int_array" as "generic_int_array",
    "generic_avalues"."scalar" as "generic_scalar"
FROM
    (
select [1,2,3,4] as int_array, 2 as scalar
) as "generic_avalues" """
        )
