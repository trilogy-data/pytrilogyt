from dagster import asset
from dagster_duckdb import DuckDBResource

from src.defs.optimization.dsgeneric_scalar_445831a9_gen_model import (
    dsgeneric_scalar_445831a9,
)


@asset(deps=[dsgeneric_scalar_445831a9])
def dim_splits_two(duck_db: DuckDBResource) -> None:
    with duck_db.get_connection() as conn:
        conn.execute(
            """ 
CREATE OR REPLACE TABLE dim_splits_two AS
SELECT
    unnest("dsgeneric_scalar_445831a9"."generic_int_array") as "generic_split",
    "dsgeneric_scalar_445831a9"."generic_scalar" as "generic_scalar"
FROM
    "dsgeneric_scalar_445831a9" """
        )
