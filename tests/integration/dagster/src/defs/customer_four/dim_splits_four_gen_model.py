from dagster_duckdb import DuckDBResource
from dagster import asset

from src.defs.optimization.dscte_generic_split_4a0c66ea_gen_model import dscte_generic_split_4a0c66ea


@asset(deps=[dscte_generic_split_4a0c66ea])
def dim_splits_four(duck_db: DuckDBResource) -> None:
    with duck_db.get_connection() as conn:
        conn.execute(
           ''' 
CREATE OR REPLACE TABLE "dim_splits_four" (
    cte_generic_split int
); INSERT INTO "dim_splits_four" 
SELECT
    "dscte_generic_split_4a0c66ea"."cte_generic_split" as "cte_generic_split"
FROM
    "dscte_generic_split_4a0c66ea" '''
        )
    