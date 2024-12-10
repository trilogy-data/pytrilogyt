from dagster import asset
from dagster_duckdb import DuckDBResource

from tests.integration.dagster.assets.optimization.split_4a0c66eaa9bd766e209f4290311d8aaa8f79548618dc80e573bc787a6dd476e4_gen_model import (
    split_4a0c66eaa9bd766e209f4290311d8aaa8f79548618dc80e573bc787a6dd476e4,
)


@asset(deps=[split_4a0c66eaa9bd766e209f4290311d8aaa8f79548618dc80e573bc787a6dd476e4])
def dim_splits_four(duck_db: DuckDBResource) -> None:
    with duck_db.get_connection() as conn:
        conn.execute(
            """ 
CREATE OR REPLACE TABLE dim_splits_four AS

WITH 
quizzical as (
SELECT
    cast(get_current_timestamp() as datetime) as "_trilogyt__created_at"
)
SELECT
    split_4a0c66eaa9bd766e209f4290311d8aaa8f79548618dc80e573bc787a6dd476e4."cte_generic_split" as "cte_generic_split",
    quizzical."_trilogyt__created_at" as "_trilogyt__created_at"
FROM
    split_4a0c66eaa9bd766e209f4290311d8aaa8f79548618dc80e573bc787a6dd476e4
    FULL JOIN quizzical on 1=1 """
        )
