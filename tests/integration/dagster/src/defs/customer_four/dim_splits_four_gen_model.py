from dagster import asset
from dagster_duckdb import DuckDBResource

from src.defs.optimization.dscte_generic_scalar_02e41b09_gen_model import (
    dscte_generic_scalar_02e41b09,
)


@asset(deps=[dscte_generic_scalar_02e41b09])
def dim_splits_four(duck_db: DuckDBResource) -> None:
    with duck_db.get_connection() as conn:
        conn.execute(
            """ 
CREATE OR REPLACE TABLE dim_splits_four AS
SELECT
    "dscte_generic_scalar_02e41b09"."cte_generic_split" as "cte_generic_split"
FROM
    "dscte_generic_scalar_02e41b09"
GROUP BY 
    "dscte_generic_scalar_02e41b09"."cte_generic_split" """
        )
