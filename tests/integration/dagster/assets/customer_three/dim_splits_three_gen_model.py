from dagster import asset
from dagster_duckdb import DuckDBResource

from assets.optimization.ds83fdbaa987de1c9e184157380531270fdaee84ab7aaca8a1a401ee8ac4015908_gen_model import (
    ds83fdbaa987de1c9e184157380531270fdaee84ab7aaca8a1a401ee8ac4015908,
)


@asset(deps=[ds83fdbaa987de1c9e184157380531270fdaee84ab7aaca8a1a401ee8ac4015908])
def dim_splits_three(duck_db: DuckDBResource) -> None:
    with duck_db.get_connection() as conn:
        conn.execute(
            """ 
CREATE OR REPLACE TABLE dim_splits_three AS

WITH 
quizzical as (
SELECT
    cast(get_current_timestamp() as datetime) as "_trilogyt__created_at"
)
SELECT
    "ds83fdbaa987de1c9e184157380531270fdaee84ab7aaca8a1a401ee8ac4015908"."cte_generic_split" as "cte_generic_split",
    "quizzical"."_trilogyt__created_at" as "_trilogyt__created_at"
FROM
    "ds83fdbaa987de1c9e184157380531270fdaee84ab7aaca8a1a401ee8ac4015908"
    FULL JOIN "quizzical" on 1=1 """
        )
